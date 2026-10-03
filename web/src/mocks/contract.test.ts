import { describe, expect, it, vi } from 'vitest';

import { if7 } from '@/api/client';
import { ApiRequestError } from '@/api/errors';

const HOUR = { from: 'now-1h', to: 'now' };

async function expectApiError(promise: Promise<unknown>): Promise<ApiRequestError> {
  try {
    await promise;
  } catch (err) {
    expect(err).toBeInstanceOf(ApiRequestError);
    return err as ApiRequestError;
  }
  throw new Error('期望请求被拒绝，但它成功了');
}

describe('Mock IF-7 · 日志检索（FE-03）', () => {
  it('强制返回字段与契约一致，即使 select_paths 为空', async () => {
    const res = await if7.searchLogs({ time_range: HOUR, select_paths: [], limit: 5 });
    expect(res.rows.length).toBe(5);
    for (const row of res.rows) {
      expect(row).toHaveProperty('row_ref');
      expect(row).toHaveProperty('ts');
      expect(row).toHaveProperty('service');
      expect(row).toHaveProperty('cluster_id');
      expect(row).toHaveProperty('severity');
      expect(row).toHaveProperty('body');
      expect('trace_id' in row).toBe(true);
      expect(row.fields).toEqual({});
    }
    expect(res.query_id).toMatch(/^q-/);
    expect(res.scan_limit_reached).toBe(false);
  });

  it('只投影请求过的路径，不整条返回', async () => {
    const res = await if7.searchLogs({
      time_range: HOUR,
      select_paths: ['http.status_code', 'k8s.pod.name'],
      limit: 20,
    });
    for (const row of res.rows) {
      expect(Object.keys(row.fields).every((k) => ['http.status_code', 'k8s.pod.name'].includes(k)))
        .toBe(true);
    }
    expect(res.rows.some((r) => 'http.status_code' in r.fields)).toBe(true);
  });

  it('游标翻页取到的是新的行，且首次请求不传 cursor', async () => {
    const first = await if7.searchLogs({ time_range: HOUR, select_paths: [], limit: 10 });
    expect(first.next_cursor).toBeTruthy();
    const second = await if7.searchLogs({
      time_range: HOUR,
      select_paths: [],
      limit: 10,
      cursor: first.next_cursor!,
    });
    const firstRefs = new Set(first.rows.map((r) => r.row_ref));
    expect(second.rows.some((r) => firstRefs.has(r.row_ref))).toBe(false);
    // ts desc：第二页一定不晚于第一页
    expect(Date.parse(second.rows[0]!.ts)).toBeLessThanOrEqual(Date.parse(first.rows.at(-1)!.ts));
  });

  it('过期游标返回 CURSOR_EXPIRED + RESTART_QUERY，而不是悄悄重头给一页', async () => {
    const first = await if7.searchLogs({ time_range: HOUR, select_paths: [], limit: 10 });
    vi.stubEnv('VITE_MOCK_CURSOR_TTL_SECONDS', '0.001');
    const err = await expectApiError(
      if7.searchLogs({
        time_range: HOUR,
        select_paths: [],
        limit: 10,
        cursor: first.next_cursor!,
      }),
    );
    vi.unstubAllEnvs();
    expect(err.code).toBe('CURSOR_EXPIRED');
    expect(err.suggestedAction).toBe('RESTART_QUERY');
    expect(err.httpStatus).toBe(400);
  });

  it('过滤条件变了之后旧游标不再适用', async () => {
    const first = await if7.searchLogs({ time_range: HOUR, select_paths: [], limit: 10 });
    const err = await expectApiError(
      if7.searchLogs({
        time_range: HOUR,
        severity: ['ERROR'],
        select_paths: [],
        limit: 10,
        cursor: first.next_cursor!,
      }),
    );
    expect(err.code).toBe('CURSOR_EXPIRED');
  });

  it('结构化维度过滤生效（cluster 是第一维度）', async () => {
    const res = await if7.searchLogs({
      time_range: HOUR,
      cluster_ids: ['c-prod-sh-01'],
      severity: ['ERROR'],
      select_paths: [],
      limit: 30,
    });
    expect(res.rows.length).toBeGreaterThan(0);
    expect(res.rows.every((r) => r.cluster_id === 'c-prod-sh-01')).toBe(true);
    expect(res.rows.every((r) => r.severity === 'ERROR')).toBe(true);
  });
});

describe('Mock IF-7 · 契约里的失败路径', () => {
  it('缺时间窗 → TIME_RANGE_REQUIRED（没有默认值）', async () => {
    const err = await expectApiError(
      if7.searchLogs({ select_paths: [] } as unknown as Parameters<typeof if7.searchLogs>[0]),
    );
    expect(err.code).toBe('TIME_RANGE_REQUIRED');
    expect(err.suggestedAction).toBe('FIX_REQUEST');
  });

  it('未传 select_paths → SELECT_PATHS_REQUIRED', async () => {
    const err = await expectApiError(
      if7.searchLogs({ time_range: HOUR } as unknown as Parameters<typeof if7.searchLogs>[0]),
    );
    expect(err.code).toBe('SELECT_PATHS_REQUIRED');
  });

  it('不存在的字段路径 → FIELD_PATH_NOT_FOUND，details 带上被拒绝的路径', async () => {
    const err = await expectApiError(
      if7.searchLogs({ time_range: HOUR, select_paths: ['no.such.path'], limit: 1 }),
    );
    expect(err.code).toBe('FIELD_PATH_NOT_FOUND');
    expect(err.payload.details).toMatchObject({ field_path: 'no.such.path' });
  });

  it('时间窗过大 → TIME_RANGE_TOO_LARGE + NARROW_TIME_RANGE', async () => {
    const err = await expectApiError(
      if7.searchLogs({ time_range: { from: 'now-60d', to: 'now' }, select_paths: [], limit: 1 }),
    );
    expect(err.code).toBe('TIME_RANGE_TOO_LARGE');
    expect(err.suggestedAction).toBe('NARROW_TIME_RANGE');
  });

  it('扫描量超限 → SCAN_LIMIT_EXCEEDED + USE_ASYNC_EXPORT（422）', async () => {
    const err = await expectApiError(
      if7.searchLogs({ time_range: { from: 'now-20d', to: 'now' }, select_paths: [], limit: 1 }),
    );
    expect(err.code).toBe('SCAN_LIMIT_EXCEEDED');
    expect(err.suggestedAction).toBe('USE_ASYNC_EXPORT');
    expect(err.httpStatus).toBe(422);
    expect(err.payload.details).toHaveProperty('scanned_rows');
  });

  it('查询串语法错误 → QUERY_SYNTAX_ERROR', async () => {
    const err = await expectApiError(
      if7.searchLogs({ time_range: HOUR, query: 'service:"checkout', select_paths: [], limit: 1 }),
    );
    expect(err.code).toBe('QUERY_SYNTAX_ERROR');
  });
});

describe('Mock IF-7 · 直方图 / Facet / 字段目录 / 详情', () => {
  it('直方图按 severity 堆叠，并返回实际采用的 interval（FE-04）', async () => {
    const res = await if7.logHistogram({
      time_range: HOUR,
      interval: 'auto',
      group_by: 'severity',
    });
    expect(res.buckets.length).toBeGreaterThan(0);
    expect(res.interval).toMatch(/^\d+[smhd]$/);
    expect(res.downsampled).toBe(false);
    expect(new Set(res.buckets.map((b) => b.severity)).size).toBeGreaterThan(1);
  });

  it('直方图与检索共享同一份过滤条件', async () => {
    const all = await if7.logHistogram({ time_range: HOUR });
    const errorsOnly = await if7.logHistogram({ time_range: HOUR, severity: ['ERROR'] });
    const sum = (bs: { count: number }[]) => bs.reduce((n, b) => n + b.count, 0);
    expect(sum(errorsOnly.buckets)).toBeLessThan(sum(all.buckets));
    expect(errorsOnly.buckets.every((b) => b.severity === 'ERROR')).toBe(true);
  });

  it('facet 返回 declared / subcolumnized / indexed 三个物理标志（FE-05）', async () => {
    const res = await if7.logFacet({ time_range: HOUR, field_path: 'http.status_code' });
    expect(res.declared).toBe(true);
    expect(res.subcolumnized).toBe(true);
    expect(res.indexed).toBe(true);
    expect(res.values.length).toBeGreaterThan(0);
    expect(res.values[0]!.count).toBeGreaterThanOrEqual(res.values.at(-1)!.count);
  });

  it('长尾字段标成未子列化，时间窗过大时聚合超时并建议缩小时间窗', async () => {
    const ok = await if7.logFacet({ time_range: HOUR, field_path: 'vendor.partner_ref' });
    expect(ok.subcolumnized).toBe(false);

    const err = await expectApiError(
      if7.logFacet({ time_range: { from: 'now-24h', to: 'now' }, field_path: 'vendor.partner_ref' }),
    );
    expect(err.code).toBe('QUERY_TIMEOUT');
    expect(err.suggestedAction).toBe('NARROW_TIME_RANGE');
  });

  it('字段目录同时给出声明字段与长尾字段，并如实告知缓存年龄（FE-06）', async () => {
    const res = await if7.listFields({ signal: 'logs', time_range: HOUR });
    expect(res.catalog_age_seconds).toBeGreaterThan(0);
    expect(res.fields.some((f) => f.declared && f.subcolumnized)).toBe(true);
    expect(res.fields.some((f) => !f.subcolumnized)).toBe(true);
    const prefixed = await if7.listFields({ signal: 'logs', time_range: HOUR, prefix: 'http.' });
    expect(prefixed.fields.every((f) => f.path.includes('http.'))).toBe(true);
  });

  it('维度枚举带 last_seen，用于灰显已失活集群（FE-02）', async () => {
    const res = await if7.listDimensions({
      time_range: { from: 'now-24h', to: 'now' },
      dimensions: ['cluster', 'service'],
    });
    expect(res.cluster?.length).toBeGreaterThan(0);
    expect(res.service?.length).toBeGreaterThan(0);
    expect(res.project).toBeUndefined();
    const stale = res.cluster?.find((c) => c.value === 'c-dev-hz-09');
    expect(Date.now() - Date.parse(stale!.last_seen!)).toBeGreaterThan(24 * 3_600_000);
  });

  it('单条详情整条读，返回列表里没有的完整属性树（FE-07）', async () => {
    const list = await if7.searchLogs({ time_range: HOUR, select_paths: [], limit: 1 });
    const rowRef = list.rows[0]!.row_ref;
    const detail = await if7.getLogRow(rowRef);
    expect(detail.row.row_ref).toBe(rowRef);
    expect(detail.row.ts).toBe(list.rows[0]!.ts);
    expect(Object.keys(detail.row.resource_attributes ?? {}).length).toBeGreaterThan(0);
    expect(Object.keys(detail.row.log_attributes ?? {}).length).toBeGreaterThan(0);
  });

  it('上下文接口如实返回 order_guaranteed=false（行序语义尚未有结论）', async () => {
    const list = await if7.searchLogs({ time_range: HOUR, select_paths: [], limit: 1 });
    const ctx = await if7.logContext({ anchor: list.rows[0]!.row_ref, before: 5, after: 5 });
    expect(ctx.rows.length).toBe(11);
    expect(ctx.order_guaranteed).toBe(false);
  });
});
