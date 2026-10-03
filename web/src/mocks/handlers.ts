import { delay, http, HttpResponse } from 'msw';

import { API_BASE_URL } from '@/api/client';
import type {
  FacetValue,
  HistogramBucket,
  ListDimensionsRequest,
  ListFieldsRequest,
  LogContextRequest,
  LogFacetRequest,
  LogHistogramRequest,
  LogRow,
  SearchLogsRequest,
  Severity,
} from '@/api/types';

import {
  CLUSTERS,
  dimensionLastSeen,
  fieldCatalog,
  fullRow,
  indexOfRowRef,
  projectedRow,
  SERVICES,
  tsAt,
} from './dataset';
import {
  assertPathsExist,
  decodeCursor,
  encodeCursor,
  fingerprint,
  resolveScan,
  rowAccessor,
} from './engine';
import { errorResponse, MockApiError, newQueryId } from './errors';
import { asSearchText } from './queryLang';

const IS_TEST = import.meta.env.MODE === 'test';
const MAX_SELECT_PATHS = 50;
/** 单次请求允许遍历的下标数。耗尽仍未填满一页即视为触达扫描上限。 */
const ITERATION_BUDGET = 600_000;
/** 聚合类接口超过这个行数就抽样，并如实置 downsampled=true。 */
const AGG_SAMPLE_THRESHOLD = 150_000;

async function latency(ms: number): Promise<void> {
  if (!IS_TEST) await delay(ms);
}

function url(path: string): string {
  return `${API_BASE_URL}${path}`;
}

async function readJson<T>(request: Request): Promise<T> {
  try {
    return (await request.json()) as T;
  } catch {
    throw new MockApiError('INVALID_PARAM', '请求体不是合法 JSON');
  }
}

function sampleStep(scannedRows: number): number {
  return scannedRows > AGG_SAMPLE_THRESHOLD ? Math.ceil(scannedRows / AGG_SAMPLE_THRESHOLD) : 1;
}

const INTERVAL_LADDER: readonly (readonly [string, number])[] = [
  ['1s', 1000],
  ['5s', 5000],
  ['10s', 10_000],
  ['30s', 30_000],
  ['1m', 60_000],
  ['5m', 300_000],
  ['10m', 600_000],
  ['30m', 1_800_000],
  ['1h', 3_600_000],
  ['3h', 10_800_000],
  ['6h', 21_600_000],
  ['12h', 43_200_000],
  ['1d', 86_400_000],
];

const TARGET_BUCKETS = 60;

function parseInterval(raw: string | undefined): number | null {
  if (!raw || raw === 'auto') return null;
  const match = /^(\d+)([smhd])$/.exec(raw.trim());
  if (!match) return null;
  const unit = { s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 }[match[2]!];
  return unit === undefined ? null : Number(match[1]) * unit;
}

function chooseInterval(durationMs: number, requested: string | undefined) {
  const wanted = parseInterval(requested);
  const auto =
    INTERVAL_LADDER.find(([, ms]) => durationMs / ms <= TARGET_BUCKETS) ??
    INTERVAL_LADDER[INTERVAL_LADDER.length - 1]!;
  // auto / 未指定：服务端按窗口选粒度，这不是「请求被放粗」
  if (wanted === null) return { label: auto[0], ms: auto[1], coarsened: false };
  if (durationMs / wanted > TARGET_BUCKETS * 4) {
    // 服务端有权因时间窗过大而放粗粒度，前端按返回值画坐标轴（IF-007 §4.4）
    return { label: auto[0], ms: auto[1], coarsened: true };
  }
  const label = INTERVAL_LADDER.find(([, ms]) => ms === wanted)?.[0] ?? requested!;
  return { label, ms: wanted, coarsened: false };
}

export const handlers = [
  // FE-02 维度枚举 -----------------------------------------------------------
  http.post(url('/dimensions'), async ({ request }) => {
    try {
      const body = await readJson<ListDimensionsRequest>(request);
      resolveScan(body);
      await latency(90);
      const now = Date.now();
      const wanted = new Set(body.dimensions);
      return HttpResponse.json({
        ...(wanted.has('project')
          ? {
              project: [...new Set(CLUSTERS.map((c) => c.project))].map((value) => ({
                value,
                last_seen: dimensionLastSeen(now, value === 'prj-dev'),
              })),
            }
          : {}),
        ...(wanted.has('cluster')
          ? {
              cluster: CLUSTERS.map((c) => ({
                value: c.id,
                last_seen: dimensionLastSeen(now, c.stale),
              })),
            }
          : {}),
        ...(wanted.has('service')
          ? {
              service: SERVICES.map((value, idx) => ({
                value,
                last_seen: new Date(now - idx * 15_000).toISOString(),
              })),
            }
          : {}),
        query_id: newQueryId(),
      });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // FE-06 字段目录 -----------------------------------------------------------
  http.post(url('/fields'), async ({ request }) => {
    try {
      const body = await readJson<ListFieldsRequest>(request);
      resolveScan(body);
      await latency(120);
      const prefix = body.prefix?.toLowerCase() ?? '';
      const fields = fieldCatalog(Date.now()).filter((f) =>
        prefix ? f.path.toLowerCase().includes(prefix) : true,
      );
      return HttpResponse.json({
        fields,
        // 服务端缓存，刷新周期 <= 5 分钟；前端不得假设它实时（IF-007 §4.2）
        catalog_age_seconds: 142,
        query_id: newQueryId(),
      });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // FE-03 日志检索 -----------------------------------------------------------
  http.post(url('/logs/search'), async ({ request }) => {
    try {
      const body = await readJson<SearchLogsRequest>(request);
      if (!Array.isArray(body.select_paths)) {
        throw new MockApiError(
          'SELECT_PATHS_REQUIRED',
          'select_paths 必填：未声明投影路径时网关无法构造路径投影',
        );
      }
      if (body.select_paths.length > MAX_SELECT_PATHS) {
        throw new MockApiError('TOO_MANY_SELECT_PATHS', `投影路径不得超过 ${MAX_SELECT_PATHS} 个`, {
          details: { requested: body.select_paths.length, limit: MAX_SELECT_PATHS },
        });
      }
      assertPathsExist(body.select_paths);

      const scan = resolveScan(body);
      const sort = body.sort ?? 'ts desc';
      const limit = body.limit ?? 100;
      const fp = fingerprint({
        time_range: body.time_range,
        query: body.query,
        cluster_ids: body.cluster_ids,
        project_ids: body.project_ids,
        severity: body.severity,
        select_paths: [...body.select_paths].sort(),
        sort,
      });

      const descending = sort === 'ts desc';
      const start = body.cursor
        ? decodeCursor(body.cursor, fp)
        : descending
          ? scan.last
          : scan.first;

      const rows: LogRow[] = [];
      let i = start;
      let steps = 0;
      let budgetExhausted = false;
      while (rows.length < limit && i >= scan.first && i <= scan.last) {
        if (steps >= ITERATION_BUDGET) {
          budgetExhausted = true;
          break;
        }
        steps += 1;
        if (scan.matches(i)) rows.push(projectedRow(i, body.select_paths));
        i += descending ? -1 : 1;
      }

      const exhausted = budgetExhausted || i < scan.first || i > scan.last;
      await latency(160 + Math.min(240, rows.length));

      return HttpResponse.json({
        rows,
        next_cursor:
          exhausted || rows.length < limit
            ? null
            : encodeCursor({ next: i, fp, at: Date.now() }),
        // 结果因触达扫描上限被截断时必须如实上报，UI 不得静默展示不完整结果
        scan_limit_reached: budgetExhausted,
        query_id: newQueryId(),
      });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // FE-04 直方图 -------------------------------------------------------------
  http.post(url('/logs/histogram'), async ({ request }) => {
    try {
      const body = await readJson<LogHistogramRequest>(request);
      const scan = resolveScan(body);
      const durationMs = scan.toMs - scan.fromMs;
      const interval = chooseInterval(durationMs, body.interval);
      const step = sampleStep(scan.scannedRows);

      const counts = new Map<string, number>();
      for (let i = scan.first; i <= scan.last; i += step) {
        if (!scan.matches(i)) continue;
        const bucketStart = Math.floor(tsAt(i) / interval.ms) * interval.ms;
        const key = `${String(bucketStart)}|${rowAccessor(i).get('severity') as Severity}`;
        counts.set(key, (counts.get(key) ?? 0) + step);
      }

      const buckets: HistogramBucket[] = [...counts.entries()]
        .map(([key, count]) => {
          const [ts, severity] = key.split('|');
          return {
            ts_start: new Date(Number(ts)).toISOString(),
            severity: severity as Severity,
            count,
          };
        })
        .sort((a, b) => a.ts_start.localeCompare(b.ts_start));

      await latency(140);
      return HttpResponse.json({
        buckets,
        interval: interval.label,
        downsampled: step > 1 || interval.coarsened,
        query_id: newQueryId(),
      });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // FE-05 Facet top values ----------------------------------------------------
  http.post(url('/logs/facet'), async ({ request }) => {
    try {
      const body = await readJson<LogFacetRequest>(request);
      if (!body.field_path) throw new MockApiError('INVALID_PARAM', 'field_path 必填');
      assertPathsExist([body.field_path]);
      const scan = resolveScan(body);
      const descriptor = fieldCatalog(Date.now()).find((f) => f.path === body.field_path)!;

      // 长尾字段聚合要走通用路径，延迟高一个量级；时间窗再大就直接超时。
      // UI 据此给出「缩小时间窗」的引导而不是一个红色报错（DD-008 §3.4）。
      if (!descriptor.subcolumnized) {
        if (scan.toMs - scan.fromMs > 6 * 3_600_000) {
          throw new MockApiError(
            'QUERY_TIMEOUT',
            `字段 ${body.field_path} 未被子列化，当前时间窗下聚合超时`,
            { details: { field_path: body.field_path, subcolumnized: false } },
          );
        }
        await latency(900);
      }

      const step = sampleStep(scan.scannedRows);
      const counts = new Map<string, number>();
      for (let i = scan.first; i <= scan.last; i += step) {
        if (!scan.matches(i)) continue;
        const value = rowAccessor(i).get(body.field_path);
        if (value === undefined || value === null) continue;
        const key = asSearchText(value);
        counts.set(key, (counts.get(key) ?? 0) + step);
      }

      const topN = body.top_n ?? 10;
      const sorted = [...counts.entries()].sort((a, b) => b[1] - a[1]);
      const values: FacetValue[] = sorted
        .slice(0, topN)
        .map(([value, count]) => ({ value, count }));

      await latency(180);
      return HttpResponse.json({
        values,
        distinct_estimate: sorted.length,
        declared: descriptor.declared,
        subcolumnized: descriptor.subcolumnized,
        indexed: descriptor.indexed,
        truncated: sorted.length > topN,
        query_id: newQueryId(),
      });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // FE-07 单条详情 -----------------------------------------------------------
  http.get(url('/logs/row/:rowRef'), async ({ params }) => {
    try {
      const rowRef = decodeURIComponent(String(params['rowRef']));
      const index = indexOfRowRef(rowRef);
      if (index === null) {
        throw new MockApiError('INVALID_PARAM', 'row_ref 无法解析');
      }
      // 展开有真实网络延迟，前端必须给骨架态（DD-008 §3.4）
      await latency(320);
      return HttpResponse.json({ row: fullRow(index), query_id: newQueryId() });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // FE-08 上下文 ±N 条 --------------------------------------------------------
  http.post(url('/logs/context'), async ({ request }) => {
    try {
      const body = await readJson<LogContextRequest>(request);
      const anchor = indexOfRowRef(body.anchor);
      if (anchor === null) throw new MockApiError('INVALID_PARAM', 'anchor 无法解析');
      const before = body.before ?? 50;
      const after = body.after ?? 50;
      const rows: LogRow[] = [];
      for (let i = anchor - before; i <= anchor + after; i += 1) {
        rows.push(projectedRow(i, []));
      }
      await latency(260);
      return HttpResponse.json({
        rows,
        // 行序语义尚未解决（IF-007 §4.7 / DD-008 §4），mock 如实返回 false，
        // UI 必须据此说明这是「时间邻近近似」而不是精确上下文。
        order_guaranteed: false,
        query_id: newQueryId(),
      });
    } catch (err) {
      return errorResponse(err);
    }
  }),

  // 异步导出：SCAN_LIMIT_EXCEEDED 的降级落点 ----------------------------------
  http.post(url('/exports'), async () => {
    await latency(120);
    return HttpResponse.json({ task_id: `exp-${Date.now().toString(36)}` }, { status: 202 });
  }),

  http.get(url('/exports/:taskId'), async () => {
    await latency(80);
    return HttpResponse.json({ status: 'running', progress: 0.42, download_url: null });
  }),

  // 查询取消 ------------------------------------------------------------------
  http.delete(url('/queries/:clientQueryId'), () => new HttpResponse(null, { status: 204 })),
];
