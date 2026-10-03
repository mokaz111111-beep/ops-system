import { describe, expect, it } from 'vitest';

import {
  buildLogFilter,
  buildSelectPaths,
  composeQuery,
  DEFAULT_COLUMNS,
  logsQueryKey,
  parseLogsViewState,
  serializeLogsViewState,
  type LogsViewState,
} from './urlState';

function stateOf(overrides: Partial<LogsViewState> = {}): LogsViewState {
  return {
    global: {
      timeRange: { from: 'now-1h', to: 'now' },
      clusterIds: [],
      projectIds: [],
    },
    query: '',
    severity: [],
    facets: [],
    columns: [...DEFAULT_COLUMNS],
    sort: 'ts desc',
    expandedRowRef: null,
    ...overrides,
  };
}

describe('URL 序列化（DD-008 §3.2 的硬要求）', () => {
  it('完整状态可往返，相对时间表达不被解析成绝对值', () => {
    const state = stateOf({
      global: {
        timeRange: { from: 'now-24h', to: 'now' },
        clusterIds: ['c-prod-sh-01', 'c-prod-bj-02'],
        projectIds: ['prj-prod'],
      },
      query: 'http.status_code:"500"',
      severity: ['ERROR', 'WARN'],
      facets: [{ path: 'k8s.pod.name', values: ['checkout-7d9f-x2k1'] }],
      columns: ['ts', 'severity', 'service', 'http.status_code'],
      sort: 'ts asc',
      expandedRowRef: 'r-abc',
    });

    const params = serializeLogsViewState(state);
    // IF-007 §2.2：相对形式必须原样往返
    expect(params.get('from')).toBe('now-24h');
    expect(params.get('to')).toBe('now');

    expect(parseLogsViewState(params)).toEqual(state);
  });

  it('facet 取值里的分隔符不会破坏往返', () => {
    const state = stateOf({
      facets: [
        { path: 'http.target', values: ['/a,b', 'x=y', 'with space'] },
        { path: 'db.statement', values: ['SELECT * FROM t WHERE a=1,b=2'] },
      ],
    });
    expect(parseLogsViewState(serializeLogsViewState(state))).toEqual(state);
  });

  it('默认列不写进 URL，但解析回来仍是默认列', () => {
    const params = serializeLogsViewState(stateOf());
    expect(params.has('cols')).toBe(false);
    expect(parseLogsViewState(params).columns).toEqual([...DEFAULT_COLUMNS]);
  });

  it('空 URL 解析出可用的默认状态（默认列是具体路径清单，不是「交给后端决定」）', () => {
    const state = parseLogsViewState(new URLSearchParams());
    expect(state.columns).toEqual(['ts', 'severity', 'service', 'cluster_id', 'body', 'trace_id']);
    expect(state.global.timeRange).toEqual({ from: 'now-1h', to: 'now' });
  });
});

describe('投影路径（DD-008 §3.2 决策 5 与 IF-007 §4.3 的耦合点）', () => {
  it('强制返回字段不进 select_paths，动态路径去重并排序', () => {
    expect(buildSelectPaths(['ts', 'service', 'trace_id', 'b.path', 'a.path', 'a.path'])).toEqual([
      'a.path',
      'b.path',
    ]);
  });

  it('即使用户把 trace_id 列删掉，跳转链接所需字段依然由契约强制返回', () => {
    const columns = ['ts', 'body'];
    expect(buildSelectPaths(columns)).toEqual([]);
    // 不出现在 select_paths 里不等于拿不到：IF-007 §4.3 的强制返回集合覆盖了它
  });
});

describe('查询键（DD-008 §3.4 结论 1：列顺序不触发查询）', () => {
  it('调整列顺序不改变查询键', () => {
    const a = stateOf({ columns: ['ts', 'service', 'http.status_code'] });
    const b = stateOf({ columns: ['http.status_code', 'ts', 'service'] });
    expect(logsQueryKey(a)).toEqual(logsQueryKey(b));
  });

  it('增删列会改变查询键', () => {
    const a = stateOf({ columns: ['ts', 'service'] });
    const b = stateOf({ columns: ['ts', 'service', 'http.status_code'] });
    expect(logsQueryKey(a)).not.toEqual(logsQueryKey(b));
  });

  it('facet 选择会改变查询键（三段共享同一份过滤条件）', () => {
    const a = stateOf();
    const b = stateOf({ facets: [{ path: 'service', values: ['checkout'] }] });
    expect(logsQueryKey(a)).not.toEqual(logsQueryKey(b));
  });
});

describe('过滤条件结构（IF-007 §2.3：四类接口共享一份）', () => {
  it('查询框与 facet 合成到同一个 query 字段，维度过滤走结构化字段', () => {
    const filter = buildLogFilter(
      stateOf({
        query: 'timeout',
        facets: [{ path: 'service', values: ['checkout', 'order-svc'] }],
        severity: ['ERROR'],
        global: {
          timeRange: { from: 'now-1h', to: 'now' },
          clusterIds: ['c-prod-sh-01'],
          projectIds: [],
        },
      }),
    );
    expect(filter.query).toBe('(timeout) AND (service:"checkout" OR service:"order-svc")');
    expect(filter.severity).toEqual(['ERROR']);
    expect(filter.cluster_ids).toEqual(['c-prod-sh-01']);
    expect(filter).not.toHaveProperty('project_ids');
  });

  it('没有任何条件时不生成空 query 字段', () => {
    expect(buildLogFilter(stateOf())).toEqual({ time_range: { from: 'now-1h', to: 'now' } });
  });

  it('facet 取值中的引号被转义', () => {
    expect(composeQuery('', [{ path: 'body', values: ['say "hi"'] }])).toBe('body:"say \\"hi\\""');
  });
});
