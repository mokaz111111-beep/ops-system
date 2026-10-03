import { describe, expect, it } from 'vitest';

import { DEFAULT_COLUMNS, parseLogsViewState, type LogsViewState } from '@/state/urlState';

import { logsAroundTimestampLink, logsFilteredByServiceLink, traceDetailLink } from './links';

const state: LogsViewState = {
  global: {
    timeRange: { from: 'now-1h', to: 'now' },
    clusterIds: ['c-prod-sh-01'],
    projectIds: ['prj-prod'],
  },
  query: 'timeout',
  severity: ['ERROR'],
  facets: [],
  columns: [...DEFAULT_COLUMNS, 'http.status_code'],
  sort: 'ts desc',
  expandedRowRef: 'r-abc',
};

function searchOf(link: string): URLSearchParams {
  return new URLSearchParams(link.slice(link.indexOf('?')));
}

describe('一切皆可跳转（DD-008 §3.2 决策 5）', () => {
  it('跳 Trace 带上完整的全局上下文，而不是在内存里传状态', () => {
    const link = traceDetailLink('4bf92f3577b34da6a3ce929d0e0e4736', state.global);
    expect(link.startsWith('/apm/traces/4bf92f3577b34da6a3ce929d0e0e4736?')).toBe(true);
    const params = searchOf(link);
    expect(params.get('from')).toBe('now-1h');
    expect(params.get('to')).toBe('now');
    expect(params.get('clusters')).toBe('c-prod-sh-01');
    expect(params.get('projects')).toBe('prj-prod');
  });

  it('点 service 留在检索页并追加为 facet 条件，其余条件不丢', () => {
    const link = logsFilteredByServiceLink('checkout', state);
    const next = parseLogsViewState(searchOf(link));
    expect(next.facets).toEqual([{ path: 'service', values: ['checkout'] }]);
    expect(next.query).toBe('timeout');
    expect(next.severity).toEqual(['ERROR']);
    expect(next.columns).toContain('http.status_code');
    // 跳转后不应继续展开上一页的那一行
    expect(next.expandedRowRef).toBeNull();
  });

  it('点时间点把窗口收到该时刻 ±5 分钟，并写成绝对时间', () => {
    const link = logsAroundTimestampLink('2026-10-02T11:58:03.412Z', state);
    const next = parseLogsViewState(searchOf(link));
    expect(next.global.timeRange).toEqual({
      from: '2026-10-02T11:53:03.412Z',
      to: '2026-10-02T12:03:03.412Z',
    });
    expect(next.global.clusterIds).toEqual(['c-prod-sh-01']);
  });
});
