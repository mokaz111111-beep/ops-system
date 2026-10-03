import type { GlobalContext, LogsViewState } from '@/state/urlState';
import { globalContextSearch, serializeLogsViewState } from '@/state/urlState';

import { toAbsoluteExpr } from './time';

/**
 * DD-008 决策 5「一切皆可跳转」的实现机制：
 * 跳转 = 构造一个带完整上下文参数的 URL，而不是在内存里传状态。
 *
 * 目标页（Trace 详情、服务目录）在 M1 还只是占位路由，但链接构造逻辑是真实的 ——
 * 页面补齐时不需要回头改调用方。
 */

export function traceDetailLink(traceId: string, global: GlobalContext): string {
  return `/apm/traces/${encodeURIComponent(traceId)}?${globalContextSearch(global)}`;
}

export function serviceDetailLink(service: string, global: GlobalContext): string {
  return `/apm/services/${encodeURIComponent(service)}?${globalContextSearch(global)}`;
}

/** 点击行内 service：留在检索页，把该 service 作为一个 facet 条件追加。 */
export function logsFilteredByServiceLink(service: string, state: LogsViewState): string {
  const next: LogsViewState = {
    ...state,
    facets: upsertFacet(state.facets, 'service', service),
    expandedRowRef: null,
  };
  return `/logs/search?${serializeLogsViewState(next).toString()}`;
}

/** 点击时间点：以该时刻为中心把时间窗收到 ±padding，其余条件原样保留。 */
export function logsAroundTimestampLink(
  isoTs: string,
  state: LogsViewState,
  paddingMs = 5 * 60_000,
): string {
  const center = Date.parse(isoTs);
  if (Number.isNaN(center)) return `/logs/search?${serializeLogsViewState(state).toString()}`;
  const next: LogsViewState = {
    ...state,
    global: {
      ...state.global,
      timeRange: {
        from: toAbsoluteExpr(center - paddingMs),
        to: toAbsoluteExpr(center + paddingMs),
      },
    },
  };
  return `/logs/search?${serializeLogsViewState(next).toString()}`;
}

function upsertFacet(
  facets: LogsViewState['facets'],
  path: string,
  value: string,
): LogsViewState['facets'] {
  const existing = facets.find((f) => f.path === path);
  if (!existing) return [...facets, { path, values: [value] }];
  if (existing.values.includes(value)) return facets;
  return facets.map((f) => (f.path === path ? { ...f, values: [...f.values, value] } : f));
}

export function toggleFacetValue(
  facets: LogsViewState['facets'],
  path: string,
  value: string,
): LogsViewState['facets'] {
  const existing = facets.find((f) => f.path === path);
  if (!existing) return [...facets, { path, values: [value] }];
  const values = existing.values.includes(value)
    ? existing.values.filter((v) => v !== value)
    : [...existing.values, value];
  return values.length
    ? facets.map((f) => (f.path === path ? { ...f, values } : f))
    : facets.filter((f) => f.path !== path);
}
