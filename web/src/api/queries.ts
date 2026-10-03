import { useInfiniteQuery, useQuery } from '@tanstack/react-query';

import type { LogsViewState } from '@/state/urlState';
import { buildLogFilter, buildSelectPaths, logsQueryKey } from '@/state/urlState';

import { if7 } from './client';
import { isApiRequestError } from './errors';
import type { Cursor, Signal, TimeRange } from './types';

const LOG_PAGE_SIZE = 100;

/**
 * 涓嶅 4xx 绫绘嫆缁濋噸璇曪細IF-007 搂3 閲?retryable=false 鐨勯敊璇噸璇曞彧浼氱櫧鐑ч厤棰濓紝
 * 鑰?UI 宸茬粡鎶婂畠浠覆鏌撴垚浜嗗彲鎿嶄綔鎻愮ず銆? */
function retryPolicy(failureCount: number, error: Error): boolean {
  if (isApiRequestError(error) && !error.retryable) return false;
  return failureCount < 2;
}

export function useDimensions(timeRange: TimeRange) {
  return useQuery({
    queryKey: ['dimensions', timeRange],
    queryFn: ({ signal }) =>
      if7.listDimensions(
        { time_range: timeRange, dimensions: ['project', 'cluster', 'service'] },
        { signal },
      ),
    retry: retryPolicy,
    staleTime: 60_000,
  });
}

export function useFieldCatalog(
  signalKind: Signal,
  timeRange: TimeRange,
  prefix: string,
) {
  return useQuery({
    queryKey: ['fields', signalKind, timeRange, prefix],
    queryFn: ({ signal }) =>
      if7.listFields(
        {
          signal: signalKind,
          time_range: timeRange,
          ...(prefix ? { prefix } : {}),
        },
        { signal },
      ),
    retry: retryPolicy,
    // 鏈嶅姟绔紦瀛樺埛鏂板懆鏈?<= 5 鍒嗛挓锛圛F-007 搂4.2锛夛紝鍓嶇娌″繀瑕佹瘮瀹冩洿鍕ゅ揩銆?    staleTime: 60_000,
  });
}

export function useLogSearch(
  state: LogsViewState,
) {
  return useInfiniteQuery({
    queryKey: logsQueryKey(state),
    initialPageParam: undefined as Cursor | undefined,
    queryFn: ({ pageParam, signal }) =>
      if7.searchLogs(
        {
          ...buildLogFilter(state),
          select_paths: buildSelectPaths(state.columns),
          sort: state.sort,
          limit: LOG_PAGE_SIZE,
          ...(pageParam ? { cursor: pageParam } : {}),
        },
        { signal },
      ),
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    retry: retryPolicy,
  });
}

export function useLogHistogram(state: LogsViewState) {
  return useQuery({
    queryKey: ['logs', 'histogram', buildLogFilter(state)],
    queryFn: ({ signal }) =>
      if7.logHistogram(
        { ...buildLogFilter(state), interval: 'auto', group_by: 'severity' },
        { signal },
      ),
    retry: retryPolicy,
  });
}

export function useLogFacet(
  state: LogsViewState,
  fieldPath: string,
  enabled: boolean,
) {
  return useQuery({
    queryKey: ['logs', 'facet', fieldPath, buildLogFilter(state)],
    queryFn: ({ signal }) =>
      if7.logFacet({ ...buildLogFilter(state), field_path: fieldPath, top_n: 10 }, { signal }),
    enabled,
    retry: retryPolicy,
  });
}

/**
 * DD-008 搂3.4锛氬睍寮€ = 涓€娆¤繑鍥?1 琛岀殑璇︽儏璇锋眰锛屼笉鏄粠鍒楄〃鏁版嵁閲屽彇 鈥斺€? * 鍒楄〃鍙甫鎶曞奖鍑虹殑璺緞锛屽畬鏁村睘鎬ф爲涓嶅湪鍏朵腑銆傚洜姝ゅ睍寮€鏈夌綉缁滃欢杩燂紝闇€瑕侀鏋舵€併€? */
export function useLogRow(rowRef: string | null) {
  return useQuery({
    queryKey: ['logs', 'row', rowRef],
    queryFn: ({ signal }) => if7.getLogRow(rowRef!, { signal }),
    enabled: rowRef !== null,
    retry: retryPolicy,
    staleTime: 5 * 60_000,
  });
}
