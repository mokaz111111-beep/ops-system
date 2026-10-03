import { useCallback, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';

import type { GlobalContext, LogsViewState } from './urlState';
import { parseGlobalContext, parseLogsViewState, serializeLogsViewState } from './urlState';

type Updater = (prev: LogsViewState) => LogsViewState;

/**
 * URL 即状态。所有变更都写回 search params，组件不持有副本 —— 这样刷新、分享链接、
 * 浏览器前进后退、AI 回放走的是同一条路径（DD-008 §3.2）。
 */
export function useLogsViewState(): [LogsViewState, (updater: Updater) => void] {
  const [searchParams, setSearchParams] = useSearchParams();
  const state = useMemo(() => parseLogsViewState(searchParams), [searchParams]);

  const update = useCallback(
    (updater: Updater) => {
      setSearchParams(
        (prev) => serializeLogsViewState(updater(parseLogsViewState(prev))),
        { replace: false },
      );
    },
    [setSearchParams],
  );

  return [state, update];
}

/** 全局上下文条跨页面共用：任何页面都能读写它，且读写的都是 URL。 */
export function useGlobalContext(): [GlobalContext, (next: GlobalContext) => void] {
  const [searchParams, setSearchParams] = useSearchParams();
  const global = useMemo(() => parseGlobalContext(searchParams), [searchParams]);

  const setGlobal = useCallback(
    (next: GlobalContext) => {
      setSearchParams((prev) => {
        const params = new URLSearchParams(prev);
        params.set('from', next.timeRange.from);
        params.set('to', next.timeRange.to);
        if (next.clusterIds.length) params.set('clusters', next.clusterIds.join(','));
        else params.delete('clusters');
        if (next.projectIds.length) params.set('projects', next.projectIds.join(','));
        else params.delete('projects');
        return params;
      });
    },
    [setSearchParams],
  );

  return [global, setGlobal];
}
