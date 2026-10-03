import { useMemo, useState } from 'react';

import { if7 } from '@/api/client';
import { isApiRequestError } from '@/api/errors';
import { useLogHistogram, useLogSearch } from '@/api/queries';
import type { SuggestedAction } from '@/api/types';
import { ErrorCallout } from '@/components/ErrorCallout';
import { resolveTimeRange, toAbsoluteExpr } from '@/lib/time';
import type { LogsViewState } from '@/state/urlState';
import { buildLogFilter, buildSelectPaths } from '@/state/urlState';
import { useLogsViewState } from '@/state/useLogsViewState';

import { ColumnPicker } from './ColumnPicker';
import { FacetSidebar } from './FacetSidebar';
import { Histogram } from './Histogram';
import { LogTable } from './LogTable';
import { QueryBar } from './QueryBar';

/**
 * 日志检索页（PLAN D6）。DD-008 §3.4 的三段式布局：
 *   查询框 + 时间选择 → 日志量直方图（按 severity 堆叠、可框选缩窗）
 *   → 左侧 Facet 侧栏 + 右侧日志列表（虚拟滚动 + 游标续拉）
 *
 * 三段共享同一份过滤条件结构（buildLogFilter），任一处操作同步刷新另外两处 ——
 * 因为它们读的都是同一个 URL 状态。
 */
export function LogsSearchPage() {
  const [state, update] = useLogsViewState();
  const [pickerOpen, setPickerOpen] = useState(false);
  const [exportTaskId, setExportTaskId] = useState<string | null>(null);

  const search = useLogSearch(state);
  const histogram = useLogHistogram(state);

  const rows = useMemo(
    () => search.data?.pages.flatMap((p) => p.rows) ?? [],
    [search.data],
  );
  const lastPage = search.data?.pages[search.data.pages.length - 1];
  const scanLimitReached = search.data?.pages.some((p) => p.scan_limit_reached) ?? false;

  // `now` 相对表达每次渲染都会漂移，这里按时间窗 + 本次直方图数据定格一次。
  const resolved = useMemo(
    () => resolveTimeRange(state.global.timeRange),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.global.timeRange.from, state.global.timeRange.to, histogram.dataUpdatedAt],
  );

  const narrowTimeRange = () => {
    const mid = resolved.fromMs + resolved.durationMs / 2;
    const half = resolved.durationMs / 4;
    update((prev) => ({
      ...prev,
      global: {
        ...prev.global,
        timeRange: { from: toAbsoluteExpr(mid - half), to: toAbsoluteExpr(mid + half) },
      },
    }));
  };

  const searchErrorActions: Partial<Record<SuggestedAction, () => void>> = {
    NARROW_TIME_RANGE: narrowTimeRange,
    RESTART_QUERY: () => void search.refetch(),
    RETRY_AFTER: () => void search.refetch(),
    REDUCE_SELECT_PATHS: () => {
      setPickerOpen(true);
    },
    USE_ASYNC_EXPORT: () => {
      void if7
        .createExport({
          ...buildLogFilter(state),
          select_paths: buildSelectPaths(state.columns),
          format: 'ndjson',
        })
        .then((res) => {
          setExportTaskId(res.task_id);
        });
    },
    FIX_REQUEST: () => {
      // FIELD_PATH_NOT_FOUND 时 details 带着被拒绝的路径，直接把那一列摘掉是最有用的动作。
      const err = search.error;
      const badPath =
        isApiRequestError(err) && typeof err.payload.details?.['field_path'] === 'string'
          ? err.payload.details['field_path']
          : null;
      if (badPath) {
        update((prev) => ({ ...prev, columns: prev.columns.filter((c) => c !== badPath) }));
      } else {
        setPickerOpen(true);
      }
    },
  };

  return (
    <div className="logs-page">
      <QueryBar state={state} onChange={update} busy={search.isFetching} />

      {histogram.error ? (
        <div style={{ padding: '8px 14px' }}>
          <ErrorCallout
            error={histogram.error}
            variant="warn"
            actions={{
              NARROW_TIME_RANGE: narrowTimeRange,
              RETRY_AFTER: () => void histogram.refetch(),
            }}
          />
        </div>
      ) : (
        <Histogram
          buckets={histogram.data?.buckets ?? []}
          interval={histogram.data?.interval ?? 'auto'}
          downsampled={histogram.data?.downsampled ?? false}
          fromMs={resolved.fromMs}
          toMs={resolved.toMs}
          loading={histogram.isFetching}
          onBrush={(fromMs, toMs) => {
            update((prev) => ({
              ...prev,
              global: {
                ...prev.global,
                timeRange: { from: toAbsoluteExpr(fromMs), to: toAbsoluteExpr(toMs) },
              },
              expandedRowRef: null,
            }));
          }}
        />
      )}

      <div className="logs-body">
        <FacetSidebar state={state} onChange={update} />

        <div className="log-list">
          <div className="log-toolbar">
            <span>
              已加载 {rows.length.toLocaleString('zh-CN')} 行
              {search.isFetching && ' · 查询中'}
            </span>
            <ActiveFilters state={state} onChange={update} />
            <span style={{ flex: 1 }} />
            {lastPage?.query_id && (
              <span className="muted mono" style={{ fontSize: 11 }}>
                query_id: {lastPage.query_id}
              </span>
            )}
            <button
              type="button"
              onClick={() => {
                setPickerOpen(true);
              }}
            >
              + 列（{state.columns.length}）
            </button>
            <button
              type="button"
              onClick={() => {
                update((prev) => ({
                  ...prev,
                  sort: prev.sort === 'ts desc' ? 'ts asc' : 'ts desc',
                }));
              }}
            >
              {state.sort === 'ts desc' ? '最新在前' : '最早在前'}
            </button>
          </div>

          {scanLimitReached && (
            <div style={{ padding: '0 12px' }}>
              <div className="callout warn">
                <div className="callout-body">
                  <div className="callout-title">结果已被扫描上限截断</div>
                  <div className="muted">
                    服务端返回 scan_limit_reached=true，当前列表不是完整结果集，不能据此下结论。
                  </div>
                  <div className="callout-actions">
                    <button type="button" className="primary" onClick={narrowTimeRange}>
                      缩小时间窗
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        searchErrorActions.USE_ASYNC_EXPORT?.();
                      }}
                    >
                      改走异步导出
                    </button>
                  </div>
                </div>
              </div>
            </div>
          )}

          {exportTaskId && (
            <div style={{ padding: '0 12px' }}>
              <div className="callout">
                <div className="callout-body">
                  <div className="callout-title">导出任务已创建</div>
                  <div className="muted mono">task_id: {exportTaskId}</div>
                </div>
              </div>
            </div>
          )}

          {search.error && (
            <div style={{ padding: '0 12px' }}>
              <ErrorCallout error={search.error} actions={searchErrorActions} />
            </div>
          )}

          {search.isLoading && (
            <div style={{ padding: 12 }}>
              {Array.from({ length: 12 }, (_, i) => (
                <div className="skeleton" key={i} style={{ margin: '6px 0' }} />
              ))}
            </div>
          )}

          {!search.isLoading && !search.error && rows.length === 0 && (
            <div className="muted" style={{ padding: 16 }}>
              当前条件下没有日志。试试放宽 severity、清掉 facet 选择，或扩大时间窗。
            </div>
          )}

          {rows.length > 0 && (
            <LogTable
              rows={rows}
              state={state}
              onChange={update}
              hasNextPage={search.hasNextPage}
              isFetchingNextPage={search.isFetchingNextPage}
              onLoadMore={() => void search.fetchNextPage()}
            />
          )}
        </div>
      </div>

      <footer className="page-footer">
        <span className="muted">模式</span>
        <div className="mode-switch">
          <button type="button" className="primary">
            列表
          </button>
          <button type="button" disabled title="FE-12 图表分析模式排在 M2">
            图表分析
          </button>
          <button type="button" disabled title="FE-13 SQL 编辑器模式排在 M3（IF-007 §7）">
            SQL 编辑器
          </button>
        </div>
        <span style={{ flex: 1 }} />
        <button type="button" disabled title="DD-008 §4 / IF-007 §6：保存视图的持久化归属未定">
          保存视图
        </button>
        <button type="button" disabled title="FE-11 排在 M3，且依赖检索语法与告警条件语法统一">
          创建告警
        </button>
      </footer>

      {pickerOpen && (
        <ColumnPicker
          state={state}
          onClose={() => {
            setPickerOpen(false);
          }}
          onChangeColumns={(columns) => {
            update((prev) => ({ ...prev, columns }));
          }}
        />
      )}
    </div>
  );
}

function ActiveFilters({
  state,
  onChange,
}: {
  state: LogsViewState;
  onChange: (updater: (prev: LogsViewState) => LogsViewState) => void;
}) {
  if (state.facets.length === 0) return null;
  return (
    <span style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
      {state.facets.flatMap((facet) =>
        facet.values.map((value) => (
          <button
            key={`${facet.path}=${value}`}
            type="button"
            className="chip on"
            title="移除该 facet 条件"
            onClick={() => {
              onChange((prev) => ({
                ...prev,
                facets: prev.facets
                  .map((f) =>
                    f.path === facet.path
                      ? { ...f, values: f.values.filter((v) => v !== value) }
                      : f,
                  )
                  .filter((f) => f.values.length > 0),
              }));
            }}
          >
            {facet.path}: {value} ×
          </button>
        )),
      )}
    </span>
  );
}
