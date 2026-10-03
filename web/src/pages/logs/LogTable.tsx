import { useVirtualizer } from '@tanstack/react-virtual';
import { useCallback, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';

import type { LogRow } from '@/api/types';
import { logsAroundTimestampLink, logsFilteredByServiceLink, traceDetailLink } from '@/lib/links';
import { formatTimestamp } from '@/lib/time';
import type { LogsViewState } from '@/state/urlState';
import { columnWidthOf, useViewState } from '@/state/viewState';

import { LogRowDetail } from './LogRowDetail';

interface Props {
  rows: LogRow[];
  state: LogsViewState;
  onChange: (updater: (prev: LogsViewState) => LogsViewState) => void;
  hasNextPage: boolean;
  isFetchingNextPage: boolean;
  onLoadMore: () => void;
}

const COLUMN_LABEL: Record<string, string> = {
  ts: '时间',
  severity: 'SEVERITY',
  service: 'SERVICE',
  cluster_id: 'CLUSTER',
  trace_id: 'TRACE',
  body: 'BODY',
};

export function LogTable({
  rows,
  state,
  onChange,
  hasNextPage,
  isFetchingNextPage,
  onLoadMore,
}: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const columnWidths = useViewState((s) => s.columnWidths);

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 25,
    overscan: 20,
    getItemKey: (index) => rows[index]?.row_ref ?? index,
  });

  const items = virtualizer.getVirtualItems();
  const lastIndex = items.length ? items[items.length - 1]!.index : 0;

  // DD-008 决策 4：虚拟滚动只是渲染侧的配套，滚动到底触发游标续拉。
  useEffect(() => {
    if (hasNextPage && !isFetchingNextPage && lastIndex >= rows.length - 15 && rows.length > 0) {
      onLoadMore();
    }
  }, [hasNextPage, isFetchingNextPage, lastIndex, rows.length, onLoadMore]);

  const toggleExpand = useCallback(
    (rowRef: string) => {
      onChange((prev) => ({
        ...prev,
        expandedRowRef: prev.expandedRowRef === rowRef ? null : rowRef,
      }));
    },
    [onChange],
  );

  const moveColumn = (path: string, delta: number) => {
    // 纯视图状态：改顺序不触发查询（查询键用的是排序后的集合）
    onChange((prev) => {
      const idx = prev.columns.indexOf(path);
      const target = idx + delta;
      if (idx < 0 || target < 0 || target >= prev.columns.length) return prev;
      const columns = [...prev.columns];
      const [moved] = columns.splice(idx, 1);
      columns.splice(target, 0, moved!);
      return { ...prev, columns };
    });
  };

  const removeColumn = (path: string) => {
    onChange((prev) => ({ ...prev, columns: prev.columns.filter((c) => c !== path) }));
  };

  return (
    <div className="log-scroller" ref={scrollRef} data-testid="log-scroller">
      <div className="log-header">
        {state.columns.map((path) => (
          <div
            key={path}
            className="cell"
            style={flexFor(path, columnWidthOf(columnWidths, path))}
            title={path}
          >
            <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>
              {COLUMN_LABEL[path] ?? path}
            </span>
            <button
              type="button"
              className="colbtn"
              title="左移（不触发查询）"
              onClick={() => {
                moveColumn(path, -1);
              }}
            >
              ‹
            </button>
            <button
              type="button"
              className="colbtn"
              title="右移（不触发查询）"
              onClick={() => {
                moveColumn(path, 1);
              }}
            >
              ›
            </button>
            <button
              type="button"
              className="colbtn"
              title="移除该列（列集合是查询的一部分，会重新发起查询）"
              onClick={() => {
                removeColumn(path);
              }}
            >
              ×
            </button>
          </div>
        ))}
      </div>

      <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
        {items.map((item) => {
          const row = rows[item.index];
          if (!row) return null;
          const expanded = state.expandedRowRef === row.row_ref;
          return (
            <div
              key={item.key}
              data-index={item.index}
              ref={virtualizer.measureElement}
              style={{
                position: 'absolute',
                top: 0,
                left: 0,
                width: '100%',
                transform: `translateY(${String(item.start)}px)`,
              }}
            >
              <div
                className={`log-row${expanded ? ' expanded' : ''}`}
                role="button"
                tabIndex={0}
                onClick={() => {
                  toggleExpand(row.row_ref);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') toggleExpand(row.row_ref);
                }}
              >
                {state.columns.map((path) => (
                  <div
                    key={path}
                    className={`cell${path === 'body' ? ' body' : ''}`}
                    style={flexFor(path, columnWidthOf(columnWidths, path))}
                  >
                    {renderCell(row, path, state)}
                  </div>
                ))}
              </div>
              {expanded && <LogRowDetail rowRef={row.row_ref} state={state} />}
            </div>
          );
        })}
      </div>

      {isFetchingNextPage && (
        <div style={{ padding: 10 }}>
          <div className="skeleton" />
        </div>
      )}
      {!hasNextPage && rows.length > 0 && (
        <div className="muted" style={{ padding: '10px 12px' }}>
          已到末页（next_cursor 为 null）
        </div>
      )}
    </div>
  );
}

function formatFieldValue(value: unknown): string {
  switch (typeof value) {
    case 'string':
      return value;
    case 'number':
    case 'boolean':
    case 'bigint':
    case 'symbol':
      return value.toString();
    default:
      return JSON.stringify(value);
  }
}

function flexFor(path: string, width: number) {
  return path === 'body'
    ? { flex: 1, minWidth: 200 }
    : { flex: `0 0 ${String(width)}px`, width };
}

/** 决策 5「一切皆可跳转」：trace_id / service / 时间点都渲染成带完整上下文的链接。 */
function renderCell(row: LogRow, path: string, state: LogsViewState) {
  const stop = (e: React.MouseEvent) => {
    e.stopPropagation();
  };
  switch (path) {
    case 'ts':
      return (
        <Link to={logsAroundTimestampLink(row.ts, state)} onClick={stop} title="以该时刻为中心缩窗">
          {formatTimestamp(row.ts)}
        </Link>
      );
    case 'severity':
      return <span className={`sev sev-${row.severity}`}>{row.severity}</span>;
    case 'service':
      return (
        <Link to={logsFilteredByServiceLink(row.service, state)} onClick={stop}>
          {row.service}
        </Link>
      );
    case 'cluster_id':
      return <span>{row.cluster_id}</span>;
    case 'trace_id':
      return row.trace_id ? (
        <Link to={traceDetailLink(row.trace_id, state.global)} onClick={stop} title={row.trace_id}>
          {row.trace_id.slice(0, 12)}…
        </Link>
      ) : (
        <span className="muted">—</span>
      );
    case 'body':
      return <span title={row.body}>{row.body}</span>;
    default: {
      const value = row.fields[path];
      return value === undefined ? (
        <span className="muted">—</span>
      ) : (
        <span>{formatFieldValue(value)}</span>
      );
    }
  }
}
