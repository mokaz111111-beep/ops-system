import { Link } from 'react-router-dom';

import { useLogRow } from '@/api/queries';
import { ErrorCallout } from '@/components/ErrorCallout';
import { logsAroundTimestampLink, logsFilteredByServiceLink, traceDetailLink } from '@/lib/links';
import { formatTimestamp } from '@/lib/time';
import type { LogsViewState } from '@/state/urlState';
import { useViewState } from '@/state/viewState';

interface Props {
  rowRef: string;
  state: LogsViewState;
}

/**
 * DD-008 §3.4「展开单条看完整属性树」：
 * 列表页的行只带投影出的路径，完整文档不在其中，所以展开 = 一次返回 1 行的详情请求
 * （FE-07 / IF-007 §4.6），不是纯前端展开。有网络延迟，因此必须有骨架态。
 */
export function LogRowDetail({ rowRef, state }: Props) {
  const detail = useLogRow(rowRef);
  const togglePinnedFacet = useViewState((s) => s.togglePinnedFacet);
  const pinnedFacets = useViewState((s) => s.pinnedFacets);

  if (detail.isLoading) {
    return (
      <div className="row-detail">
        <div className="skeleton" style={{ width: '45%', marginBottom: 8 }} />
        <div className="skeleton" style={{ marginBottom: 6 }} />
        <div className="skeleton" style={{ width: '80%', marginBottom: 6 }} />
        <div className="skeleton" style={{ width: '62%' }} />
      </div>
    );
  }

  if (detail.error) {
    return (
      <div className="row-detail">
        <ErrorCallout error={detail.error} actions={{ RETRY_AFTER: () => void detail.refetch() }} />
      </div>
    );
  }

  const row = detail.data?.row;
  if (!row) return null;

  const attrSections: { title: string; entries: [string, unknown][] }[] = [
    { title: 'log attributes', entries: Object.entries(row.log_attributes ?? {}) },
    { title: 'resource attributes', entries: Object.entries(row.resource_attributes ?? {}) },
  ];

  return (
    <div className="row-detail">
      <h4>body</h4>
      <div className="mono" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
        {row.body}
      </div>

      {attrSections.map((section) => (
        <div key={section.title}>
          <h4>{section.title}</h4>
          <div className="attr-tree">
            {section.entries.length === 0 && <span className="muted">（空）</span>}
            {section.entries.map(([k, v]) => (
              <div className="attr-line" key={k}>
                <span className="k">{k}</span>
                <span className="v">{formatValue(v)}</span>
                <button
                  type="button"
                  className="ghost"
                  title="把该路径提升为常驻 Facet（DD-008 §3.4「创建 Facet」）"
                  onClick={() => {
                    togglePinnedFacet(k);
                  }}
                >
                  {pinnedFacets.includes(k) ? '取消常驻' : '+ Facet'}
                </button>
              </div>
            ))}
          </div>
        </div>
      ))}

      <div className="detail-actions">
        {row.trace_id ? (
          <Link className="chip" to={traceDetailLink(row.trace_id, state.global)}>
            跳转 Trace
          </Link>
        ) : (
          <span className="chip muted">本行无 trace_id</span>
        )}
        <Link className="chip" to={logsFilteredByServiceLink(row.service, state)}>
          只看 {row.service}
        </Link>
        <Link className="chip" to={logsAroundTimestampLink(row.ts, state)}>
          {formatTimestamp(row.ts)} 前后 ±5 分钟
        </Link>

        {/* 以下三个按钮位置按 DD-008 §3.4 的草图留出，但不实现：
            上下文 ±50 条的行序语义、保存视图与创建告警的归属，都还是 §4 的 Open Question。 */}
        <button
          type="button"
          disabled
          title="DD-008 §4：单行定位键已由 IF-007 §5.1 定为 row_ref，但「上下文」的行序语义（同毫秒多行排序、跨 tablet 顺序）仍未有结论，/logs/context 会返回 order_guaranteed=false"
        >
          上下文 ±50 条
        </button>
        <button
          type="button"
          disabled
          title="DD-008 §4 / IF-007 §6：保存视图的持久化归属（IF-7 vs 控制面）未定"
        >
          保存视图
        </button>
        <button
          type="button"
          disabled
          title="FE-11 排在 M3，且要求检索语法与告警规则条件语法一致，而语法尚未定稿"
        >
          创建告警
        </button>
      </div>

      <div className="muted mono" style={{ marginTop: 8, fontSize: 11 }}>
        row_ref: {row.row_ref} · query_id: {detail.data?.query_id}
      </div>
    </div>
  );
}

function formatValue(value: unknown): string {
  switch (typeof value) {
    case 'string':
      return value;
    case 'number':
    case 'boolean':
    case 'bigint':
    case 'symbol':
      return value.toString();
    case 'undefined':
      return 'undefined';
    default:
      return JSON.stringify(value);
  }
}
