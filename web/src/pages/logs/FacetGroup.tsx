import { useLogFacet } from '@/api/queries';
import type { FieldDescriptor } from '@/api/types';
import { ErrorCallout } from '@/components/ErrorCallout';
import type { LogsViewState } from '@/state/urlState';

interface Props {
  field: FieldDescriptor;
  state: LogsViewState;
  expanded: boolean;
  onToggleExpand: () => void;
  onToggleValue: (value: string) => void;
  onNarrowTimeRange: () => void;
}

export function FacetGroup({
  field,
  state,
  expanded,
  onToggleExpand,
  onToggleValue,
  onNarrowTimeRange,
}: Props) {
  const facet = useLogFacet(state, field.path, expanded);
  const selected = state.facets.find((f) => f.path === field.path)?.values ?? [];

  // declared / subcolumnized 两个标志是存储层的物理事实，前端判断不出来，
  // 必须由 IF-7 透出（DD-008 §3.4「Facet 免注册」）。facet 返回里也重复带了一份，
  // 优先用返回值 —— subcolumnized 是运行时状态，会漂移，目录可能已经过期。
  const subcolumnized = facet.data?.subcolumnized ?? field.subcolumnized;

  return (
    <div className="facet-group">
      <button type="button" className="facet-head" onClick={onToggleExpand} aria-expanded={expanded}>
        <span>{expanded ? '▾' : '▸'}</span>
        <span className="facet-path" title={field.path}>
          {field.path}
        </span>
        {selected.length > 0 && <span className="facet-badge">{selected.length}</span>}
        {!subcolumnized && (
          <span className="facet-badge slow" title="该路径未被子列化，聚合要走通用路径">
            低频字段，聚合较慢
          </span>
        )}
      </button>

      {expanded && (
        <div className="facet-values">
          {facet.isLoading && (
            <>
              <div className="skeleton" style={{ margin: '4px 0' }} />
              <div className="skeleton" style={{ margin: '4px 0', width: '70%' }} />
            </>
          )}

          {facet.error && (
            <ErrorCallout
              error={facet.error}
              variant="warn"
              actions={{ NARROW_TIME_RANGE: onNarrowTimeRange }}
            />
          )}

          {facet.data?.values.map((v) => {
            const on = selected.includes(v.value);
            return (
              <div
                key={v.value}
                className={`facet-value${on ? ' selected' : ''}`}
                role="button"
                tabIndex={0}
                onClick={() => {
                  onToggleValue(v.value);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') onToggleValue(v.value);
                }}
              >
                <span>{on ? '☑' : '☐'}</span>
                <span className="v" title={v.value}>
                  {v.value}
                </span>
                <span className="c">{v.count.toLocaleString('zh-CN')}</span>
              </div>
            );
          })}

          {facet.data && facet.data.values.length === 0 && (
            <div className="muted">当前条件下无取值</div>
          )}
          {facet.data?.truncated && (
            <div className="muted" style={{ fontSize: 11 }}>
              已截断，distinct 估算 {facet.data.distinct_estimate ?? '—'}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
