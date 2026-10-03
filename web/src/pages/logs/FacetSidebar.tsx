import { useMemo, useState } from 'react';

import { useFieldCatalog } from '@/api/queries';
import type { FieldDescriptor } from '@/api/types';
import { ErrorCallout } from '@/components/ErrorCallout';
import { toggleFacetValue } from '@/lib/links';
import { resolveTimeRange, toAbsoluteExpr } from '@/lib/time';
import type { LogsViewState } from '@/state/urlState';
import { useViewState } from '@/state/viewState';

import { FacetGroup } from './FacetGroup';

interface Props {
  state: LogsViewState;
  onChange: (updater: (prev: LogsViewState) => LogsViewState) => void;
}

/** 这些路径做 facet 没有意义（时间本身由直方图负责，body 是自由文本）。 */
const NON_FACETABLE = new Set(['ts', 'body', 'row_ref']);

const DEFAULT_EXPANDED = ['service', 'severity', 'cluster_id'];

/**
 * DD-008 §3.4「Facet 免注册」：字段目录（FE-06）是 facet 列表的唯一数据源，
 * 平台声明字段（declared）置顶，长尾字段标注「低频字段，聚合较慢」。
 * 没有这两条标注，免注册的红利会退化成随机踩坑。
 */
export function FacetSidebar({ state, onChange }: Props) {
  const catalog = useFieldCatalog('logs', state.global.timeRange, '');
  const [filter, setFilter] = useState('');
  const [expanded, setExpanded] = useState<string[]>(DEFAULT_EXPANDED);
  const pinned = useViewState((s) => s.pinnedFacets);

  const groups = useMemo(() => {
    const fields = (catalog.data?.fields ?? []).filter(
      (f) => !NON_FACETABLE.has(f.path) && f.path.toLowerCase().includes(filter.toLowerCase()),
    );
    const rank = (f: FieldDescriptor) => {
      if (pinned.includes(f.path)) return 0;
      if (f.declared) return 1;
      if (f.subcolumnized) return 2;
      return 3; // 长尾字段沉底
    };
    return [...fields].sort((a, b) => rank(a) - rank(b) || a.path.localeCompare(b.path));
  }, [catalog.data, filter, pinned]);

  const narrowTimeRange = () => {
    const { fromMs, toMs } = resolveTimeRange(state.global.timeRange);
    const mid = fromMs + (toMs - fromMs) / 2;
    const half = (toMs - fromMs) / 4;
    onChange((prev) => ({
      ...prev,
      global: {
        ...prev.global,
        timeRange: { from: toAbsoluteExpr(mid - half), to: toAbsoluteExpr(mid + half) },
      },
    }));
  };

  return (
    <aside className="facet-sidebar" aria-label="Facet 侧栏">
      <h3>Facets</h3>
      <div style={{ padding: '0 12px 8px' }}>
        <input
          style={{ width: '100%' }}
          placeholder="过滤字段路径…"
          aria-label="过滤字段路径"
          value={filter}
          onChange={(e) => {
            setFilter(e.target.value);
          }}
        />
        {catalog.data && (
          <div className="muted" style={{ fontSize: 11, marginTop: 4 }}>
            字段目录缓存 {catalog.data.catalog_age_seconds}s 前生成，非实时
          </div>
        )}
      </div>

      {catalog.isLoading && (
        <div style={{ padding: '0 12px' }}>
          <div className="skeleton" style={{ margin: '6px 0' }} />
          <div className="skeleton" style={{ margin: '6px 0', width: '60%' }} />
        </div>
      )}
      {catalog.error && (
        <div style={{ padding: '0 8px' }}>
          <ErrorCallout
            error={catalog.error}
            variant="warn"
            actions={{ NARROW_TIME_RANGE: narrowTimeRange }}
          />
        </div>
      )}

      {groups.map((field) => (
        <FacetGroup
          key={field.path}
          field={field}
          state={state}
          expanded={expanded.includes(field.path)}
          onToggleExpand={() => {
            setExpanded((prev) =>
              prev.includes(field.path)
                ? prev.filter((p) => p !== field.path)
                : [...prev, field.path],
            );
          }}
          onToggleValue={(value) => {
            onChange((prev) => ({
              ...prev,
              facets: toggleFacetValue(prev.facets, field.path, value),
              expandedRowRef: null,
            }));
          }}
          onNarrowTimeRange={narrowTimeRange}
        />
      ))}
    </aside>
  );
}
