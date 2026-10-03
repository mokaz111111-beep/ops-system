import { useMemo, useState } from 'react';

import { useFieldCatalog } from '@/api/queries';
import type { LogsViewState } from '@/state/urlState';
import { DEFAULT_COLUMNS } from '@/state/urlState';

interface Props {
  state: LogsViewState;
  onClose: () => void;
  onChangeColumns: (columns: string[]) => void;
}

/**
 * DD-008 §3.4 列配置交互的四条结论在这里落地：
 *   结论 1 增删列重新发起查询（调用方把 columns 写回 URL → 查询键变化）；
 *   结论 2 默认列是一个具体的路径清单（DEFAULT_COLUMNS），不是「不传 select」；
 *   结论 3 加列入口是字段目录（FE-06），手输只是高级用法，且拒绝要渲染成可操作提示；
 *   结论 4 保存视图必须包含列集合（列集合已在 URL 里，见 urlState）。
 */
export function ColumnPicker({ state, onClose, onChangeColumns }: Props) {
  const catalog = useFieldCatalog('logs', state.global.timeRange, '');
  const [filter, setFilter] = useState('');
  const [manual, setManual] = useState('');
  const [unknownPath, setUnknownPath] = useState<string | null>(null);

  const fields = useMemo(() => {
    const all = catalog.data?.fields ?? [];
    const kw = filter.trim().toLowerCase();
    return all.filter((f) => (kw ? f.path.toLowerCase().includes(kw) : true));
  }, [catalog.data, filter]);

  const known = useMemo(
    () => new Set((catalog.data?.fields ?? []).map((f) => f.path)),
    [catalog.data],
  );

  const toggle = (path: string) => {
    onChangeColumns(
      state.columns.includes(path)
        ? state.columns.filter((c) => c !== path)
        : [...state.columns, path],
    );
  };

  const tryManualAdd = () => {
    const path = manual.trim();
    if (!path) return;
    if (state.columns.includes(path)) {
      setManual('');
      return;
    }
    if (!known.has(path)) {
      setUnknownPath(path);
      return;
    }
    onChangeColumns([...state.columns, path]);
    setManual('');
  };

  return (
    <div className="modal-backdrop" role="dialog" aria-modal="true" aria-label="列配置">
      <div className="modal">
        <header>列配置 · 字段目录（FE-06）</header>
        <div className="modal-body">
          <p className="muted" style={{ marginTop: 0 }}>
            增删列会<strong>重新发起查询</strong>（列集合是查询的一部分）；调整顺序与列宽只是视图状态。
            ts / service / cluster_id / trace_id 无论是否显示都会被请求，否则行内跳转链接无从构造。
          </p>

          <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
            <input
              style={{ flex: 1 }}
              placeholder="搜索字段路径…"
              aria-label="搜索字段路径"
              value={filter}
              onChange={(e) => {
                setFilter(e.target.value);
              }}
            />
            <button
              type="button"
              onClick={() => {
                onChangeColumns([...DEFAULT_COLUMNS]);
              }}
            >
              恢复默认列
            </button>
          </div>

          {catalog.isLoading && <div className="skeleton" />}

          {fields.map((f) => (
            <label className="field-row" key={f.path}>
              <input
                type="checkbox"
                checked={state.columns.includes(f.path)}
                onChange={() => {
                  toggle(f.path);
                }}
              />
              <span className="path">{f.path}</span>
              <span className="type">{f.type}</span>
              {f.declared && <span className="facet-badge">声明字段</span>}
              {!f.subcolumnized && <span className="facet-badge slow">未子列化</span>}
            </label>
          ))}

          <hr style={{ borderColor: 'var(--border)', margin: '12px 0' }} />
          <div style={{ display: 'flex', gap: 8 }}>
            <input
              style={{ flex: 1 }}
              className="mono"
              placeholder="高级用法：手输字段路径"
              aria-label="手输字段路径"
              value={manual}
              onChange={(e) => {
                setManual(e.target.value);
                setUnknownPath(null);
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') tryManualAdd();
              }}
            />
            <button type="button" onClick={tryManualAdd}>
              添加
            </button>
          </div>

          {unknownPath && (
            <div className="callout warn" style={{ marginTop: 8 }}>
              <div className="callout-body">
                <div className="callout-title">
                  该路径在当前时间窗内没有数据，是否仍要添加？
                </div>
                <div className="muted mono">{unknownPath}</div>
                <div className="muted" style={{ marginTop: 4 }}>
                  字段目录由服务端缓存（{catalog.data?.catalog_age_seconds ?? '—'} 秒前生成），
                  不是实时的；路径确实不存在时，后端会以 FIELD_PATH_NOT_FOUND 拒绝这次查询。
                </div>
                <div className="callout-actions">
                  <button
                    type="button"
                    className="primary"
                    onClick={() => {
                      onChangeColumns([...state.columns, unknownPath]);
                      setManual('');
                      setUnknownPath(null);
                    }}
                  >
                    仍要添加
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setUnknownPath(null);
                    }}
                  >
                    取消
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
        <footer>
          <button type="button" onClick={onClose}>
            完成
          </button>
        </footer>
      </div>
    </div>
  );
}
