import { useEffect, useState } from 'react';

import { SEVERITIES } from '@/api/types';
import type { LogsViewState } from '@/state/urlState';

interface Props {
  state: LogsViewState;
  onChange: (updater: (prev: LogsViewState) => LogsViewState) => void;
  busy: boolean;
}

/**
 * IF-007 §2.3：`query` 的语法形态尚未定稿，契约刻意把它定成不透明字符串。
 * 所以这里**不做任何解析、不做补全、不实现 KQL**，只把用户输入原样透传，
 * 结构化的维度收敛交给 severity chips 与 facet 侧栏 —— 三者共享同一份过滤条件结构。
 */
export function QueryBar({ state, onChange, busy }: Props) {
  const [draft, setDraft] = useState(state.query);

  useEffect(() => {
    setDraft(state.query);
  }, [state.query]);

  const submit = () => {
    onChange((prev) => ({ ...prev, query: draft, expandedRowRef: null }));
  };

  return (
    <div className="query-bar">
      <input
        className="query-input"
        placeholder='检索串（语法未定稿，当前按不透明串透传）：如 http.status_code:"500"'
        aria-label="查询串"
        value={draft}
        onChange={(e) => {
          setDraft(e.target.value);
        }}
        onKeyDown={(e) => {
          if (e.key === 'Enter') submit();
        }}
      />
      <button type="button" className="primary" onClick={submit} disabled={busy}>
        {busy ? '查询中…' : '查询'}
      </button>

      <div className="severity-chips" role="group" aria-label="severity 过滤">
        {SEVERITIES.map((sev) => {
          const on = state.severity.includes(sev);
          return (
            <button
              key={sev}
              type="button"
              className={`chip${on ? ' on' : ''} sev-${sev}`}
              aria-pressed={on}
              onClick={() => {
                onChange((prev) => ({
                  ...prev,
                  severity: on
                    ? prev.severity.filter((s) => s !== sev)
                    : [...prev.severity, sev],
                  expandedRowRef: null,
                }));
              }}
            >
              {sev}
            </button>
          );
        })}
      </div>

      {/* 模式切换：双模式（类 KQL / SQL WHERE）的语法决策与 DD-005 共有，见 DD-008 §4 */}
      <select disabled title="查询框双模式待 DD-005 共同定稿（DD-008 §4 Open Questions）">
        <option>关键字模式</option>
      </select>
    </div>
  );
}
