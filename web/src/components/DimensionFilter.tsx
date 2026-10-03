import { useEffect, useRef, useState } from 'react';

import type { DimensionValue } from '@/api/types';

interface Props {
  label: string;
  options: DimensionValue[];
  selected: string[];
  loading?: boolean;
  onChange: (next: string[]) => void;
}

/** last_seen 超过这个阈值即视为已失活，灰显（IF-007 §4.1 的用途说明）。 */
const STALE_AFTER_MS = 24 * 3_600_000;

export function DimensionFilter({ label, options, selected, loading, onChange }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDocClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDocClick);
    return () => {
      document.removeEventListener('mousedown', onDocClick);
    };
  }, [open]);

  const summary = selected.length === 0 ? '全部' : `${selected.length} 项`;

  return (
    <div className="dropdown" ref={ref}>
      <button
        type="button"
        onClick={() => {
          setOpen((o) => !o);
        }}
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        {label}: {summary} ▾
      </button>
      {open && (
        <div className="dropdown-panel" role="listbox">
          {loading && <div className="muted">加载中…</div>}
          {!loading && options.length === 0 && <div className="muted">当前时间窗内没有数据</div>}
          {options.map((opt) => {
            const stale = opt.last_seen
              ? Date.now() - Date.parse(opt.last_seen) > STALE_AFTER_MS
              : false;
            const checked = selected.includes(opt.value);
            return (
              <label key={opt.value} className={`option-row${stale ? ' stale' : ''}`}>
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => {
                    onChange(
                      checked
                        ? selected.filter((v) => v !== opt.value)
                        : [...selected, opt.value],
                    );
                  }}
                />
                <span className="mono">{opt.value}</span>
                {stale && <span className="muted">· 已失活</span>}
              </label>
            );
          })}
          {selected.length > 0 && (
            <button
              type="button"
              className="ghost"
              onClick={() => {
                onChange([]);
              }}
            >
              清空
            </button>
          )}
        </div>
      )}
    </div>
  );
}
