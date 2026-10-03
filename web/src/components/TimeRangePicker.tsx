import { useEffect, useRef, useState } from 'react';

import type { TimeRange } from '@/api/types';
import { formatTimeRange, isValidTimeRange, TIME_PRESETS } from '@/lib/time';

interface Props {
  value: TimeRange;
  onChange: (next: TimeRange) => void;
}

/**
 * IF-007 §2.2：相对表达与绝对区间都要支持，且原样往返 ——
 * 所以这里编辑的始终是字符串本身（`now-1h`），不在选择器里就地解析成时间戳。
 */
export function TimeRangePicker({ value, onChange }: Props) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState(value);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setDraft(value);
  }, [value]);

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

  const draftValid = isValidTimeRange(draft);

  return (
    <div className="dropdown" ref={ref}>
      <button
        type="button"
        onClick={() => {
          setOpen((o) => !o);
        }}
        aria-label="时间窗"
      >
        🕑 {formatTimeRange(value)} ▾
      </button>
      {open && (
        <div className="dropdown-panel right">
          {TIME_PRESETS.map((preset) => (
            <div
              key={preset.label}
              className="option-row"
              role="button"
              tabIndex={0}
              onClick={() => {
                onChange(preset.range);
                setOpen(false);
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  onChange(preset.range);
                  setOpen(false);
                }
              }}
            >
              <span>{preset.label}</span>
              <span className="muted mono">
                {preset.range.from} → {preset.range.to}
              </span>
            </div>
          ))}
          <hr style={{ borderColor: 'var(--border)' }} />
          <div style={{ display: 'grid', gap: 6 }}>
            <label style={{ display: 'grid', gap: 2 }}>
              <span className="muted">from（支持 now-1h 或 RFC3339）</span>
              <input
                className="mono"
                value={draft.from}
                onChange={(e) => {
                  setDraft({ ...draft, from: e.target.value });
                }}
              />
            </label>
            <label style={{ display: 'grid', gap: 2 }}>
              <span className="muted">to</span>
              <input
                className="mono"
                value={draft.to}
                onChange={(e) => {
                  setDraft({ ...draft, to: e.target.value });
                }}
              />
            </label>
            {!draftValid && <span style={{ color: 'var(--sev-error)' }}>时间窗无法解析</span>}
            <button
              type="button"
              className="primary"
              disabled={!draftValid}
              onClick={() => {
                onChange(draft);
                setOpen(false);
              }}
            >
              应用
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
