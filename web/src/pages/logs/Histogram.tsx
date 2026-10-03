import { useMemo, useRef, useState } from 'react';

import type { HistogramBucket, Severity } from '@/api/types';
import { SEVERITIES } from '@/api/types';
import { humanizeDuration } from '@/lib/time';

const VB_W = 1000;
const VB_H = 110;

const SEVERITY_COLOR: Record<Severity, string> = {
  TRACE: 'var(--sev-trace)',
  DEBUG: 'var(--sev-debug)',
  INFO: 'var(--sev-info)',
  WARN: 'var(--sev-warn)',
  ERROR: 'var(--sev-error)',
  FATAL: 'var(--sev-fatal)',
};

interface Props {
  buckets: HistogramBucket[];
  interval: string;
  downsampled: boolean;
  fromMs: number;
  toMs: number;
  loading: boolean;
  /** 框选缩窗：把新的绝对时间窗写回全局上下文（DD-008 §3.4）。 */
  onBrush: (fromMs: number, toMs: number) => void;
}

interface Column {
  ts: number;
  total: number;
  parts: { severity: Severity; count: number }[];
}

export function Histogram({
  buckets,
  interval,
  downsampled,
  fromMs,
  toMs,
  loading,
  onBrush,
}: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [drag, setDrag] = useState<{ a: number; b: number } | null>(null);

  const { columns, max, barWidth } = useMemo(() => {
    const byTs = new Map<number, Column>();
    for (const b of buckets) {
      const ts = Date.parse(b.ts_start);
      if (Number.isNaN(ts)) continue;
      const col = byTs.get(ts) ?? { ts, total: 0, parts: [] };
      col.total += b.count;
      col.parts.push({ severity: b.severity ?? 'INFO', count: b.count });
      byTs.set(ts, col);
    }
    const cols = [...byTs.values()].sort((a, b) => a.ts - b.ts);
    for (const col of cols) {
      col.parts.sort((a, b) => SEVERITIES.indexOf(a.severity) - SEVERITIES.indexOf(b.severity));
    }
    const span = Math.max(1, toMs - fromMs);
    const step = cols.length > 1 ? (cols[1]!.ts - cols[0]!.ts) / span : 1 / 60;
    return {
      columns: cols,
      max: Math.max(1, ...cols.map((c) => c.total)),
      barWidth: Math.max(1.2, step * VB_W * 0.86),
    };
  }, [buckets, fromMs, toMs]);

  const xOf = (ts: number) => ((ts - fromMs) / Math.max(1, toMs - fromMs)) * VB_W;
  const tsOf = (clientX: number) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return fromMs;
    const frac = Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
    return fromMs + frac * (toMs - fromMs);
  };

  const total = columns.reduce((sum, c) => sum + c.total, 0);
  const selection =
    drag && Math.abs(drag.b - drag.a) > 1
      ? { from: Math.min(drag.a, drag.b), to: Math.max(drag.a, drag.b) }
      : null;

  return (
    <div className="histogram">
      <div className="histogram-head">
        <span>
          共 {total.toLocaleString('zh-CN')} 条 · 粒度 {interval}
          {downsampled && ' · 已降采样'}
        </span>
        <span className="histogram-legend">
          {SEVERITIES.map((s) => (
            <span key={s}>
              <span className="swatch" style={{ background: SEVERITY_COLOR[s] }} />
              {s}
            </span>
          ))}
        </span>
        <span className="spacer" style={{ flex: 1 }} />
        <span>{loading ? '刷新中…' : '拖拽框选可缩小时间窗'}</span>
      </div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${String(VB_W)} ${String(VB_H)}`}
        preserveAspectRatio="none"
        height={VB_H}
        role="img"
        aria-label="日志量直方图（按 severity 堆叠）"
        onMouseDown={(e) => {
          const ts = tsOf(e.clientX);
          setDrag({ a: ts, b: ts });
        }}
        onMouseMove={(e) => {
          if (drag) setDrag({ ...drag, b: tsOf(e.clientX) });
        }}
        onMouseUp={() => {
          if (selection && selection.to - selection.from > 1000) {
            onBrush(Math.round(selection.from), Math.round(selection.to));
          }
          setDrag(null);
        }}
        onMouseLeave={() => {
          setDrag(null);
        }}
      >
        {columns.map((col) => {
          let y = VB_H;
          return (
            <g key={col.ts}>
              {col.parts.map((part) => {
                const h = (part.count / max) * (VB_H - 6);
                y -= h;
                return (
                  <rect
                    key={part.severity}
                    x={xOf(col.ts)}
                    y={y}
                    width={barWidth}
                    height={h}
                    fill={SEVERITY_COLOR[part.severity]}
                  >
                    <title>
                      {new Date(col.ts).toLocaleString('zh-CN', { hour12: false })} · {part.severity}{' '}
                      {part.count}
                    </title>
                  </rect>
                );
              })}
            </g>
          );
        })}
        {selection && (
          <rect
            x={xOf(selection.from)}
            y={0}
            width={Math.max(1, xOf(selection.to) - xOf(selection.from))}
            height={VB_H}
            fill="var(--accent)"
            opacity={0.22}
          />
        )}
      </svg>
      <div className="histogram-legend" style={{ justifyContent: 'space-between' }}>
        <span>{new Date(fromMs).toLocaleString('zh-CN', { hour12: false })}</span>
        <span>跨度 {humanizeDuration(toMs - fromMs)}</span>
        <span>{new Date(toMs).toLocaleString('zh-CN', { hour12: false })}</span>
      </div>
    </div>
  );
}
