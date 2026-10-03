import type { TimeRange } from '@/api/types';

/**
 * IF-007 §2.2：时间窗支持绝对区间（RFC3339）与相对表达（now-1h）两种形式，
 * 且「在保存视图与分享链接中原样往返」—— 相对形式不得在序列化时被解析成绝对值，
 * 否则巡检类视图每次打开都停在创建那天。
 *
 * 因此本模块严格区分两件事：
 *   - 传给后端 / 写进 URL 的，永远是用户给的原始字符串；
 *   - 只有「画坐标轴、算桶宽、判断时间窗大小」这类本地计算才 resolve 成毫秒。
 */

const RELATIVE_RE = /^now(?:\s*-\s*(\d+)\s*([smhdw]))?$/i;

const UNIT_MS: Record<string, number> = {
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
  w: 604_800_000,
};

export function isRelativeTimeExpr(expr: string): boolean {
  return RELATIVE_RE.test(expr.trim());
}

/** 把一个时间表达式解析成毫秒时间戳。无法解析时抛错 —— 静默回退会让坐标轴错得很隐蔽。 */
export function resolveTimeExpr(expr: string, now: number = Date.now()): number {
  const trimmed = expr.trim();
  const relative = RELATIVE_RE.exec(trimmed);
  if (relative) {
    const amount = relative[1];
    const unit = relative[2];
    if (amount === undefined || unit === undefined) return now;
    const unitMs = UNIT_MS[unit.toLowerCase()];
    if (unitMs === undefined) throw new Error(`无法识别的时间单位: ${unit}`);
    return now - Number(amount) * unitMs;
  }
  const absolute = Date.parse(trimmed);
  if (Number.isNaN(absolute)) throw new Error(`无法解析的时间表达式: ${expr}`);
  return absolute;
}

export interface ResolvedRange {
  fromMs: number;
  toMs: number;
  durationMs: number;
}

export function resolveTimeRange(range: TimeRange, now: number = Date.now()): ResolvedRange {
  const fromMs = resolveTimeExpr(range.from, now);
  const toMs = resolveTimeExpr(range.to, now);
  return { fromMs, toMs, durationMs: Math.max(0, toMs - fromMs) };
}

export function isValidTimeRange(range: TimeRange): boolean {
  try {
    const { fromMs, toMs } = resolveTimeRange(range);
    return toMs > fromMs;
  } catch {
    return false;
  }
}

export interface TimePreset {
  label: string;
  range: TimeRange;
}

export const TIME_PRESETS: readonly TimePreset[] = [
  { label: '最近 15 分钟', range: { from: 'now-15m', to: 'now' } },
  { label: '最近 1 小时', range: { from: 'now-1h', to: 'now' } },
  { label: '最近 4 小时', range: { from: 'now-4h', to: 'now' } },
  { label: '最近 24 小时', range: { from: 'now-24h', to: 'now' } },
  { label: '最近 7 天', range: { from: 'now-7d', to: 'now' } },
];

export function formatTimeRange(range: TimeRange): string {
  const preset = TIME_PRESETS.find((p) => p.range.from === range.from && p.range.to === range.to);
  if (preset) return preset.label;
  if (isRelativeTimeExpr(range.from) && isRelativeTimeExpr(range.to)) {
    return `${range.from} → ${range.to}`;
  }
  return `${formatAbsolute(range.from)} → ${formatAbsolute(range.to)}`;
}

function formatAbsolute(expr: string): string {
  const ms = Date.parse(expr);
  return Number.isNaN(ms) ? expr : new Date(ms).toLocaleString('zh-CN', { hour12: false });
}

export function formatTimestamp(iso: string): string {
  const ms = Date.parse(iso);
  if (Number.isNaN(ms)) return iso;
  const d = new Date(ms);
  const pad = (n: number, width = 2) => String(n).padStart(width, '0');
  return (
    `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.` +
    `${pad(d.getMilliseconds(), 3)}`
  );
}

/** 把毫秒时间戳转成可写进 TimeRange 的绝对表达（框选缩窗后用）。 */
export function toAbsoluteExpr(ms: number): string {
  return new Date(ms).toISOString();
}

export function humanizeDuration(ms: number): string {
  if (ms < 60_000) return `${String(Math.round(ms / 1000))} 秒`;
  if (ms < 3_600_000) return `${String(Math.round(ms / 60_000))} 分钟`;
  if (ms < 86_400_000) return `${(ms / 3_600_000).toFixed(1)} 小时`;
  return `${(ms / 86_400_000).toFixed(1)} 天`;
}
