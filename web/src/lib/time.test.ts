import { describe, expect, it } from 'vitest';

import { isRelativeTimeExpr, resolveTimeExpr, resolveTimeRange } from './time';

const NOW = Date.parse('2026-10-02T12:00:00.000Z');

describe('时间表达式（IF-007 §2.2）', () => {
  it('解析相对表达', () => {
    expect(resolveTimeExpr('now', NOW)).toBe(NOW);
    expect(resolveTimeExpr('now-15m', NOW)).toBe(NOW - 900_000);
    expect(resolveTimeExpr('now-1h', NOW)).toBe(NOW - 3_600_000);
    expect(resolveTimeExpr('now-7d', NOW)).toBe(NOW - 7 * 86_400_000);
  });

  it('解析 RFC3339 绝对时间', () => {
    expect(resolveTimeExpr('2026-10-02T10:00:00Z', NOW)).toBe(Date.parse('2026-10-02T10:00:00Z'));
  });

  it('无法解析时抛错，而不是静默回退（静默会让坐标轴错得很隐蔽）', () => {
    expect(() => resolveTimeExpr('yesterday', NOW)).toThrow();
  });

  it('区分相对与绝对', () => {
    expect(isRelativeTimeExpr('now-30m')).toBe(true);
    expect(isRelativeTimeExpr('2026-10-02T10:00:00Z')).toBe(false);
  });

  it('混合形式的时间窗也能算出跨度', () => {
    const resolved = resolveTimeRange({ from: '2026-10-02T11:00:00Z', to: 'now' }, NOW);
    expect(resolved.durationMs).toBe(3_600_000);
  });
});
