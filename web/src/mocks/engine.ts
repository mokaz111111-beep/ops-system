import type { LogFilter, Severity } from '@/api/types';
import { resolveTimeExpr } from '@/lib/time';

import {
  allFieldsAt,
  bodyAt,
  clusterAt,
  FIELD_DEFS,
  fieldCatalog,
  fieldValueAt,
  indexRange,
  MOCK_SCAN_LIMIT_ROWS,
  MOCK_TIME_RANGE_LIMIT_MS,
  serviceAt,
  severityAt,
  traceIdAt,
  tsAt,
} from './dataset';
import { MockApiError } from './errors';
import { compileQuery, referencedPaths, type RowAccessor } from './queryLang';

export interface ResolvedScan {
  fromMs: number;
  toMs: number;
  /** 下标降序遍历用：last 是最新的一行。 */
  first: number;
  last: number;
  scannedRows: number;
  matches: (i: number) => boolean;
  /** 查询里命中的长尾（未子列化）路径，用来模拟聚合变慢。 */
  slowPaths: string[];
}

function accessorFor(i: number): RowAccessor {
  return {
    get(path) {
      switch (path) {
        case 'severity':
          return severityAt(i);
        case 'service':
          return serviceAt(i);
        case 'cluster_id':
          return clusterAt(i).id;
        case 'project_id':
          return clusterAt(i).project;
        case 'trace_id':
          return traceIdAt(i);
        case 'ts':
          return new Date(tsAt(i)).toISOString();
        case 'body':
          return bodyAt(i, severityAt(i), serviceAt(i));
        default: {
          const def = FIELD_DEFS.find((d) => d.path === path);
          return def ? fieldValueAt(i, def) : undefined;
        }
      }
    },
    body: () => bodyAt(i, severityAt(i), serviceAt(i)),
  };
}

export function rowAccessor(i: number): RowAccessor {
  return accessorFor(i);
}

/**
 * 检索 / 直方图 / Facet / 聚合共用这一个解析入口，对应 IF-007 §2.3
 * 「四类接口共享同一份过滤条件结构」—— mock 侧也只能有一份实现，否则就和契约一起漂移了。
 */
export function resolveScan(filter: Partial<LogFilter> | undefined, now = Date.now()): ResolvedScan {
  if (!filter?.time_range) {
    throw new MockApiError('TIME_RANGE_REQUIRED', '查询必须显式携带时间窗，服务端不提供默认值');
  }
  let fromMs: number;
  let toMs: number;
  try {
    fromMs = resolveTimeExpr(filter.time_range.from, now);
    toMs = resolveTimeExpr(filter.time_range.to, now);
  } catch (err) {
    throw new MockApiError('INVALID_PARAM', `时间窗无法解析: ${String(err)}`);
  }
  if (!(toMs > fromMs)) {
    throw new MockApiError('INVALID_PARAM', '时间窗的 to 必须晚于 from');
  }
  if (toMs - fromMs > MOCK_TIME_RANGE_LIMIT_MS) {
    throw new MockApiError('TIME_RANGE_TOO_LARGE', '时间窗超过本接口上限（30 天）', {
      details: { requested_ms: toMs - fromMs, limit_ms: MOCK_TIME_RANGE_LIMIT_MS },
    });
  }

  const { first, last } = indexRange(fromMs, toMs);
  const scannedRows = Math.max(0, last - first + 1);
  if (scannedRows > MOCK_SCAN_LIMIT_ROWS) {
    throw new MockApiError('SCAN_LIMIT_EXCEEDED', '查询扫描量超过单次上限', {
      details: { scanned_rows: scannedRows, limit: MOCK_SCAN_LIMIT_ROWS },
    });
  }

  const catalog = new Set(fieldCatalog(now).map((f) => f.path));
  const referenced = referencedPaths(filter.query);
  for (const path of referenced) {
    if (!catalog.has(path)) {
      throw new MockApiError(
        'FIELD_PATH_NOT_FOUND',
        `字段路径 ${path} 在当前时间窗内没有数据`,
        { details: { field_path: path } },
      );
    }
  }

  const predicate = compileQuery(filter.query);
  const clusterIds = filter.cluster_ids?.length ? new Set(filter.cluster_ids) : null;
  const projectIds = filter.project_ids?.length ? new Set(filter.project_ids) : null;
  const severities = filter.severity?.length ? new Set<Severity>(filter.severity) : null;

  const slowPaths = referenced.filter((p) => {
    const def = FIELD_DEFS.find((d) => d.path === p);
    return def ? !def.subcolumnized : false;
  });

  return {
    fromMs,
    toMs,
    first,
    last,
    scannedRows,
    slowPaths,
    matches(i) {
      const cluster = clusterAt(i);
      if (clusterIds && !clusterIds.has(cluster.id)) return false;
      if (projectIds && !projectIds.has(cluster.project)) return false;
      if (severities && !severities.has(severityAt(i))) return false;
      return predicate(accessorFor(i));
    },
  };
}

export function assertPathsExist(paths: readonly string[], now = Date.now()): void {
  const catalog = new Set(fieldCatalog(now).map((f) => f.path));
  for (const path of paths) {
    if (!catalog.has(path)) {
      throw new MockApiError('FIELD_PATH_NOT_FOUND', `字段路径 ${path} 在当前时间窗内没有数据`, {
        details: { field_path: path },
      });
    }
  }
}

// ---------------------------------------------------------------------------
// 游标：不透明、带有效期（IF-007 §2.4）
// ---------------------------------------------------------------------------

const DEFAULT_CURSOR_TTL_SECONDS = 300;

export function cursorTtlSeconds(): number {
  const raw = import.meta.env.VITE_MOCK_CURSOR_TTL_SECONDS;
  const parsed = raw ? Number(raw) : NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_CURSOR_TTL_SECONDS;
}

interface CursorPayload {
  /** 下一页从这个下标继续往下（时间降序）。 */
  next: number;
  /** 过滤条件指纹：条件变了就不能复用游标。 */
  fp: string;
  /** 签发时间，用于过期判定。 */
  at: number;
}

export function fingerprint(value: unknown): string {
  const json = JSON.stringify(value) ?? '';
  let h = 2166136261;
  for (let i = 0; i < json.length; i += 1) {
    h = Math.imul(h ^ json.charCodeAt(i), 16777619);
  }
  return (h >>> 0).toString(36);
}

export function encodeCursor(payload: CursorPayload): string {
  return btoa(JSON.stringify(payload)).replace(/=+$/, '');
}

export function decodeCursor(cursor: string, expectedFp: string, now = Date.now()): number {
  let payload: CursorPayload;
  try {
    payload = JSON.parse(atob(cursor)) as CursorPayload;
  } catch {
    throw new MockApiError('CURSOR_EXPIRED', '游标无法解析，请重新发起查询');
  }
  if (now - payload.at > cursorTtlSeconds() * 1000) {
    throw new MockApiError('CURSOR_EXPIRED', '翻页游标已过期（有效期 5 分钟），请重新发起查询', {
      details: { issued_at: new Date(payload.at).toISOString() },
    });
  }
  if (payload.fp !== expectedFp) {
    throw new MockApiError('CURSOR_EXPIRED', '过滤条件已变更，游标不再适用，请重新发起查询');
  }
  return payload.next;
}

export { allFieldsAt };
