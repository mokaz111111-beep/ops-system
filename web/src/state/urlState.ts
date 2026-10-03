import type { LogFilter, Severity, TimeRange } from '@/api/types';
import { isAlwaysReturnedPath, SEVERITIES } from '@/api/types';

/**
 * DD-008 §3.2 的硬要求：页面状态必须可完整序列化进 URL
 * （时间窗、cluster/project 过滤、查询语句、facet 选择、列集合）。
 *
 * 这一条同时服务三件事 —— 跳转不丢上下文、保存视图（§3.4）、AI 证据卡片回放（§3.6）——
 * 三者本质是同一个能力，所以 URL 就是页面状态的唯一真相源，不另设内存副本。
 *
 * 不进 URL 的只有纯视图状态：列宽、facet 分区展开/折叠（见 state/viewState.ts）。
 * 列顺序是个例外 —— 它进 URL（分享链接要还原视觉顺序），但不进查询键（DD-008 §3.4 结论 1）。
 */

export interface GlobalContext {
  /** 原样往返：相对表达不在这里被解析成绝对值（IF-007 §2.2）。 */
  timeRange: TimeRange;
  clusterIds: string[];
  projectIds: string[];
}

export interface FacetSelection {
  path: string;
  values: string[];
}

export interface LogsViewState {
  global: GlobalContext;
  /** IF-007 §2.3：语法未定稿，前端当作不透明串透传，不做解析。 */
  query: string;
  severity: Severity[];
  facets: FacetSelection[];
  /** 有序列集合。顺序是视图语义，集合是查询语义。 */
  columns: string[];
  sort: 'ts desc' | 'ts asc';
  /** 展开中的行，进 URL 以便分享链接直接打开某条详情。 */
  expandedRowRef: string | null;
}

/**
 * DD-008 §3.4 结论 2：默认列必须是一个具体的路径清单，
 * 不能实现成「不传 select，由后端决定」—— 后端在路径集合不明确时会拒绝。
 */
export const DEFAULT_COLUMNS: readonly string[] = [
  'ts',
  'severity',
  'service',
  'cluster_id',
  'body',
  'trace_id',
] as const;

export const DEFAULT_TIME_RANGE: TimeRange = { from: 'now-1h', to: 'now' };

const PARAM = {
  from: 'from',
  to: 'to',
  clusters: 'clusters',
  projects: 'projects',
  query: 'q',
  severity: 'severity',
  facet: 'facet',
  columns: 'cols',
  sort: 'sort',
  row: 'row',
} as const;

function splitList(raw: string | null): string[] {
  if (!raw) return [];
  return raw
    .split(',')
    .map((s) => decodeURIComponent(s.trim()))
    .filter((s) => s.length > 0);
}

function joinList(values: readonly string[]): string {
  return values.map((v) => encodeURIComponent(v)).join(',');
}

function parseFacets(params: URLSearchParams): FacetSelection[] {
  const byPath = new Map<string, string[]>();
  for (const raw of params.getAll(PARAM.facet)) {
    const sep = raw.indexOf('=');
    if (sep <= 0) continue;
    const path = decodeURIComponent(raw.slice(0, sep));
    const value = decodeURIComponent(raw.slice(sep + 1));
    const existing = byPath.get(path);
    if (existing) {
      if (!existing.includes(value)) existing.push(value);
    } else {
      byPath.set(path, [value]);
    }
  }
  return [...byPath.entries()].map(([path, values]) => ({ path, values }));
}

export function parseGlobalContext(params: URLSearchParams): GlobalContext {
  return {
    timeRange: {
      from: params.get(PARAM.from) ?? DEFAULT_TIME_RANGE.from,
      to: params.get(PARAM.to) ?? DEFAULT_TIME_RANGE.to,
    },
    clusterIds: splitList(params.get(PARAM.clusters)),
    projectIds: splitList(params.get(PARAM.projects)),
  };
}

export function parseLogsViewState(params: URLSearchParams): LogsViewState {
  const columnsRaw = params.get(PARAM.columns);
  const severity = splitList(params.get(PARAM.severity)).filter((s): s is Severity =>
    (SEVERITIES as readonly string[]).includes(s),
  );
  const sortRaw = params.get(PARAM.sort);
  return {
    global: parseGlobalContext(params),
    query: params.get(PARAM.query) ?? '',
    severity,
    facets: parseFacets(params),
    columns: columnsRaw === null ? [...DEFAULT_COLUMNS] : dedupe(splitList(columnsRaw)),
    sort: sortRaw === 'ts asc' ? 'ts asc' : 'ts desc',
    expandedRowRef: params.get(PARAM.row),
  };
}

function dedupe(values: string[]): string[] {
  return [...new Set(values)];
}

export function serializeGlobalContext(global: GlobalContext, into = new URLSearchParams()) {
  into.set(PARAM.from, global.timeRange.from);
  into.set(PARAM.to, global.timeRange.to);
  if (global.clusterIds.length) into.set(PARAM.clusters, joinList(global.clusterIds));
  else into.delete(PARAM.clusters);
  if (global.projectIds.length) into.set(PARAM.projects, joinList(global.projectIds));
  else into.delete(PARAM.projects);
  return into;
}

export function serializeLogsViewState(state: LogsViewState): URLSearchParams {
  const params = serializeGlobalContext(state.global);
  if (state.query) params.set(PARAM.query, state.query);
  if (state.severity.length) params.set(PARAM.severity, joinList(state.severity));
  for (const facet of state.facets) {
    for (const value of facet.values) {
      params.append(
        PARAM.facet,
        `${encodeURIComponent(facet.path)}=${encodeURIComponent(value)}`,
      );
    }
  }
  if (!sameColumns(state.columns, DEFAULT_COLUMNS)) {
    params.set(PARAM.columns, joinList(state.columns));
  }
  if (state.sort !== 'ts desc') params.set(PARAM.sort, state.sort);
  if (state.expandedRowRef) params.set(PARAM.row, state.expandedRowRef);
  return params;
}

function sameColumns(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

/**
 * DD-008 §3.9.3 + IF-007 §2.3：检索 / 直方图 / Facet 三类接口共享同一份过滤条件结构。
 * 三者在前端也只能有这一个构造函数，否则必然漂移成三套。
 */
export function buildLogFilter(state: LogsViewState): LogFilter {
  const queryWithFacets = composeQuery(state.query, state.facets);
  return {
    time_range: state.global.timeRange,
    ...(queryWithFacets ? { query: queryWithFacets } : {}),
    ...(state.global.clusterIds.length ? { cluster_ids: state.global.clusterIds } : {}),
    ...(state.global.projectIds.length ? { project_ids: state.global.projectIds } : {}),
    ...(state.severity.length ? { severity: state.severity } : {}),
  };
}

/**
 * facet 选择必须和查询框落到同一份过滤条件里（DD-008 §3.4：三者作用于同一组条件）。
 * `query` 的语法尚未定稿（IF-007 §2.3），这里采用最保守的拼接形态：
 * `path:"value"`，多值用 OR，与查询框原串用 AND 连接。语法定稿后只需改这一个函数。
 */
export function composeQuery(query: string, facets: readonly FacetSelection[]): string {
  const clauses: string[] = [];
  const trimmed = query.trim();
  if (trimmed) clauses.push(facets.length ? `(${trimmed})` : trimmed);
  for (const facet of facets) {
    if (!facet.values.length) continue;
    const alternatives = facet.values.map((v) => `${facet.path}:${quote(v)}`);
    clauses.push(alternatives.length === 1 ? alternatives[0]! : `(${alternatives.join(' OR ')})`);
  }
  return clauses.join(' AND ');
}

function quote(value: string): string {
  return `"${value.replace(/(["\\])/g, '\\$1')}"`;
}

/**
 * DD-008 §3.2 决策 5 / §3.4：ts、service、cluster_id、trace_id 无论用户是否把列显示出来
 * 都必须在投影集合内，否则行内跳转链接无从构造。IF-007 §4.3 把强制返回集合扩到 7 个，
 * 这几个字段由服务端兜底返回，因此 select_paths 只需要携带其余的动态路径。
 */
export function buildSelectPaths(columns: readonly string[]): string[] {
  return [...new Set(columns.filter((c) => !isAlwaysReturnedPath(c)))].sort();
}

/** 列顺序/列宽变化不得触发查询（DD-008 §3.4 结论 1），所以查询键用「排序后的集合」。 */
export function logsQueryKey(state: LogsViewState) {
  return [
    'logs',
    'search',
    buildLogFilter(state),
    buildSelectPaths(state.columns),
    state.sort,
  ] as const;
}

/** 构造一个只带全局上下文的查询串，用于跨页面跳转（DD-008 决策 5）。 */
export function globalContextSearch(global: GlobalContext): string {
  return serializeGlobalContext(global).toString();
}
