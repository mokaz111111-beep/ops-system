import type { FieldDescriptor, LogRow, LogRowFull, Severity } from '@/api/types';

/**
 * Mock 数据集（PLAN D5）。
 *
 * 不预先物化行，而是把「第 i 行」定义成下标的纯函数：ts = i * SPACING + 抖动，
 * 其余属性由 hash(i) 派生。好处是任意时间窗都能即时取数且跨请求完全一致
 * （row_ref、游标翻页、上下文查询指向的都是同一行），同时 7 天窗口也不占内存。
 */

const SPACING_MS = 400; // 2.5 行/秒 ≈ 21.6 万行/天
export const MOCK_SCAN_LIMIT_ROWS = 2_000_000; // 约 9.2 天窗口，超过即触发 SCAN_LIMIT_EXCEEDED
export const MOCK_TIME_RANGE_LIMIT_MS = 30 * 86_400_000;

function hash(i: number, salt: number): number {
  let x = (i ^ (salt * 0x9e3779b9)) >>> 0;
  x = Math.imul(x ^ (x >>> 16), 0x21f0aaad) >>> 0;
  x = Math.imul(x ^ (x >>> 15), 0x735a2d97) >>> 0;
  return (x ^ (x >>> 15)) >>> 0;
}

function pick<T>(items: readonly T[], i: number, salt: number): T {
  return items[hash(i, salt) % items.length]!;
}

export interface ClusterDef {
  id: string;
  project: string;
  /** 已失活集群：last_seen 远在过去，前端据此灰显（IF-007 §4.1）。 */
  stale?: boolean;
}

export const CLUSTERS: readonly ClusterDef[] = [
  { id: 'c-prod-sh-01', project: 'prj-prod' },
  { id: 'c-prod-bj-02', project: 'prj-prod' },
  { id: 'c-prod-sg-01', project: 'prj-prod' },
  { id: 'c-stg-hz-01', project: 'prj-staging' },
  { id: 'c-dev-hz-09', project: 'prj-dev', stale: true },
];

export const PROJECTS: readonly string[] = ['prj-prod', 'prj-staging', 'prj-dev'];

export const SERVICES: readonly string[] = [
  'checkout',
  'payment-svc',
  'order-svc',
  'inventory-svc',
  'auth-svc',
  'cart-svc',
  'search-svc',
  'notification-svc',
  'shipping-svc',
  'recommendation-svc',
];

/** 严重度分布：按真实日志的长尾形状配权，ERROR/FATAL 稀疏但足以撑起直方图堆叠。 */
const SEVERITY_WEIGHTS: readonly (readonly [Severity, number])[] = [
  ['TRACE', 4],
  ['DEBUG', 16],
  ['INFO', 58],
  ['WARN', 14],
  ['ERROR', 7],
  ['FATAL', 1],
];

const SEVERITY_TABLE: Severity[] = (() => {
  const table: Severity[] = [];
  for (const [sev, weight] of SEVERITY_WEIGHTS) {
    for (let n = 0; n < weight; n += 1) table.push(sev);
  }
  return table;
})();

interface FieldDef {
  path: string;
  type: string;
  declared: boolean;
  subcolumnized: boolean;
  indexed: boolean;
  /** 0~100，该路径在一行里出现的概率。长尾字段刻意做得很稀疏。 */
  presence: number;
  values: readonly (string | number)[];
}

/**
 * 字段目录（FE-06）。三个物理标志刻意覆盖全部有意义的组合：
 *   declared=true  → facet 列表置顶
 *   subcolumnized=false → UI 标注「低频字段，聚合较慢」，且 mock 真的会慢
 */
export const FIELD_DEFS: readonly FieldDef[] = [
  {
    path: 'http.status_code',
    type: 'INT',
    declared: true,
    subcolumnized: true,
    indexed: true,
    presence: 72,
    values: [200, 201, 204, 301, 400, 401, 403, 404, 429, 500, 502, 503, 504],
  },
  {
    path: 'http.method',
    type: 'STRING',
    declared: true,
    subcolumnized: true,
    indexed: true,
    presence: 72,
    values: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
  },
  {
    path: 'http.target',
    type: 'STRING',
    declared: true,
    subcolumnized: true,
    indexed: false,
    presence: 70,
    values: ['/pay', '/checkout', '/cart/items', '/orders', '/search', '/api/v2/inventory'],
  },
  {
    path: 'k8s.pod.name',
    type: 'STRING',
    declared: true,
    subcolumnized: true,
    indexed: true,
    presence: 100,
    values: [],
  },
  {
    path: 'k8s.namespace',
    type: 'STRING',
    declared: true,
    subcolumnized: true,
    indexed: true,
    presence: 100,
    values: ['prod', 'prod-edge', 'staging', 'default'],
  },
  {
    path: 'k8s.container.name',
    type: 'STRING',
    declared: true,
    subcolumnized: true,
    indexed: false,
    presence: 100,
    values: ['app', 'sidecar-envoy', 'log-shipper'],
  },
  {
    path: 'host.name',
    type: 'STRING',
    declared: true,
    subcolumnized: true,
    indexed: false,
    presence: 100,
    values: ['node-a1', 'node-a2', 'node-b7', 'node-c3', 'node-c9'],
  },
  {
    path: 'db.system',
    type: 'STRING',
    declared: false,
    subcolumnized: true,
    indexed: false,
    presence: 24,
    values: ['mysql', 'redis', 'postgresql'],
  },
  {
    path: 'db.statement',
    type: 'STRING',
    declared: false,
    subcolumnized: true,
    indexed: false,
    presence: 20,
    values: [
      'SELECT * FROM orders WHERE id = ?',
      'UPDATE inventory SET qty = qty - ? WHERE sku = ?',
      'GET session:?',
    ],
  },
  {
    path: 'rpc.method',
    type: 'STRING',
    declared: false,
    subcolumnized: true,
    indexed: false,
    presence: 30,
    values: ['Pay', 'Refund', 'ReserveStock', 'ReleaseStock', 'SendSms'],
  },
  {
    path: 'user.id',
    type: 'STRING',
    declared: false,
    subcolumnized: true,
    indexed: false,
    presence: 55,
    values: [],
  },
  // 以下四条为长尾字段：被 Top-N 字段配额挤出子列化，聚合要走通用路径
  {
    path: 'biz.coupon.campaign_id',
    type: 'STRING',
    declared: false,
    subcolumnized: false,
    indexed: false,
    presence: 6,
    values: ['cmp-2026-double11', 'cmp-newuser', 'cmp-flashsale-0930'],
  },
  {
    path: 'custom.legacy_header',
    type: 'STRING',
    declared: false,
    subcolumnized: false,
    indexed: false,
    presence: 3,
    values: ['X-Legacy-A', 'X-Legacy-B'],
  },
  {
    path: 'app.feature_flag.v2_checkout',
    type: 'BOOLEAN',
    declared: false,
    subcolumnized: false,
    indexed: false,
    presence: 9,
    values: ['true', 'false'],
  },
  {
    path: 'vendor.partner_ref',
    type: 'STRING',
    declared: false,
    subcolumnized: false,
    indexed: false,
    presence: 2,
    values: ['alipay', 'wechatpay', 'unionpay', 'stripe'],
  },
];

/** 平台声明的语义约定字段也要出现在目录里，否则「加列」入口看不到它们。 */
const BUILTIN_FIELD_DEFS: readonly FieldDescriptor[] = [
  { path: 'ts', type: 'DATETIME', declared: true, subcolumnized: true, indexed: true },
  { path: 'severity', type: 'STRING', declared: true, subcolumnized: true, indexed: true },
  { path: 'service', type: 'STRING', declared: true, subcolumnized: true, indexed: true },
  { path: 'cluster_id', type: 'STRING', declared: true, subcolumnized: true, indexed: true },
  { path: 'trace_id', type: 'STRING', declared: true, subcolumnized: true, indexed: true },
  { path: 'body', type: 'STRING', declared: true, subcolumnized: true, indexed: false },
];

export function fieldCatalog(now: number): FieldDescriptor[] {
  const dynamic = FIELD_DEFS.map((def, idx) => ({
    path: def.path,
    type: def.type,
    declared: def.declared,
    subcolumnized: def.subcolumnized,
    indexed: def.indexed,
    last_seen: new Date(now - idx * 37_000).toISOString(),
  }));
  const builtin = BUILTIN_FIELD_DEFS.map((d) => ({ ...d, last_seen: new Date(now).toISOString() }));
  return [...builtin, ...dynamic];
}

export function findFieldDef(path: string): FieldDescriptor | undefined {
  return fieldCatalog(Date.now()).find((f) => f.path === path);
}

// ---------------------------------------------------------------------------
// 行的纯函数定义
// ---------------------------------------------------------------------------

export function indexRange(fromMs: number, toMs: number): { first: number; last: number } {
  return { first: Math.ceil(fromMs / SPACING_MS), last: Math.floor(toMs / SPACING_MS) };
}

export function tsAt(i: number): number {
  return i * SPACING_MS + (hash(i, 11) % SPACING_MS);
}

export function severityAt(i: number): Severity {
  return SEVERITY_TABLE[hash(i, 3) % SEVERITY_TABLE.length]!;
}

export function serviceAt(i: number): string {
  return pick(SERVICES, i, 5);
}

export function clusterAt(i: number): ClusterDef {
  // 让非生产集群稀疏一些，更接近真实分布
  const roll = hash(i, 7) % 100;
  if (roll < 40) return CLUSTERS[0]!;
  if (roll < 70) return CLUSTERS[1]!;
  if (roll < 85) return CLUSTERS[2]!;
  if (roll < 97) return CLUSTERS[3]!;
  return CLUSTERS[4]!;
}

export function traceIdAt(i: number): string | null {
  if (hash(i, 13) % 100 < 28) return null;
  return (hash(i, 17).toString(16) + hash(i, 19).toString(16) + hash(i, 23).toString(16))
    .padEnd(32, '0')
    .slice(0, 32);
}

const BODY_TEMPLATES: Record<Severity, readonly string[]> = {
  TRACE: ['span started for {svc}', 'entering handler {svc}.dispatch'],
  DEBUG: ['cache lookup miss key=session:{n}', 'resolved upstream {svc} in {n}ms'],
  INFO: [
    'request completed status=200 duration={n}ms',
    '{svc} processed order ord-{n}',
    'healthcheck ok, {n} connections in pool',
  ],
  WARN: [
    'slow downstream call to {svc}, took {n}ms',
    'retrying request, attempt {n}',
    'connection pool nearing capacity ({n}%)',
  ],
  ERROR: [
    'payment gateway timeout after {n}ms',
    'failed to reserve stock for sku-{n}: upstream 503',
    'unhandled exception in {svc}: NullPointerException',
  ],
  FATAL: ['{svc} shutting down: unable to reach database after {n} retries'],
};

export function bodyAt(i: number, severity: Severity, service: string): string {
  const templates = BODY_TEMPLATES[severity];
  const template = templates[hash(i, 29) % templates.length]!;
  return template.replace('{svc}', service).replace('{n}', String(hash(i, 31) % 4000));
}

export function fieldValueAt(i: number, def: FieldDef): string | number | undefined {
  if (hash(i, def.path.length * 101 + 3) % 100 >= def.presence) return undefined;
  if (def.path === 'k8s.pod.name') {
    return `${serviceAt(i)}-${hash(i, 41).toString(16).slice(0, 4)}-${hash(i, 43)
      .toString(36)
      .slice(0, 4)}`;
  }
  if (def.path === 'user.id') return `u-${String(hash(i, 47) % 90000 + 10000)}`;
  return def.values[hash(i, 53) % def.values.length];
}

export function allFieldsAt(i: number): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const def of FIELD_DEFS) {
    const value = fieldValueAt(i, def);
    if (value !== undefined) out[def.path] = value;
  }
  return out;
}

export function rowRefOf(i: number): string {
  return `r-${btoa(JSON.stringify({ i }))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '')}`;
}

export function indexOfRowRef(rowRef: string): number | null {
  if (!rowRef.startsWith('r-')) return null;
  try {
    const padded = rowRef.slice(2).replace(/-/g, '+').replace(/_/g, '/');
    const parsed: unknown = JSON.parse(atob(padded));
    const i = (parsed as { i?: unknown }).i;
    return typeof i === 'number' && Number.isFinite(i) ? i : null;
  } catch {
    return null;
  }
}

/** 列表行：只带 select_paths 投影出的路径 + 契约强制返回的 7 个字段（IF-007 §4.3）。 */
export function projectedRow(i: number, selectPaths: readonly string[]): LogRow {
  const severity = severityAt(i);
  const service = serviceAt(i);
  const all = allFieldsAt(i);
  const fields: Record<string, unknown> = {};
  for (const path of selectPaths) {
    if (path in all) fields[path] = all[path];
  }
  return {
    row_ref: rowRefOf(i),
    ts: new Date(tsAt(i)).toISOString(),
    service,
    cluster_id: clusterAt(i).id,
    trace_id: traceIdAt(i),
    severity,
    body: bodyAt(i, severity, service),
    fields,
  };
}

/** 单行详情：唯一允许整条文档读的接口（IF-007 §4.6）。 */
export function fullRow(i: number): LogRowFull {
  const base = projectedRow(i, []);
  const cluster = clusterAt(i);
  const all = allFieldsAt(i);
  const logAttributes: Record<string, unknown> = {};
  const resourceAttributes: Record<string, unknown> = {
    'cluster.id': cluster.id,
    'project.id': cluster.project,
    'service.name': base.service,
    'service.version': `1.${String(hash(i, 59) % 20)}.${String(hash(i, 61) % 9)}`,
    'telemetry.sdk.language': pick(['go', 'java', 'python', 'node'], i, 67),
    'cloud.region': cluster.id.includes('sh') ? 'cn-shanghai' : 'cn-beijing',
  };
  for (const [path, value] of Object.entries(all)) {
    if (path.startsWith('k8s.') || path.startsWith('host.')) resourceAttributes[path] = value;
    else logAttributes[path] = value;
  }
  logAttributes['log.file.path'] = `/var/log/pods/${base.service}/app/0.log`;
  logAttributes['log.iostream'] = base.severity === 'ERROR' ? 'stderr' : 'stdout';
  return { ...base, log_attributes: logAttributes, resource_attributes: resourceAttributes };
}

export function dimensionLastSeen(now: number, stale: boolean | undefined): string {
  return new Date(stale ? now - 11 * 86_400_000 : now - 30_000).toISOString();
}
