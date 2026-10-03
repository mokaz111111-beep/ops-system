# IF-007 · 查询 API 规格

| 项 | 值 |
|----|----|
| 契约编号 | IF-7 |
| 版本 | **v1**（M2 冻结范围） |
| 状态 | 待双方确认（DD-005 出规格 / DD-008 验收覆盖度） |
| 定义方 | DD-005（查询层） |
| 消费方 | DD-008（前端）、DD-007（AI 层，部分能力经 IF-8 转发）、客户 Open API、Grafana |
| 上游 | [DD-005 §3.7](../dd/DD-005_查询层与信号关联.md)、[DD-008 §3.9](../dd/DD-008_前端与交互.md) |
| 技术栈 | 服务端 Go；前端 React + TypeScript |

> **为什么独立成册**：API 契约的变更节奏与设计文档不同（设计稳定后接口仍会小步演进），且它是前端、AI 层、Open API、Grafana 四方共同编码的对象。嵌在 DD-005 的一个小节里会让版本管理失效。
>
> **本文档是 SD-000 Q5-4 的答案**：§8 的覆盖矩阵逐条核对了 DD-008 的 FE-01 ~ FE-31。

---

## 1. 范围

**v1 冻结范围 = M2 所需的全部接口**（DD-008 标为 M2 阻塞的 10 条）。M3 及以后的接口在 §7 给出签名级定义，不在 v1 冻结，但路径与命名已预留以免后续破坏性变更。

**产品边界（v2.1）**：路径按产品切开——`/logs*` 属 Logs，`/traces*` 属 Traces，`/metrics*` 属 Metrics。`ops-query` 先查开通再路由。未开通返回 `FIX_REQUEST`（后续可单列 `PRODUCT_NOT_ENABLED`）；**不得**因为 Logs 与 Traces 同 Doris 就让 `/logs` 扫到 `spans`。跨产品跳转走独立关联接口，不靠调用方拼 SQL。本规格由 **`ops-query`** 实现，不是第四个查询进程。

**明确不在 IF-7 范围内**（但前端 M2 需要，归属见 §6）：接入侧实时验收、Agent 健康状态、管理面各页。

---

## 2. 通用约定

### 2.1 基础

| 项 | 规则 |
|----|------|
| Base path | `/api/v1/` |
| 传输 | HTTPS，JSON（PromQL 透传接口保持 Prometheus 原生格式） |
| 版本化 | 路径前缀带版本号。破坏性变更须新开 `/api/v2/` 并保留 v1 **至少一个发布周期**（具体窗口见 DD-005 Q5-3） |
| 认证 | SSO 会话或 AKSK，经统一鉴权中间件解析租户 |
| **租户** | **请求中任何 `tenant_id` / `project_id` 字段一律被服务端忽略并覆盖。** 前端永远在单租户上下文内，不传、也无法伪造 |

### 2.2 时间窗（所有查询接口必填）

```json
{ "from": "now-1h", "to": "now" }
```

```json
{ "from": "2026-10-02T10:00:00Z", "to": "2026-10-02T11:00:00Z" }
```

- 两种形式都必须支持，且**在保存视图与分享链接中原样往返**——相对形式不得在存储时被解析成绝对值，否则巡检类视图每次打开都停在创建那天；
- **无时间窗的请求直接拒绝**（`TIME_RANGE_REQUIRED`），不提供默认值。默认值会让一次误操作变成全量扫描。

### 2.3 过滤条件（检索 / 直方图 / Facet / 聚合四类接口共享同一结构）

```json
{
  "time_range":  { "from": "now-1h", "to": "now" },
  "query":       "service:checkout AND http.status_code:500",
  "cluster_ids": ["c-prod-sh-01"],
  "project_ids": ["prj-prod"],
  "severity":    ["ERROR", "WARN"]
}
```

两条设计说明：

- **四类接口必须共享这一份结构**，不得各自定义。它们作用于同一组条件且需要同步刷新；各自定义会在前端产生三套转换逻辑，并且一定会随时间漂移；
- **`query` 字段的语法形态尚未定稿**（DD-005 Q5-2：类 KQL 自研 / SQL 子集 / 双模式）。本规格刻意把它定为一个字符串字段，**使语法决策不阻塞接口定稿与前端开工**——前端先按"不透明查询串 + 结构化维度筛选"实现，语法定稿后只影响查询框的解析与补全，不影响任何接口签名。

### 2.4 分页

- **统一不透明游标，不提供 offset 分页。** 深 offset 在列存上代价随偏移线性增长，且持续写入会导致翻页错行；
- 游标形如 `"cursor": "eyJ0cyI6..."`，客户端不得解析或构造；
- 游标有**有效期**（默认 5 分钟）。过期返回 `CURSOR_EXPIRED` + `suggested_action: RESTART_QUERY`；
- 首页请求不传 `cursor`；返回 `next_cursor` 为 `null` 表示已到末页。

> 游标在 RANDOM 分桶 + 多 tablet 并行下的稳定性是 DD-005 Q5-6 的 M1 验证项。**若实测不稳定（同一游标重复请求返回不一致结果），需改为带排序键快照的分页方案——那是一次破坏性变更，所以这项必须在 M1 内出结论，不能拖到 M2。**

### 2.5 `query_id` 与查询取消

每个查询类接口的返回**必须**包含服务端生成的 `query_id`，与 DD-005 的查询审计记录同键。

> **这条从 M2 起就必须落地，即使 AI 结论回放（FE-28）要到 M4。** 它同时是审计、计量与回放的关联键，晚加等于丢掉之前所有查询的可回溯性——典型的"晚加一个字段、丢掉两年数据"。

**取消**：客户端可在请求头带 `X-Client-Query-Id: <uuid>`，随后用它取消：

```
DELETE /api/v1/queries/{client_query_id}
```

- 取消后服务端释放该租户的并发配额槽位；
- 未提供该请求头的查询不可取消；
- 用户改条件重查的频率很高，不支持取消会让并发配额被已被放弃的查询占满——所以前端应**默认总是带这个头**。

### 2.6 错误结构

```json
{
  "error": {
    "code": "SCAN_LIMIT_EXCEEDED",
    "message": "查询扫描量超过单次上限",
    "retryable": false,
    "suggested_action": "USE_ASYNC_EXPORT",
    "details": { "scanned_rows": 523000000, "limit": 200000000 },
    "retry_after_seconds": null,
    "query_id": "q-01HQ..."
  }
}
```

`suggested_action` 是**枚举而非文案**，这是 DD-008 的硬要求：UI 要把拒绝渲染成可点击的按钮（"缩小时间窗"／"改走异步导出"／"稍后重试"），而不是一段红色文字。只有文本的话，查询治理的产品化引导就落不了地。

枚举值：`NARROW_TIME_RANGE` · `REDUCE_SELECT_PATHS` · `USE_ASYNC_EXPORT` · `RETRY_AFTER` · `RESTART_QUERY` · `FIX_REQUEST` · `CONTACT_SUPPORT`

---

## 3. 错误码表

| 错误码 | HTTP | 可重试 | 建议动作 | 触发条件 |
|--------|------|--------|---------|---------|
| `INVALID_PARAM` | 400 | 否 | `FIX_REQUEST` | 参数格式错误 |
| `TIME_RANGE_REQUIRED` | 400 | 否 | `FIX_REQUEST` | 未传时间窗 |
| `TIME_RANGE_TOO_LARGE` | 400 | 否 | `NARROW_TIME_RANGE` | 时间窗超过该接口上限 |
| `SELECT_PATHS_REQUIRED` | 400 | 否 | `FIX_REQUEST` | 日志/Trace 列表未传 `select_paths`（见 §4.3 说明） |
| `TOO_MANY_SELECT_PATHS` | 400 | 否 | `REDUCE_SELECT_PATHS` | 投影路径数超上限 |
| `FIELD_PATH_NOT_FOUND` | 400 | 否 | `FIX_REQUEST` | 请求的字段路径在字段目录中不存在 |
| `QUERY_SYNTAX_ERROR` | 400 | 否 | `FIX_REQUEST` | `query` 解析失败，`details` 带错误位置 |
| `UNSUPPORTED_SQL` | 400 | 否 | `FIX_REQUEST` | SQL 模式下超出允许子集（含任何写操作） |
| `CURSOR_EXPIRED` | 400 | 否 | `RESTART_QUERY` | 游标过期 |
| `SCAN_LIMIT_EXCEEDED` | 422 | 否 | `USE_ASYNC_EXPORT` | 预估或实际扫描量超单次上限 |
| `QUOTA_EXCEEDED` | 429 | **是** | `RETRY_AFTER` | 租户查询配额耗尽，带 `retry_after_seconds` |
| `CONCURRENCY_QUEUE_TIMEOUT` | 429 | **是** | `RETRY_AFTER` | 排队超时。**不静默排队到请求超时** |
| `QUERY_TIMEOUT` | 504 | **是** | `NARROW_TIME_RANGE` | 查询执行超时 |
| `QUERY_CANCELLED` | 499 | — | — | 客户端主动取消 |
| `INTERNAL` | 500 | **是** | `RETRY_AFTER` | 内部错误 |

> **配额类错误必须如实返回 429 并带退避提示，不得静默排队。** 这与 IF-1 的错误传播原则同源：任何一跳把可重试错误伪装成成功或超时，上层的治理与兜底逻辑就全部失效。

---

## 4. M2 接口（v1 冻结）

### 4.1 维度枚举 — `FE-02`

```
POST /api/v1/dimensions
```

```json
{ "time_range": { "from": "now-24h", "to": "now" },
  "dimensions": ["project", "cluster", "service"] }
```

```json
{ "project": [{ "value": "prj-prod", "last_seen": "2026-10-02T11:58:00Z" }],
  "cluster": [{ "value": "c-prod-sh-01", "last_seen": "2026-10-02T11:59:00Z" }],
  "service": [{ "value": "checkout", "last_seen": "2026-10-02T11:59:30Z" }],
  "query_id": "q-01HQ..." }
```

`last_seen` 用于前端灰显已失活集群。本接口同时是 AI 输入框 `@` 补全的数据源。

### 4.2 字段目录 — `FE-06`

```
POST /api/v1/fields
```

```json
{ "signal": "logs", "time_range": { "from": "now-24h", "to": "now" }, "prefix": "http." }
```

```json
{ "fields": [
    { "path": "http.status_code", "type": "INT",
      "declared": true, "subcolumnized": true, "indexed": true,
      "last_seen": "2026-10-02T11:59:00Z" },
    { "path": "http.custom_header", "type": "STRING",
      "declared": false, "subcolumnized": false, "indexed": false,
      "last_seen": "2026-10-02T09:12:00Z" } ],
  "catalog_age_seconds": 142,
  "query_id": "q-01HQ..." }
```

**这是列配置与 Facet 列表的唯一数据源**，v1.3 全文没有任何对应设计，是 v2.0 新增能力。没有它，"加列"只能靠用户手输路径。

三个物理标志的含义与来源：

| 标志 | 含义 | 来源 |
|------|------|------|
| `declared` | 该路径在 Schema Template 中声明了强类型 | DD-002 的建表定义（静态） |
| `subcolumnized` | 该路径当前确实被列式提取为子列 | 引擎运行时状态（IF-6） |
| `indexed` | 该路径有路径级倒排索引 | DD-002 的索引定义 |

> **`subcolumnized` 是运行时事实而非配置，会漂移。** 热点路径被 Top-N 取舍挤出子列化时它会变为 false，而此时该字段的聚合会显著变慢——UI 据此标注"低频字段，聚合较慢"，`declared` 为 true 的路径则在列表中置顶（它们一定被子列化）。
>
> 字段目录的计算代价不低（需要探查运行时子列状态），因此**服务端缓存，并用 `catalog_age_seconds` 如实告知数据新鲜度**。前端不得假设它是实时的。缓存刷新周期 ≤ 5 分钟。

### 4.3 日志检索 — `FE-03`

```
POST /api/v1/logs/search
```

```json
{ "time_range": { "from": "now-1h", "to": "now" },
  "query": "http.status_code:500",
  "cluster_ids": ["c-prod-sh-01"],
  "severity": ["ERROR"],
  "select_paths": ["http.status_code", "http.target", "k8s.pod.name"],
  "sort": "ts desc",
  "limit": 100,
  "cursor": null }
```

```json
{ "rows": [
    { "row_ref": "r-eyJwIjoi...",
      "ts": "2026-10-02T11:58:03.412Z",
      "service": "checkout",
      "cluster_id": "c-prod-sh-01",
      "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
      "severity": "ERROR",
      "body": "payment gateway timeout",
      "fields": { "http.status_code": 500, "http.target": "/pay",
                  "k8s.pod.name": "checkout-7d9f-x2k1" } } ],
  "next_cursor": "eyJ0cyI6...",
  "scan_limit_reached": false,
  "query_id": "q-01HQ..." }
```

**`select_paths` 是必填参数，不是可选优化。** 它直接对应 DD-005 §3.2 的投影规则：前端不声明要哪些字段，网关就无法构造路径投影，只能整条读（在列表场景代价不可接受）或拒绝。

> 这把存储层的物理约束一路顶到了 API 契约上。**看起来别扭，但它是真实的——藏起来只会让列表页在上线后慢得没法救。**

为降低前端负担，以下字段**无论是否出现在 `select_paths` 中都强制返回**：`row_ref` · `ts` · `service` · `cluster_id` · `trace_id` · `severity` · `body`。

`scan_limit_reached` 为 true 表示结果因触达扫描上限而被截断，UI 必须提示用户，**不得静默展示不完整结果**。

### 4.4 日志直方图 — `FE-04`

```
POST /api/v1/logs/histogram
```

入参 = §2.3 过滤条件 + `interval`（可为 `"auto"`）+ `group_by`（v1 仅支持 `"severity"`）。

```json
{ "buckets": [ { "ts_start": "2026-10-02T11:00:00Z", "severity": "ERROR", "count": 142 },
               { "ts_start": "2026-10-02T11:00:00Z", "severity": "WARN",  "count": 891 } ],
  "interval": "1m",
  "downsampled": false,
  "query_id": "q-01HQ..." }
```

返回**实际采用的 `interval`**（服务端可因时间窗过大而放粗）与 `downsampled` 标记。前端按返回值渲染坐标轴，不按请求值。

> `severity` 的物理形态影响本接口性能：按 severity 堆叠着色是高频操作，若 severity 落在半结构化列中且未被子列化，每次渲染都要付出长尾字段的聚合代价。**本规格要求 DD-002 将 `severity` 实现为独立物化列**（DD-008 OQ 已提出，此处转为契约要求）。

### 4.5 Facet 取值 — `FE-05`

```
POST /api/v1/logs/facet
```

入参 = §2.3 过滤条件 + `field_path` + `top_n`（默认 10）。

```json
{ "values": [ { "value": "500", "count": 1420 }, { "value": "503", "count": 88 } ],
  "distinct_estimate": 7,
  "declared": true, "subcolumnized": true, "indexed": true,
  "truncated": false,
  "query_id": "q-01HQ..." }
```

三个物理标志与 §4.2 同义同源，在此重复返回是为了让 Facet 面板不必先查字段目录。

> **Facet 的聚合分母必须只含本租户数据。** 这是 DD-006 §3.2.4 串扰测试套件里最容易漏的一类——数据本身取不到，但 top values 的计数被其他租户污染了。本接口必须进串扰测试断言。

### 4.6 日志单条详情 — `FE-07`

```
GET /api/v1/logs/row/{row_ref}
```

返回完整 log attributes + resource attributes + body 原文。**这是唯一允许整条文档读的日志接口**，因为它只返回 1 行。

### 4.7 日志上下文 — `FE-08`

```
POST /api/v1/logs/context
```

```json
{ "anchor": "r-eyJwIjoi...", "before": 50, "after": 50,
  "scope": "pod" }
```

`scope` 枚举：`pod` · `container` · `file` · `service`。

> **行序语义有一个未解决的前提**：同一毫秒内多行如何稳定排序、跨 tablet 时能否保证顺序一致，目前无法承诺（DD-008 OQ）。若 M1 验证表明不可保证，本接口的语义将退化为"同 scope 内时间邻近的 N 条"而非严格的前后 N 条，**需产品确认是否接受**。接口签名不受此影响。

### 4.8 日志聚合（图表分析模式）— `FE-12`

```
POST /api/v1/logs/aggregate
```

入参 = §2.3 过滤条件 + `group_by_paths[]` + `agg`（`count` / `sum` / `avg` / `p50` / `p95` / `p99`）+ `agg_path`（非 count 时必填）+ `interval`（省略则返回 topN 而非时序）。

**仅允许路径投影**，与 §4.3 同一约束。

### 4.9 PromQL 透传 — `FE-21`

```
GET|POST /api/v1/metrics/query
GET|POST /api/v1/metrics/query_range
```

**保持 Prometheus HTTP API 原生语义与响应格式**（`query` / `time` / `start` / `end` / `step`），以便 Grafana 直接把它配成 Prometheus 数据源。租户路由由网关注入 vmauth accountID，前端不传任何租户参数。

### 4.10 指标元数据 — `FE-22`

```
GET /api/v1/metrics/label/{name}/values
GET /api/v1/metrics/labels
GET /api/v1/metrics/series
GET /api/v1/metrics/metadata
```

四类均需支持 `match[]` 与时间窗，返回 Prometheus 兼容格式。**没有它，Metrics Explorer 只能让用户手写 PromQL。**

### 4.11 异步导出 — `FE-13`（部分，M2 只需任务态）

```
POST   /api/v1/exports          → { "task_id": "..." }
GET    /api/v1/exports/{id}     → { "status": "running|succeeded|failed",
                                    "progress": 0.42, "download_url": null }
DELETE /api/v1/exports/{id}
```

这是 `SCAN_LIMIT_EXCEEDED` 的降级落点——错误里的 `USE_ASYNC_EXPORT` 指向这里。M2 需要它是因为扫描超限在 dogfood 阶段就会发生；SQL 编辑器模式（FE-13 全量）留到 M3。

---

## 5. 仍待定义的基础设施，及其对契约的约束

### 5.1 `row_ref` 单行稳定定位键（DD-005 Q5-7）

日志行无主键、`ts` 不唯一，但 FE-07 与 FE-08 都需要唯一定位某一行。本规格把它定为**不透明字符串 `row_ref`**，使接口签名不被实现选择阻塞。

但契约对它提出一条**有效期要求**，而这条要求能筛掉候选方案：

> **`row_ref` 必须在生成后至少 24 小时内保持有效，且跨 compaction 有效。**

理由是分享链接与告警里的日志引用会被人在第二天打开，而 compaction 在高导入速率下随时发生。由此：

| 候选方案 | 是否满足 |
|---------|---------|
| DD-002 在写入时生成行内唯一标识 | ✅ |
| 查询层用 `(分区, tablet, rowset, 行号)` 合成游标 | ❌ **compaction 后即失效** |

**所以 Q5-7 实际上已被这条契约要求收敛到写入时生成。** 这是一次破坏性表结构变更（IF-6），需 DD-002 会签并尽早排进 M1，否则 M2 的 FE-07 / FE-08 无法交付。

### 5.2 Live Tail 流式能力（DD-005 Q5-8）

信息架构中有 Live Tail 入口（FE-09），但查询层从未定义任何流式或长轮询能力。三条路径：

| 路径 | 问题 |
|------|------|
| 独立流式通道（SSE / WebSocket） | 需新增网关能力，但隔离与审计可复用 |
| Kafka 旁路 | **绕过查询网关，与租户隔离的收口原则冲突**——租户注入、配额、审计全部失效 |
| 高频轮询降级 | 体验与成本都打折，但零新增基础设施 |

**v1 不包含 Live Tail**，FE-09 排期待定。Kafka 旁路方案在隔离方案未解决前不予采纳。

---

## 6. 前端 M2 需要、但不属于 IF-7 的能力

DD-008 把这几条列为 M2 阻塞项，但它们的归属不在查询层。**单独列出以免在文档之间漏掉**——这正是 DD-008 已经点出的风险。

| 需求 | 真实归属 | 说明 |
|------|---------|------|
| **FE-30 接入向导实时验收** | **IF-11（DD-001）** | 需要近 10 秒的信号到达计数。落库延迟 p99 为 10 秒，**走检索路径做实时反馈体验不可接受**，必须由摄入侧提供计量旁路。IF-11 目前尚不存在（SD-000 §7.4） |
| **FE-29 集群与 Agent 状态** | **IF-11（DD-001）** | 心跳、Agent 版本、上报速率、背压与丢弃计数。**丢弃计数必须可见** |
| FE-31 管理面各页 | 控制面 API（DD-006 / IF-9） | 项目与 Token、配额与用量、留存策略、脱敏规则、审计日志、AI 模型配置 |
| FE-10 保存视图 | **未定**（IF-7 vs 控制面，DD-005 Q5-10） | 两处都说得通，但必须定一处，否则前端要对接两套鉴权 |
| FE-23 Grafana 免密嵌入 | 未定 | 方案未定 |

> **FE-29 / FE-30 是 M2 阻塞项，而 IF-11 连契约都还没有。** 这两条的排期风险高于 IF-7 本身——IF-7 有草案可补全，IF-11 是从零开始。建议在 M1 内与 DD-001 owner 同步启动。

---

## 7. M3 及以后（签名预留，v1 不冻结）

路径与命名在此预留，避免后续破坏性变更：

| 接口 | 路径 | 对应 |
|------|------|------|
| 服务 RED 概览 | `POST /api/v1/services/overview` | FE-01 |
| Trace 检索 | `POST /api/v1/traces/search` | FE-14 |
| Trace 详情 | `GET /api/v1/traces/{trace_id}` | FE-15 |
| span → 日志 | `POST /api/v1/traces/{trace_id}/logs` | FE-16 |
| Trace 候选（兜底动线） | `POST /api/v1/traces/candidates` | FE-17 |
| 指标 → trace（exemplar） | `POST /api/v1/metrics/exemplars` | FE-24 |
| 错误分析 | `POST /api/v1/errors/analyze` | FE-18 |
| 服务拓扑 | `POST /api/v1/topology` | FE-19 |
| 服务目录 | `POST /api/v1/services` | FE-20 |
| 告警规则 / 事件 | `/api/v1/alerts/**` | FE-25（归属待定，Q5-9） |
| SLO 与 burn rate | `/api/v1/slo/**` | FE-26 |
| SQL 编辑器模式 | `POST /api/v1/logs/sql` | FE-13 |

**FE-24 与 FE-17 合并为服务端决策**：exemplar 命中则走 FE-24 语义，未命中则服务端自动降级到候选列表，并在返回中标明走的是哪条动线。理由是前端不应该知道 exemplar 是否可用（SD-000 Q-5 的待验证项），把选择权留在服务端，验证结论出来后前端无需改动。

---

## 8. 覆盖矩阵（SD-000 / DD-005 Q5-4 的答案）

| FE | 需求 | 覆盖 |
|----|------|------|
| FE-01 | 服务健康矩阵 | §7（M3） |
| **FE-02** | 维度枚举 | **§4.1 ✅** |
| **FE-03** | 日志列表 | **§4.3 ✅** |
| **FE-04** | 日志直方图 | **§4.4 ✅**（附带对 DD-002 的 severity 物化列要求） |
| **FE-05** | Facet top values | **§4.5 ✅** |
| **FE-06** | 字段目录 | **§4.2 ✅**（全新能力） |
| **FE-07** | 单条展开 | **§4.6 ✅**（依赖 §5.1 `row_ref`） |
| FE-08 | 上下文 ±50 条 | §4.7 ✅（行序语义待产品确认） |
| FE-09 | Live Tail | **§5.2 未覆盖**，排期待定 |
| FE-10 | 保存视图 | §6 归属未定 |
| FE-11 | 创建告警预填 | §7（M3） |
| FE-12 | 图表分析模式 | §4.8 ✅ |
| FE-13 | SQL 编辑器 / 异步导出 | §4.11 ✅（仅导出）/ §7（SQL 模式 M3） |
| FE-14 ~ FE-20 | APM 与拓扑各页 | §7（M3） |
| **FE-21** | PromQL 透传 | **§4.9 ✅** |
| **FE-22** | 指标元数据 | **§4.10 ✅** |
| FE-23 | Grafana 免密 | §6 未定 |
| FE-24 | exemplar 跳转 | §7（M3，与 FE-17 合并） |
| FE-25 | 告警规则/事件 | §7（归属待定） |
| FE-26 | SLO 视图 | §7（M4） |
| FE-27 | AI 对话与证据卡片 | 经 IF-8，不在 IF-7 |
| FE-28 | 结论回放 | 依赖 §2.5 的 `query_id`，**M2 已埋** ✅ |
| **FE-29** | Agent 状态 | **§6 → IF-11（DD-001）** |
| **FE-30** | 接入实时验收 | **§6 → IF-11（DD-001）** |
| FE-31 | 管理面 | §6 → 控制面 API |

**结论：M2 的 10 条阻塞项中，8 条由本规格 v1 覆盖，2 条（FE-29 / FE-30）归属 IF-11 需 DD-001 立即启动。** FE-09 Live Tail 因基础设施未定而不在 v1。

---

## 9. 待办

- [ ] DD-008 owner 验收 §8 覆盖矩阵，确认无遗漏（这是 Q5-4 的关闭条件）
- [ ] DD-002 owner 会签两项：§5.1 的 `row_ref` 写入时生成、§4.4 的 `severity` 独立物化列——**两者都是表结构变更，需进 M1**
- [ ] DD-001 owner 启动 IF-11 契约（FE-29 / FE-30）
- [ ] 定 FE-10 保存视图的归属（Q5-10）
- [ ] M1 内出结论：游标在多 tablet 并行下的稳定性（Q5-6），不稳定则 §2.4 需破坏性修改
- [ ] 生成 OpenAPI 描述文件，供前端 TS 类型与服务端 Go 结构体双向生成
