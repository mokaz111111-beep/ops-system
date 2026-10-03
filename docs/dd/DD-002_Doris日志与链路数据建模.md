# DD-002 · Doris 日志与链路数据建模详细设计

| 项 | 值 |
|----|----|
| 文档编号 | DD-002 |
| 版本 | v2.0（正文，内容等价于单体文档 v1.3 的存储建模部分） |
| 状态 | 待评审 |
| Owner | TBD（后端 · 存储） |
| 必需评审人 | 架构 owner、DD-001 owner（因 IF-3 双签）、DD-005 owner（因 IF-6 是其查询基础）、SRE |
| 上游文档 | [SD-000 总体系统设计](../SD-000_总体系统设计.md) |
| 版本基线 | **Apache Doris 4.0.8，存算分离模式。** 标注 `[4.0 特性]` 的能力需在 M1 PoC 对 4.0.8 实测确认后定稿 |

> **一句话定位**：定义 Logs 与 Traces 的物理存储形态——表结构、半结构化建模方式、索引与分桶，以及支撑它们的引擎参数。

> **本文档是全系统技术密度最高的一份**，直接决定查询能力边界与存储成本。§3.3 是它的核心。

---

## 0. 范围与非范围

**范围**：Doris 存算分离集群形态、`logs` / `spans` / `metric_exemplars` 表结构、VARIANT 半结构化建模基线、倒排索引策略、分区与分桶、BE 侧调优参数、Schema 演进策略。

**非范围**：

- 冷热分层、归档与派生数据 → DD-004
- 指标存储 → DD-003
- 查询语句的生成与改写 → DD-005（本文档只定义**能力边界**，不定义查询逻辑）
- 集群容量与成本推导 → X-003
- 故障矩阵与备份策略 → X-001

---

## 1. 从 SD-000 继承的约束（不得自行放宽）

| 来源 | 约束 |
|------|------|
| SLO-1 | 摄入延迟预算中本模块分得 **2.0s**（Stream Load 提交至可见） |
| SLO-2 | 日志检索预算中本模块分得 **2.4s**（前提：BE 缓存命中） |
| SLO-3 | Trace 点查预算中本模块分得 **800ms** |
| 可用性 | 存储层月度错误预算 15 分钟（与 DD-003 共担） |
| §4 推论 1 | **BE 本地缓存命中率是一级 SLI**，缓存未命中回源 S3 会使延迟上升一个量级 |
| 分层原则 5 | **分区删除是全系统唯一不可逆操作**，引擎自带的 TTL 自动回收必须关闭，删除权交给 DD-004 的归档调度器 |
| G8 | 扩容到 10 TB/天不得改变表结构 |

---

## 2. 本文档拥有的接口契约

| ID | 内容 | 备注 |
|----|------|------|
| **IF-6** | **表 schema 即存储层对外的公开接口**：列名、类型、分区与排序键、可用索引清单 | DD-004 与 DD-005 依赖此契约。**加列可向后兼容；改类型、删列、删索引是破坏性变更，须走架构评审**（流程见 §3.7） |
| IF-3 | 与 DD-001 双签：Stream Load 提交方式、label 幂等、批次大小、背压语义 | 批次大小同时受摄入延迟与导入稳定性约束，任一方改坏都会伤到对方 |
| IF-5（消费方） | 接收 metrics-ingest 写入的 exemplar | 表结构由本文档定义（§3.5），写入规则由 DD-003 定义 |
| IF-10（消费方） | 接受归档调度器的写入、校验与显式删除调用 | 本模块不得自行删除分区 |

---

## 3. 详细设计

### 3.1 Doris 集群形态（存算分离）

**采用 Doris 4.0.8 存算分离（Compute-Storage Decoupled）模式**，不走"存算一体 + cooldown 搬迁"。一期即采用而非到 10 TB 再切换的理由见 SD-000 §4 推论 2：终点规模在 10 TB/天，若先走存算一体，迁移窗口正好落在业务量最大的时刻。

| 组件 | 规格 | 职责 |
|------|------|------|
| FE | 3 台小规格 | 元数据、查询规划、Stream Load 转发 |
| BE | 4 台 16C64G + 3 × 500GB NVMe | 计算 + 本地缓存。**多盘是为满足官方「桶数 ≈ 磁盘数 × 3」规则**（§3.2） |
| Meta Service | 3 副本 × 4C8G | 存算分离必备，无状态 |
| Recycler | 2 副本 × 4C8G（与 MS 混部） | 回收 S3 上的过期 rowset |
| FoundationDB | 3 节点 × 4C16G + 200GB SSD（每 AZ 一台） | Meta Service 的元数据后端，`double` 冗余。元数据量在 GB 级，瓶颈是事务 QPS 不是容量 |

> **FoundationDB 是全架构最高等级单点**：元数据丢失 = 全集群数据不可读。备份策略、恢复演练与运维就绪度评估见 X-001。

**计算组（compute group）划分**：存算分离提供的计算组是比 Workload Group 更彻底的隔离手段——不同计算组是不同的 BE 进程组，共享同一份 S3 数据但 CPU / 内存 / 缓存完全物理隔离。

| 计算组 | 用途 | 规格 |
|--------|------|------|
| `hot` | 用户交互查询 + 摄入 | 4 × 16C64G + 3 × 500GB NVMe |
| `alert` | 告警调度 SQL + L1 物化刷新 | 2 × 8C32G（可与 hot 混部起步，量大后拆出） |
| `cold` | Iceberg 归档查询（DD-005 §3.5） | 按需弹性，0~2 节点 |
| `dedicated-{tenant}` | 大租户专属（DD-006 §3.5） | 按租户量级配置 |

组内再用 Workload Group 做租户级 CPU / 内存软限制。**这是"一期即上存算分离"的一项直接红利**：大租户隔离从"共享 BE + 软限制"升级为"独立 BE 进程组"，爆炸半径真正收敛。

---

### 3.2 Logs 表设计

```sql
CREATE TABLE logs (
    ts              DATETIME(3)      NOT NULL COMMENT '事件时间',
    tenant_id       VARCHAR(64)      NOT NULL COMMENT '租户',
    cluster_id      VARCHAR(64)      NOT NULL COMMENT 'K8s 集群（DD-001 §3.6）',
    service         VARCHAR(128)     NULL,
    severity        VARCHAR(16)      NULL,
    trace_id        VARCHAR(64)      NULL COMMENT '关联键',
    span_id         VARCHAR(16)      NULL,
    body            STRING           NULL COMMENT '原始日志体',

    -- Schema Template 锁定 OTel 语义约定路径。
    --   官方规定 typed path 一定参与子列列式提取，且默认不计入
    --   variant_max_subcolumns_count 预算（variant_enable_typed_paths_to_sparse
    --   默认 false）——等于把平台关心的字段从租户间的子列额度竞争中摘出来；
    --   同时锁死类型，防止跨租户类型冲突导致索引静默失效（§3.3）。
    --   路径级倒排索引（field_pattern）也依赖此处的类型声明。
    log_attributes  VARIANT<
        'http.status_code'  : INT,
        'http.method'       : STRING,
        'http.route'        : STRING,
        'url.path'          : STRING,
        'db.system'         : STRING,
        'db.statement'      : STRING,
        'rpc.service'       : STRING,
        'rpc.method'        : STRING,
        'error.type'        : STRING,
        'exception.type'    : STRING,
        'exception.message' : STRING,
        properties(
            'variant_max_subcolumns_count'         = '2048',
            'variant_enable_doc_mode'              = 'true',
            'variant_doc_hash_shard_count'         = '384',    -- ≈ JSON key 总数/128
            'variant_doc_materialization_min_rows' = '10000'
        )
    >                                NULL COMMENT 'OTel log attributes',

    resource        VARIANT<
        'k8s.namespace.name'  : STRING,
        'k8s.deployment.name' : STRING,
        'k8s.node.name'       : STRING,
        'host.name'           : STRING,
        properties(
            'variant_max_subcolumns_count' = '2048',  -- 同表内必须与上列一致
            'variant_enable_doc_mode'      = 'true',
            'variant_doc_hash_shard_count' = '64'
        )
    >                                NULL COMMENT 'k8s/host 等资源元数据',

    INDEX idx_body       (body)     USING INVERTED PROPERTIES("parser"="unicode", "support_phrase"="true"),
    INDEX idx_trace_id   (trace_id) USING INVERTED,
    INDEX idx_service    (service)  USING INVERTED,
    -- 路径级索引取代「对整个 VARIANT 建索引」。
    --   后者会为所有子列建索引，是索引膨胀的直接来源；官方 FAQ 亦提示
    --   「给 VARIANT 整体建的索引」与子列索引不是一回事（§3.3 第 5 点）。
    INDEX idx_attr_status (log_attributes) USING INVERTED PROPERTIES("field_pattern"="http.status_code"),
    INDEX idx_attr_route  (log_attributes) USING INVERTED PROPERTIES("field_pattern"="http.route"),
    INDEX idx_attr_dbsys  (log_attributes) USING INVERTED PROPERTIES("field_pattern"="db.system"),
    INDEX idx_attr_errtyp (log_attributes) USING INVERTED PROPERTIES("field_pattern"="error.type"),
    -- 同一路径可并存「分词」与「不分词」两套索引，MATCH 与 = 各走各的
    INDEX idx_attr_exc_kw (log_attributes) USING INVERTED PROPERTIES("parser"="unicode", "field_pattern"="exception.message"),
    INDEX idx_res_ns      (resource)       USING INVERTED PROPERTIES("field_pattern"="k8s.namespace.name"),
    INDEX idx_res_host    (resource)       USING INVERTED PROPERTIES("field_pattern"="host.name")
)
ENGINE=OLAP
DUPLICATE KEY(tenant_id, ts)
PARTITION BY RANGE(ts) ()
DISTRIBUTED BY RANDOM BUCKETS 36          -- 230GB/天 ÷ 36 ≈ 6.4GB/tablet，且 = 磁盘数12 × 3
PROPERTIES (
    "compression" = "zstd",
    "compaction_policy" = "time_series",           -- 降低高频导入的写放大
    "storage_format" = "V3",                       -- 宽列必备，见 §3.3 第 4 点
    "inverted_index_storage_format" = "V2",        -- 与上一项是不同属性，两者都要设
    "dynamic_partition.enable" = "true",
    -- 注：不设 create_history_partition。它会按 start~end 区间一次性补建历史分区，
    --     与下面的 start=-3650 组合会尝试创建 3650+ 个分区并触发
    --     max_dynamic_partition_num 限制而建表失败。
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3650",           -- ★ 关闭自动回收，见下
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "36"
);
-- 注 1：存算分离模式下数据主存储在 S3，replication_num 不再表达数据副本语义，
--       早期版本中的 "replication_num"="1" 已移除以免误导。
-- 注 2：官方日志实践用 DUPLICATE KEY(ts)，称对「查询最新 N 条」有数倍加速。
--       本设计前置 tenant_id 是为租户裁剪（见下方设计要点），单租户内部 ts
--       有序性保留，该加速大部分仍在；代价在 M1 量化（§4 P2）。
```

#### 3.2.1 `dynamic_partition.start = -3650`：关闭自动回收

这是一处阻塞级修正，必须理解它的动机才不会在后续调优中改回去。

早期版本写的是 `-7`，Doris 会在分区超过 7 天时**自动 DROP，不等待任何外部条件**。而 DD-004 的归档门禁要求「归档写入完成、行数与校验和比对通过、Iceberg 登记成功之后才允许删除分区」。两者同时存在时，动态分区调度会先把分区删掉——**这正是归档门禁想要防住的数据丢失窗口，在配置层面却没有落实**。

修正方案三条：

1. `dynamic_partition.start` 设为 `-3650`（十年），**只保留自动建分区能力，关闭自动回收**；
2. `DROP PARTITION` 交由归档调度器在校验通过后显式执行（IF-10，DD-004）；
3. 新增自监控 SLI **「已过期但未归档的分区数」**（X-001），阈值 > 2 告警——归档管线卡住时分区会无限堆积，必须有人知道，但**绝不允许配置自动 DROP 兜底**，因为删除不可逆。

#### 3.2.2 其他设计要点

- **DUPLICATE KEY 而非主键模型**：日志只写不更新，DUPLICATE 模型写入开销最低；幂等去重交给 Stream Load 的 label 机制（IF-3）+ 微量重复容忍。
- **`tenant_id` 作为 DUPLICATE KEY 前缀**：tablet 内数据按 `(tenant_id, ts)` 排序，租户过滤可借助前缀稀疏索引与 zone map 裁剪——这补偿了 RANDOM 分桶没有分桶裁剪的损失。
- **分桶策略 RANDOM**：此类负载的过滤性由时间分区裁剪 + 倒排索引承担，分桶裁剪收益边际；而 `HASH(tenant_id)` 会让大租户永久落到同一批 bucket，数据倾斜不可控。RANDOM 保证写入绝对均衡，**但必须配套 `load_to_single_tablet=true`**（IF-3，DD-001）。
- **倒排索引选择性建设（路径级）**：body 必建；高频过滤字段（service / trace_id）必建；attributes **不对整个 VARIANT 建索引**，改为对 Schema Template 声明的关键路径逐个建路径级索引（`field_pattern`）。对"任意字段关键字搜"的兜底能力由 body 全文索引 + DOC mode 的整条文档扫描承担，代价是长尾字段的精确过滤会慢一档——这是用确定的索引体积换不确定的长尾查询，在 SaaS 成本模型下是对的取舍。

#### 3.2.3 分桶数：官方双规则的联立

官方日志实践给出两条**并列**规则：① 单桶压缩后约 5 GB；② 桶数约为**集群磁盘总数的 3 倍**。

早期的单盘配置（4 台 BE × 1 块 1TB NVMe）只有 4 块盘，规则 ② 反推仅 12 桶、单 tablet 达 19 GB，与规则 ① 直接冲突。改为 4 台 × 3 盘 = 12 块盘后两条规则收敛：

| 表 | 日增 | 桶数 | 单 tablet | 规则 ① | 规则 ② |
|----|------|------|-----------|--------|--------|
| logs | 230 GB | 36 | 6.4 GB | ✓ | ✓（12 × 3 = 36） |
| spans | 17 GB | 8 | 2.1 GB | ✓ | 不适用，见 §3.4 |

**规模增长后两条规则会分叉**：在 10 TB/天 的点上，规则 ① 要 300 桶、规则 ② 只要 90 桶。届时**以规则 ①（tablet 物理大小）为准，规则 ② 视为并行度下限**。扩容推演见 X-003 §2.4。

---

### 3.3 半结构化建模基线（本模块核心）

本节确立全平台 VARIANT 列的统一建模方式。它同时是 §3.2 与 §3.4 两份建表语句中所有 VARIANT 相关参数的依据。

#### （1）子列取舍是渐进降级，不是天花板

**这是对早期判断的修正。** 早期版本把「子列数触顶」列为存储设计的首要风险，描述为"触顶即退化为 sparse 存储、查询性能崩塌"。对照官方 4.x VARIANT 文档后，该判断不准确。

官方机制：系统按「非空比例 / 稀疏度」对路径排序，**高频（不稀疏）路径优先参与子列列式提取并存为独立子列，其余低频路径合并进共享结构**；系统会优先让非空比例高、访问频率高的路径留在子列化中。也就是说超限后的行为是**自动 Top-N 取舍**，而非整列退化。

这个机制在本平台是有利的：全部租户的热点路径高度同构（都是 `http.*` / `db.*` / `rpc.*` / `k8s.*` 这些 OTel 语义约定字段），Top-2048 会自然被这些共享字段占据，各租户的私有字段本就属于该进共享结构的长尾。所以「1000 租户 × 50 字段 = 5 万子路径」的算术没错，但推不出"查询性能崩塌"的结论。

**官方硬约束清单**（设计边界，M1 需逐项对 4.0.8 复核）：

| 约束 | 值 | 影响 |
|------|----|------|
| `variant_max_subcolumns_count` 默认值 | 2048 | 官方明确不建议激进调大 |
| 实践上限 | 10000 | 接近该值时单机需 ≥128G 内存、≥32C，此时应优先转 DOC mode |
| JSON key 长度 | ≤ 255 | loader 侧校验并截断（DD-001 §3.4） |
| VARIANT 不能作主键或排序键 | — | 决定了 §3.2 的 DUPLICATE KEY 只能用固定列 |
| VARIANT 不能嵌套进 ARRAY / STRUCT | — | spans 的 events / links 只能用 MAP<STRING,STRING> |
| **同表内所有 VARIANT 列的 `variant_max_subcolumns_count` 必须一致** | 要么全 0、要么全 > 0 | 否则建表或 Schema Change 报错。这是 §3.2 / §3.4 中两个 VARIANT 列取相同值的原因 |

#### （2）真正的首要风险：跨租户类型冲突会静默摧毁索引

官方两条规则叠加起来，在多租户共享表下很危险：

- 「当同一路径出现不兼容类型（如同一字段既出现整数又出现字符串）时，将提升为 JSONB 类型以避免信息丢失」；
- 「类型变更导致索引丢失：子列类型发生不兼容变更（如 INT→JSONB）会丢失索引」。

后果：租户 A 的埋点写 `http.status_code: 200`，租户 B 写 `"200"`，该路径被提升为 JSONB，**索引丢失，且影响该 tablet 内的所有租户**。而 RANDOM 分桶意味着每个 tablet 都包含全部租户的数据（这是 §3.2.2 选 RANDOM 的代价的另一面），因此**一个客户的埋点质量问题会静默地拖垮所有客户在该字段上的查询性能**。没有报错，没有告警——官方 FAQ 里「为什么我的查询/索引没有生效」列的第一条正是这个。

这比子列数触顶严重得多：**触顶是渐进、可测量的，类型冲突是突发、静默的，而且事后不可修复**（只能重建分区）。故 §4 的实测优先级中此项置首位。

**三道防线**：

1. **Schema Template 锁类型（主）**：建表时为 OTel 语义约定路径声明强类型（§3.2 / §3.4 的 DDL）。官方规定 typed path **一定参与子列列式提取，且默认不计入 `variant_max_subcolumns_count` 预算**（`variant_enable_typed_paths_to_sparse` 默认 false）——既锁死类型，又不挤占租户的动态子列额度。这比"per-tenant 字段数配额"更根本：配额是限制租户，Schema Template 是把平台字段从竞争中直接摘出来。
2. **`otlp-loader` 侧类型强制**：typed path 上的值若类型不符，按声明类型转换；无法转换时旁路到影子路径并计一次类型冲突事件，**绝不让它污染主路径**（DD-001 §3.4 第 8 条）。
3. **监控**：类型冲突事件数为**零容忍指标**（X-001），任一 typed path 出现即排查该租户埋点。

per-tenant attributes 字段数配额（默认 200 个不同 key）与大租户独立库表（DD-006 §3.5，触发条件扩展为「数据量 **或** attributes 基数」）作为第二、第三道防线保留。

#### （3）两张表统一启用 DOC 编码模式

官方 VARIANT 选型矩阵把负载分为四类：

| 类 | 场景 | 机制 |
|----|------|------|
| A | 事件日志 / 审计日志 | 默认 |
| B | 宽而热点少的遥测 / 画像 | 稀疏列 |
| **C** | **模型输出 / Trace / 归档，写入优先或整条返回** | **DOC mode** |
| **D** | **关键路径需稳定类型** | **Schema Template（可与 A/B 组合）** |

本平台取 **C + D**。

**一处需要明确的更正**：DOC mode **不牺牲子字段过滤**。官方 DOC 编码机制第一条原文为「子路径**仍可**执行子列列式提取用于按路径查询，同时会**额外**保存一份原始 JSON 作为存储字段」。稀疏列与 DOC 编码互斥的是这两种"宽列应对机制"本身，不是子列化能力。

唯一可能的过滤退化来自 `variant_doc_materialization_min_rows`——小批量写入暂不子列化、留到 compaction 再做。**该参数对本设计不构成风险**：`otlp-loader` 按 64 MB 攒批，日志约 12.8 万行、span 约 6.4 万行，均远超该阈值（示例值 10000），子列化在写入时即完成，不存在"最新数据只能扫原始 JSON"的窗口。**反过来说，若 DD-001 后续为压延迟而调小批次，必须同步核对这个阈值**——这是 IF-3 双签条款覆盖的耦合点之一。

**选择 DOC mode 的收益**（官方数据）：

- compaction 内存下降约 **2/3**；稀疏宽列导入场景下导入性能提升约 **5~10 倍**——直接缓解 `-235 too many tablet versions` 风险；
- `SELECT variant_col` 整条文档读取效率获得**数量级**提升；
- 超宽列场景下比即时子列化更稳定，尤其当子列规模接近万列时。

**两张表选 DOC mode 的依据略有不同**：

- **spans 表**：trace 瀑布是「按 `trace_id` 取出该 trace 的全部 span 并完整返回 attributes」，整条文档读就是主查询路径，与官方 C 类完全吻合；
- **logs 表**：主查询是"关键字检索 + 字段过滤，返回一页约 100 行"，整条读只发生在用户展开单条时。按官方矩阵本可走 B 类（稀疏列），但选 DOC mode 有两个额外理由：一是它原生支撑「展开单条日志看完整 attributes JSON 树」的交互，**否则需要额外存一列原始 JSON 的 STRING**（官方在「限制」一节直接给出该变通建议），DOC mode 省掉这份冗余（对应 DD-008 §3.4）；二是两张表配置统一，降低运维与调参的认知负担。代价是放弃稀疏列分片，以及额外存一份原始 JSON。

**代价量化**：DOC 编码额外存原始 JSON，估计 logs 日增 +20%（190 → 230 GB/天）、traces +30%（13 → 17 GB/天），合计日增 215 → 260 GB/天，热缓存需求 2.4 → 2.9 TB。**该比例是估算，M1 必须实测校准**——原始 JSON 经 zstd 后的膨胀幅度取决于 attributes 的重复度，实测值可能显著低于估计。这个数字直接进 X-003 的单位经济性模型。

**分片参数**：`variant_doc_hash_shard_count` 默认 64，官方估算口径为「JSON key 总个数 / 128」。按 5 万子路径估算：

| 列 | 取值 | 依据 |
|----|------|------|
| `logs.log_attributes` | 384 | 5 万子路径 ÷ 128 |
| `logs.resource` | 64 | 基数低，用默认值 |
| `spans.span_attributes` | 128 | 路径数介于两者之间 |
| `spans.resource_attributes` | 64 | 同 logs.resource |

这些值需随 M1 实测的真实路径数调整。

#### （4）`storage_format = V3`

官方在「限制」一节明确：对于会产生大量独立子列的宽表场景（例如超过 2000 列），**强烈建议开启 V3 存储格式**，作用是把列元数据与 Segment Footer 解耦，加快文件打开速度并降低内存占用；官方 VARIANT 配置指南亦称"新的 VARIANT 表优先使用 V3，没有它宽 JSON 场景下文件打开慢、内存开销高"。

注意 `storage_format` 与 `inverted_index_storage_format` 是**两个不同的属性**：前者管列元数据布局、后者管索引存储格式，**两者都要显式设置**。这是容易遗漏的一处——早期版本只设了后者。

#### （5）倒排索引改为路径级（`field_pattern`）

写成 `INDEX idx_attrs (log_attributes) USING INVERTED` 是对**整个 VARIANT 列**建索引，会为其所有子列建索引，这本身就是索引膨胀的来源；官方 FAQ 也提示不要"误以为给 VARIANT 整体建的索引可用于子列"。

3.1.x / 4.0 起支持路径级索引：`PROPERTIES("field_pattern" = "<子路径>")` 只对指定路径建索引，支持通配批量（如 `'gen_ai.*'`），且**同一路径可并存分词与不分词两套索引**，`MATCH` 与 `=` 各走各的。**前提是路径必须在 Schema Template 中声明类型**，因此第 5 点与第 2 点是同一套机制的两面。

这取代了「监控索引膨胀率、超阈值租户降级为仅 body 索引」的事后补救方案——**从一开始就不产生膨胀，优于事后降级**。

#### （6）查询侧纪律（约束 DD-005）

官方要求：未启用 DOC mode 时读取整个 VARIANT 列会扫描所有子字段，一般不建议直接 `SELECT variant_col`，应使用 `SELECT v['path']` 路径投影；并避免大范围 `SELECT *`。

本设计虽已启用 DOC mode（整条读被优化），但列表页返回约 100 行，整条读 100 份 attributes 仍显著贵于只投影所需路径。**查询网关的 SQL 生成器需硬性约束**，规则矩阵见 DD-005 §3.2。

这条规则的存在本身也说明一件事：**DOC mode 不是"整条读变免费"，只是把它从不可接受变为可接受。**

#### （7）BE 侧参数

官方针对 VARIANT 宽列场景给出：

| 参数 | 取值 | 作用 | 代价 |
|------|------|------|------|
| `max_cumu_compaction_threads` | ≥ 8 | VARIANT 宽列的 compaction 开销远高于普通列，线程不足会直接体现为 Compaction Score 攀升与 `-235` 导入失败 | CPU 占用上升 |
| `vertical_compaction_num_columns_per_group` | 500 | 提升纵向合并效率 | **内存占用上升**，需与 BE 内存水位一起观察 |
| `segment_cache_memory_percentage` | 20 | 提升 segment 元数据缓存命中，配合 `storage_format=V3` 缓解宽列的文件打开开销 | 挤占查询内存 |

这三项与第 3 点的 DOC mode 是互补关系：DOC mode 把 compaction 内存降了约 2/3，上面的线程与分组参数则把腾出的空间转成吞吐。**M1 需在真实写入压力下联调这组参数**，单独调任何一项都可能把瓶颈推到另一处。写入侧适度增大客户端 batch（本设计 64 MB 已足够）。

#### （8）监控信号

官方给出的三类症状，已接入 X-001 的 SLI 体系：

| 症状 | 含义 | 动作 |
|------|------|------|
| Compaction Score 持续上升 | `variant_max_subcolumns_count` 过高或导入速率过快 | 降导入压力 / 复核子列上限 |
| 某字段查询性能突然退化，平台侧无配置变更 | schema 漂移——热点路径被挤出子列化 | 补入 Schema Template 锁定 |
| 同一路径频繁类型冲突 | 该路径必须锁类型，否则 JSONB 提升 + 索引失效 | 加入 Schema Template + loader 强制转换 |

---

### 3.4 Spans 表设计

```sql
-- 对齐 Doris 官方 otel_traces 列模型（与官方 OTel exporter 自动建表 schema 兼容，
-- 用户可绕过我方 agent 直接以官方 exporter 写入——开放性卖点）
CREATE TABLE spans (
    ts                  DATETIME(6)    NOT NULL COMMENT 'span 开始时间',
    tenant_id           VARCHAR(64)    NOT NULL COMMENT '租户',
    cluster_id          VARCHAR(64)    NOT NULL COMMENT 'K8s 集群（DD-001 §3.6）',
    service_name        VARCHAR(200)   NOT NULL,
    service_instance_id VARCHAR(200)   NULL,
    trace_id            VARCHAR(200)   NOT NULL,
    span_id             STRING         NOT NULL,
    parent_span_id      STRING         NULL,
    span_name           STRING         NULL COMMENT 'operation',
    span_kind           STRING         NULL,
    end_time            DATETIME(6)    NULL,
    duration            BIGINT         NULL COMMENT '微秒',
    status_code         STRING         NULL,
    status_message      STRING         NULL,

    -- 与 §3.2 同一套基线（Schema Template + DOC 编码）。
    --   spans 是官方 VARIANT 选型矩阵里 C 类的标准形态——trace 瀑布按
    --   trace_id 取出整条链路并完整返回 attributes，整条文档读就是主查询路径。
    span_attributes     VARIANT<
        'http.status_code'  : INT,
        'http.method'       : STRING,
        'http.route'        : STRING,
        'url.full'          : STRING,
        'db.system'         : STRING,
        'db.statement'      : STRING,
        'rpc.system'        : STRING,
        'rpc.service'       : STRING,
        'rpc.method'        : STRING,
        'messaging.system'  : STRING,
        'exception.type'    : STRING,
        'gen_ai.system'     : STRING,      -- AI 层依赖（DD-007 §3.9），必须锁类型
        'gen_ai.request.model'       : STRING,
        'gen_ai.usage.input_tokens'  : BIGINT,
        'gen_ai.usage.output_tokens' : BIGINT,
        properties(
            'variant_max_subcolumns_count'         = '2048',
            'variant_enable_doc_mode'              = 'true',
            'variant_doc_hash_shard_count'         = '128',
            'variant_doc_materialization_min_rows' = '10000'
        )
    >                                  NULL,
    resource_attributes VARIANT<
        'k8s.namespace.name'  : STRING,
        'k8s.deployment.name' : STRING,
        'k8s.node.name'       : STRING,
        'host.name'           : STRING,
        properties(
            'variant_max_subcolumns_count' = '2048',  -- 同表内必须一致
            'variant_enable_doc_mode'      = 'true',
            'variant_doc_hash_shard_count' = '64'
        )
    >                                  NULL,

    events              ARRAY<STRUCT<timestamp:DATETIME(6), name:STRING, attributes:MAP<STRING,STRING>>> NULL,
    links               ARRAY<STRUCT<trace_id:STRING, span_id:STRING, trace_state:STRING, attributes:MAP<STRING,STRING>>> NULL,
    scope_name          STRING         NULL,
    scope_version       STRING         NULL,

    INDEX idx_trace_id   (trace_id)      USING INVERTED,
    INDEX idx_svc        (service_name)  USING INVERTED,
    INDEX idx_span_name  (span_name)     USING INVERTED,
    INDEX idx_status     (status_code)   USING INVERTED,
    -- 路径级索引取代整列索引（§3.3 第 5 点）
    INDEX idx_sa_status  (span_attributes) USING INVERTED PROPERTIES("field_pattern"="http.status_code"),
    INDEX idx_sa_route   (span_attributes) USING INVERTED PROPERTIES("field_pattern"="http.route"),
    INDEX idx_sa_dbsys   (span_attributes) USING INVERTED PROPERTIES("field_pattern"="db.system"),
    INDEX idx_sa_rpcm    (span_attributes) USING INVERTED PROPERTIES("field_pattern"="rpc.method"),
    INDEX idx_sa_exc     (span_attributes) USING INVERTED PROPERTIES("field_pattern"="exception.type"),
    -- 通配批量：一条索引覆盖全部 gen_ai.* 路径，服务 DD-007 的 AI 可观测性查询
    INDEX idx_sa_genai   (span_attributes) USING INVERTED PROPERTIES("field_pattern"="gen_ai.*"),
    INDEX idx_ra_ns      (resource_attributes) USING INVERTED PROPERTIES("field_pattern"="k8s.namespace.name"),
    INDEX idx_ra_host    (resource_attributes) USING INVERTED PROPERTIES("field_pattern"="host.name")
    -- 已删除：idx_span_id（不存在按 span_id 点查的场景，而它是每行唯一的
    --         超高基数列，倒排索引体积接近数据本体）
    -- 已删除：idx_duration（数值范围查询走 Doris 自带的 min/max zone map
    --         + 列扫描优于倒排索引，且 duration 同样是超高基数列）
)
ENGINE=OLAP
DUPLICATE KEY(tenant_id, service_name, ts)
PARTITION BY RANGE(ts) ()
DISTRIBUTED BY RANDOM BUCKETS 8           -- 17GB/天 ÷ 8 ≈ 2.1GB/tablet
                                          -- 未按「磁盘数12 × 3 = 36」取值：那会让单
                                          -- tablet 仅 0.5GB，小文件与元数据开销更不划算。
                                          -- 官方双规则在小表上以「5GB/桶」为主，磁盘规则
                                          -- 退化为并行度下限，8 桶 ≥ BE 数 4，并行度够用。
PROPERTIES (
    "compression" = "zstd",
    "compaction_policy" = "time_series",
    "storage_format" = "V3",                       -- 同 §3.2
    "inverted_index_storage_format" = "V2",
    "dynamic_partition.enable" = "true",       -- 同 §3.2，不设 create_history_partition
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3650",           -- 同 §3.2，关闭自动回收
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);
```

**查询路径**：trace 瀑布点查 = 按天分区裁剪 + `trace_id` 倒排索引定位，SLO-3 的 800ms 预算达标压力不大；检索列表页（service / span_name / status 过滤）走倒排索引 + 列存扫描，duration 范围过滤走 zone map。

**`duration` 保留原始微秒值，P50/P99 由 `percentile_approx` 在线计算**——原始 span 全量留存，不存在预聚合丢分位数的问题。这条决策的反面教材见 X-003 §2.8：同类产品把 metrics 全部落 AGGREGATE KEY 预聚合表，分布信息不可逆丢失，产品内 AI 自认"无法查询 P99"。

**索引删减说明**：早期版本建了 8 个倒排索引，其中 `span_id` 与 `duration` 是每行唯一或近似唯一的超高基数列，倒排索引的 term dictionary 体积接近原始数据本体，而查询收益为零（span_id）或为负（duration）。删掉这两个后，spans 表的索引占比预期从接近 100% 降到 40% 量级，直接改善 BE 缓存容量需求（X-003）。

`trace_id` 同样是超高基数列**但必须保留**——它是 trace 点查与 log↔trace 关联的唯一路径，其膨胀率纳入 §4 实测。

---

### 3.5 metric_exemplars 表

**背景**：早期设计假设 VictoriaMetrics 原生支持 exemplar 存储与查询。据现有资料，VM 长期未实现 Prometheus exemplars，remote-write 中的 exemplar 字段会被丢弃，官方立场是建议改用 trace_id label 关联。**若属实，SD-000 的 G4 在原架构上根本没有落点**——这不是体验降级，是功能不存在。M1 第一周必须实测确认（SD-000 Q-5）。

**方案**：由 metrics-ingest 在写入路径上抽取 exemplar（规则见 DD-003 §3.7，即 IF-5），单独落 Doris 小表。这比依赖 VM 原生能力更强——可 SQL 过滤、可按服务聚合、可与 spans 表直接 JOIN。

```sql
CREATE TABLE metric_exemplars (
    ts           DATETIME(3)   NOT NULL,
    tenant_id    VARCHAR(64)   NOT NULL,
    cluster_id   VARCHAR(64)   NOT NULL,
    service      VARCHAR(128)  NULL,
    metric_name  VARCHAR(256)  NOT NULL,
    labels       VARIANT       NULL COMMENT '产生该 exemplar 的 series labels',
    value        DOUBLE        NULL,
    trace_id     VARCHAR(64)   NOT NULL,
    span_id      VARCHAR(16)   NULL,
    span_status  VARCHAR(16)   NULL COMMENT 'metrics-ingest 无法填充时留空，由 MV 回填',
    INDEX idx_metric (metric_name) USING INVERTED,
    INDEX idx_trace  (trace_id)    USING INVERTED,
    INDEX idx_svc    (service)     USING INVERTED
)
ENGINE=OLAP
DUPLICATE KEY(tenant_id, ts)
PARTITION BY RANGE(ts) ()
DISTRIBUTED BY RANDOM BUCKETS 4
PROPERTIES (
    "compression" = "zstd",
    "compaction_policy" = "time_series",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3650",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);
```

> **为什么这张表的 `labels` 列不启用 DOC mode**：它的查询模式是按 `metric_name` / `service` 过滤后取少量行，`labels` 的基数远低于日志 attributes，不构成宽列场景。保持默认即可，避免为一张 250 MB/天 的小表引入额外的原始 JSON 存储。**这是本文档在拆分时明确化的一处，早期单体文档未说明该列为何与另两张表处理不同。**

**量级控制（关键）**：不加节制地全量落 exemplar 会产生每秒数千条记录。metrics-ingest 侧执行保留策略：**每 `(tenant_id, metric_name, service)` 每分钟最多保留 5 条**，优先级为「关联 span 为 ERROR」>「value 最大」>「随机」。按 1000 个活跃组合估算约 **700 万条/天，压缩后约 250 MB/天**，已计入 X-003 的"派生/元数据 12 GB/天"。这个策略足以支撑"从图上某个异常点跳到一条有代表性的 trace"，而这正是 exemplar 的全部用途。

**留存**：30 天（短于日志的热 + 温窗口，exemplar 的价值高度集中在近期排障）。

---

### 3.6 Schema 演进策略

> **本节为 v2.0 新增，单体文档 v1.3 中完全不存在，需评审确认。** 表结构是 IF-6 契约，但此前没有任何变更流程——这是拆分后暴露的缺口。

#### 3.6.1 变更分级

| 级别 | 变更类型 | 兼容性 | 流程 |
|------|----------|--------|------|
| **L0 无感** | 加普通列（NULL 默认值）、加路径级索引 | 向后兼容，下游无需改动 | DD-002 owner 自行决定，周知 DD-004 / DD-005 |
| **L1 需协调** | Schema Template 增加 typed path、调整 `variant_doc_hash_shard_count` | 向后兼容，但影响存储占用与查询计划 | 通知 DD-001（loader 需同步类型强制规则）、DD-005（新路径可用于过滤） |
| **L2 破坏性** | 改列类型、删列、删索引、改分桶键、改排序键 | **不兼容** | **必须走架构评审**，并给出 DD-004 / DD-005 的迁移窗口 |
| **L3 不可逆** | 删除分区 | — | **不属于 Schema 变更，走 IF-10 归档门禁**（DD-004），本模块无权执行 |

#### 3.6.2 各类变更的在线性

- **加列**：Doris 支持轻量级 Schema Change，加 NULL 列近似瞬时，不重写数据；
- **加索引**：对**新写入数据**立即生效，历史分区需显式 BUILD INDEX，期间占用 compaction 资源，应在低峰执行并观察 Compaction Score；
- **改 Schema Template 的 typed path**：**只对新分区生效**。已有分区中该路径若已因类型冲突被提升为 JSONB，**索引不会自动恢复**——这是 §3.3（2）说的"事后不可修复"，只能重建分区；
- **改分桶数（`dynamic_partition.buckets`）**：**只对新分区生效，须提前一个分区周期调整**，旧分区不自动 rebalance。该项接入 X-003 的容量水位巡检，由自动任务提单。

#### 3.6.3 typed path 的增量维护

Schema Template 的路径清单不是一次定死的。随着租户接入，会有新的高频语义约定路径出现。维护节奏：

1. 由 X-001 的 schema 漂移探针发现"某热点路径未被子列化"或"某路径反复类型冲突"；
2. DD-002 owner 评估是否纳入 Schema Template；
3. 纳入后走 L1 流程，同步 DD-001 的类型强制规则；
4. **注意只对新分区生效**，历史数据的查询性能不会回溯改善。

---

## 4. Open Questions（本模块）

| # | 问题 | 时点 | 不达标的后果 |
|---|------|------|-------------|
| **Q2-1** | **跨租户类型冲突的发生率与爆炸半径**：真实多租户埋点下，未被 Schema Template 覆盖的路径出现不兼容类型的频率；一旦发生 JSONB 提升，该 tablet 内其他租户的查询退化幅度；loader 侧强制转换的拦截率与 CPU 开销 | M1 | 决定 Schema Template 的路径清单要覆盖多广、是否需要按租户隔离 tablet（即放弃 RANDOM 分桶） |
| **Q2-2**<br>（优先级因 Q2-9 关闭而**上升**） | **DOC mode 下 `variant_max_subcolumns_count` 与路径级索引是否完全生效**。官方称子路径"仍可"子列化，但未明确说明与子列上限、`field_pattern` 索引的交互细节 | M1 | **这是 §3.3 整个方案成立的前提。** 若为否，logs 表必须退回稀疏列模式（官方 B 类），spans 表因整条读是主路径仍保留 DOC mode，代价是两表配置不一致。<br>**引擎已定稿后，这条退路成为唯一退路**——不再有"换 ClickHouse"的兜底，因此 M1 必须把退路本身也验证一遍（退回稀疏列后的检索 p95 与存储成本是否仍满足 SLO-2 与 G6），而不是只验证主方案 |
| **Q2-3** | **DOC 编码额外存储开销的真实比例**。§3.3 用的 logs +20% / traces +30% 是估算值 | M1 | 直接决定 X-003 的单位经济性，进而决定 G6 能否验收 |
| **Q2-4** | 共享表下子列数随租户数的增长曲线，以及 Top-N 取舍后长尾路径的查询衰减幅度 | M1 | 决定 per-tenant 字段数配额定在多少、大租户线是否需要按 attributes 基数触发 |
| **Q2-5** | **路径级索引 vs 整列索引的体积对比**，验证 §3.3（5）确实消除了膨胀 | M1 | 不达标则需缩减索引清单 |
| **Q2-6** | `trace_id` 超高基数列倒排索引的体积占比 | M1 | 若占比过高，需评估 bloom filter 等替代方案 |
| **Q2-7** | `variant_doc_hash_shard_count` 取值验证：按「JSON key 总数/128」估算的 384/128/64 是否合理 | M1 | 调参项，不影响架构 |
| **Q2-8** | 排序键前置 `tenant_id` 相对官方基线 `DUPLICATE KEY(ts)` 的性能代价 | M1 | 若代价显著，需重新权衡租户裁剪与"查询最新 N 条"加速 |
| ~~**Q2-9**~~ | ~~Doris vs ClickHouse 最终裁决~~ | — | **已关闭（2026-10）：Doris 定稿，不做对比 PoC**（SD-000 Q-1）。本文档的引擎前提由此固定，但同时意味着 **Q2-2 的退路是唯一退路**，见下 |
| **Q2-11**<br>（由 IF-007 提出，**M1 必须处理**） | **两项表结构变更需会签**：① `row_ref` 单行稳定定位键——IF-007 §5.1 要求它"至少 24 小时有效且跨 compaction 有效"，这筛掉了查询层合成方案，只剩写入时生成行内唯一标识；② `severity` 改为独立物化列——直方图按 severity 堆叠是高频操作，若它落在半结构化列且未被子列化，每次渲染都要付长尾聚合的代价 | **M1** | 两者都是 IF-6 破坏性变更，**越晚做代价越高**；①不做则 FE-07 单条展开与 FE-08 上下文无法交付（M2 阻塞） |
| ~~**Q2-10**~~<br>（由 DD-001 提出，IF-3 双签范围） | ~~Stream Load label 的批次边界在 consumer rebalance 后未定义~~ | — | **已关闭（B3，规则正文在 DD-001 §3.4）**：`end(start, log)` 为纯函数；全局格子 `S=4096`；64MB 为确定性体积帽；2s 不得单独决定 `endOffset`；label 的 `[start, end)` 必须等于实际写入范围；`label already exists` 视为成功并 commit 到该 `end`。本侧会签点：改 `S` / `B` 或改 label 格式须回到本文档签字 |

---

## 5. 撰写待办

- [ ] M1 实测后回填 §3.3（3）的真实存储开销比例，并同步 X-003
- [ ] M1 实测后确认 §3.3（1）的官方硬约束清单在 4.0.8 上逐项成立
- [x] Q2-10 label 批次边界对齐：接受 DD-001 §3.4 的 B3 定稿（改 S/B/label 格式须回签）
- [ ] 与 DD-001 owner 会签 IF-3 其余条款（重点：批次大小与 `variant_doc_materialization_min_rows` 的耦合）
- [ ] 向 DD-005 owner 交付 IF-6 的正式列清单与可用索引清单（含哪些过滤条件走索引、哪些走扫描）
- [ ] §3.6 Schema 演进策略需经架构评审确认后生效
