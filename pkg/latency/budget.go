// Package latency 把 SD-000 §8.1 的摄入延迟分段预算编码为代码。
//
// # 为什么预算要进代码而不只是留在文档里
//
// PLAN §2.4 的要求是"五段各自有数，不接受只给总和"。只给总和时 4.0s 的未分配余量会
// 掩盖某一段的严重超支，直到它吃掉全部缓冲。而要让五段可比、可告警，各段的段名必须在
// 网关与 otlp-loader 两个二进制里完全一致——否则监控侧拼不出一条链路。本包就是这个
// 段名与预算的唯一事实来源。
//
// # 直方图桶边界刻意包含预算值本身
//
// 每段的桶边界里一定有一个点等于该段预算（Buckets 保证这一点）。这样"该段超预算的
// 请求占比"就是一次直接的 bucket 相除，不需要插值估算分位数——分位数插值在尾部的误差
// 恰好落在我们最关心的区间上。
//
// # 预算 ≠ 估算
//
// SD-000 §8.1 已澄清：本表是 p99 的分段上限，不是预期耗时（设计估算为 3~5s）。
// DD-001 OQ-1 记录了这处口径差异。验收按预算，不要拿估算值交差。
package latency

import (
	"sort"
	"time"
)

// Stage 是 §8.1 的一个预算段。取值同时用作指标标签，变更即破坏监控查询。
type Stage string

const (
	// StageCollectorBatch 客户集群 Collector 攒批（含网络到达前的驻留）。
	StageCollectorBatch Stage = "collector_batch"
	// StageGateway 网络传输 + 摄入网关处理。
	StageGateway Stage = "gateway"
	// StageKafka Kafka 写入至可消费。
	StageKafka Stage = "kafka"
	// StageLoaderBatch otlp-loader 攒批。
	StageLoaderBatch Stage = "loader_batch"
	// StageStreamLoad Stream Load 提交至可见。
	StageStreamLoad Stage = "stream_load"
)

// Component 是产出某段观测值的进程。
//
// 它与"责任模块"不是一回事：StageStreamLoad 的责任模块是 DD-002（IF-3 双签），但观测值
// 只能由 otlp-loader 这个进程产出，因为只有它看得到 Stream Load 的提交与返回。
type Component string

const (
	ComponentGateway Component = "ingest-gateway"
	ComponentLoader  Component = "otlp-loader"
)

// SLOIngestEndToEnd 是 SLO-1 的端到端上限。
const SLOIngestEndToEnd = 10 * time.Second

// Unallocated 是架构 owner 持有的未分配余量。
//
// 任一模块超支需走评审申请，不得自行占用（SD-000 §8.1）。本常量存在的意义是让
// TestBudgetsSumToSLO 能把"有人悄悄把自己的预算从余量里加满"变成一次测试失败。
const Unallocated = 4 * time.Second

// StageSpec 是一段的完整定义。
type StageSpec struct {
	Stage Stage
	// Budget 是该段的 p99 上限。
	Budget time.Duration
	// Owner 是 SD-000 §8.1 的责任模块。
	Owner string
	// EmittedBy 是实际产出该段观测值的进程。
	EmittedBy Component
	// Note 记录该段观测口径上的已知偏差，为空表示可直接测量。
	Note string
}

var specs = []StageSpec{
	{
		Stage: StageCollectorBatch, Budget: 1000 * time.Millisecond,
		Owner: "DD-001", EmittedBy: ComponentGateway,
		Note: "网关只能用客户端自报的事件时间戳做代理观测，受客户集群时钟偏移影响；" +
			"负值不记入直方图（压成 0 会让分位数向下偏）而单独计数，不并入网关自身预算",
	},
	{
		Stage: StageGateway, Budget: 500 * time.Millisecond,
		Owner: "DD-001", EmittedBy: ComponentGateway,
	},
	{
		Stage: StageKafka, Budget: 500 * time.Millisecond,
		Owner: "DD-001", EmittedBy: ComponentGateway,
		Note: "以 acks=all 的生产确认耗时作为\"写入至可消费\"的代理：ISR 全部落盘即可被消费",
	},
	{
		Stage: StageLoaderBatch, Budget: 2000 * time.Millisecond,
		Owner: "DD-001", EmittedBy: ComponentLoader,
	},
	{
		Stage: StageStreamLoad, Budget: 2000 * time.Millisecond,
		Owner: "DD-002（IF-3 双签）", EmittedBy: ComponentLoader,
	},
}

// Stages 按链路顺序返回全部五段。
func Stages() []StageSpec {
	out := make([]StageSpec, len(specs))
	copy(out, specs)
	return out
}

// StagesFor 返回由指定进程产出观测值的段。
//
// 摄入网关只产出前三段。后两段由 otlp-loader 产出（PLAN B4，尚未实现），本函数的存在
// 就是让"网关这边只有三段"是一个显式的、可查询的事实，而不是实现遗漏。
func StagesFor(c Component) []StageSpec {
	var out []StageSpec
	for _, s := range specs {
		if s.EmittedBy == c {
			out = append(out, s)
		}
	}
	return out
}

// Spec 返回某段的定义。
func Spec(st Stage) (StageSpec, bool) {
	for _, s := range specs {
		if s.Stage == st {
			return s, true
		}
	}
	return StageSpec{}, false
}

// Budget 返回某段的预算，未定义的段返回 0。
func Budget(st Stage) time.Duration {
	s, _ := Spec(st)
	return s.Budget
}

// TotalBudget 返回五段预算之和（不含未分配余量）。
func TotalBudget() time.Duration {
	var sum time.Duration
	for _, s := range specs {
		sum += s.Budget
	}
	return sum
}

// UnionBuckets 返回覆盖全部五段的桶边界并集。
//
// 五段共用一个指标名 + stage 标签（见 internal/gateway/selfmon），而 Prometheus 的
// 直方图桶由指标名决定、不能按标签取值不同。取并集保证每一段的预算值仍然是一个精确的
// 桶边界——这是包注释里那条"超预算占比可直接相除"的性质得以跨段保留的唯一办法。
// 代价是桶数变多（约 20 个），在五段 × 若干组件的规模下无足轻重。
func UnionBuckets() []float64 {
	seen := make(map[float64]bool)
	var out []float64
	for _, s := range specs {
		for _, b := range Buckets(s.Stage) {
			if !seen[b] {
				seen[b] = true
				out = append(out, b)
			}
		}
	}
	sort.Float64s(out)
	return out
}

// Buckets 返回该段直方图的桶边界（秒）。
//
// 边界以预算为中心按倍率展开，并保证预算值本身是其中一个边界（见包注释）。
func Buckets(st Stage) []float64 {
	b := Budget(st)
	if b <= 0 {
		return nil
	}
	budget := b.Seconds()
	// 倍率覆盖 1/32 ~ 8 倍预算：下界足够看清常态（预期耗时远小于预算），
	// 上界足够看清病态（超预算 8 倍时已不必再分辨具体值）。
	factors := []float64{1.0 / 32, 1.0 / 16, 1.0 / 8, 1.0 / 4, 1.0 / 2, 0.75, 1, 1.5, 2, 4, 8}
	out := make([]float64, 0, len(factors))
	for _, f := range factors {
		out = append(out, budget*f)
	}
	return out
}
