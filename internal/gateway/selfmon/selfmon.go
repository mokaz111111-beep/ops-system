// Package selfmon 暴露摄入网关的自监控指标（X-001 §2.6、DD-001 §3.8）。
//
// # 为什么是 Prometheus 文本格式
//
// X-001 §2.6 要求所有组件把指标送进独立的元集群（小规模 VM + Doris）。VM 的采集面与
// Prometheus 一致，因此网关只需按 Prometheus 约定暴露 /metrics，不需要自己推送——
// 推模式还要在网关里实现重试与缓冲，而网关恰恰不该持有状态。
//
// # 分段打点：五段用一个指标名 + stage 标签
//
// PLAN §2.4 要求 §8.1 的五段各自有数。本网关产出前三段（latency.StagesFor 给出这个划分），
// 后两段由 otlp-loader 产出。两个进程写同一个指标名 ops_ingest_stage_duration_seconds，
// 只以 stage 与 component 标签区分，这样"把五段拼成一条链路"是一次 sum by (stage)，
// 而不是五个指标名的手工对齐。桶边界取五段并集，见 latency.UnionBuckets。
//
// # 关于 tenant 标签的基数
//
// DD-001 §3.8 明确要求"各错误码（E1~E9）分租户计数"，供容量分析与合规审计使用。
// 带租户标签必然带来基数，因此只在三处使用：错误码计数、限流拒绝计数、以及接收量计数。
// 延迟直方图不带租户标签——直方图的基数是标签组合数 × 桶数，在 50 租户规模下就会压垮
// 元集群，而按租户拆延迟的需求用接收量 + 错误码已能覆盖大部分排障场景。
package selfmon

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mokaz/ops-system/internal/gateway/authn"
	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/latency"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

const (
	namespace = "ops"
	subsystem = "ingest_gateway"
)

// Metrics 是网关的指标集合。
type Metrics struct {
	reg *prometheus.Registry

	stage *prometheus.HistogramVec

	requests *prometheus.CounterVec
	errors   *prometheus.CounterVec

	acceptedRecords *prometheus.CounterVec
	acceptedBytes   *prometheus.CounterVec
	rejectedRecords *prometheus.CounterVec

	throttled *prometheus.CounterVec

	clientClockSkew  *prometheus.CounterVec
	identityOverride *prometheus.CounterVec

	inflightBytes prometheus.Gauge
}

// New 创建指标集合并注册到自带的 Registry。
//
// 用独立 Registry 而不是 prometheus.DefaultRegisterer：默认注册表是进程全局单例，
// 会让两个测试用例互相看到对方注册的指标，也让"网关实例"无法在一个进程里存在两个
// （压测工具里偶尔需要）。
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	m.stage = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "ingest_stage_duration_seconds",
		Help:      "SD-000 §8.1 摄入延迟分段耗时，按 stage 分段上报",
		Buckets:   latency.UnionBuckets(),
	}, []string{"stage", "component", "signal"})

	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "requests_total",
		Help: "摄入请求数，outcome 取 accepted / partial / 或 IF-1 错误码",
	}, []string{"signal", "outcome"})

	m.errors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "errors_total",
		Help: "IF-1 错误码分租户计数（DD-001 §3.8）",
	}, []string{"code", "tenant", "cluster", "signal"})

	m.acceptedRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "accepted_records_total",
		Help: "已投递下游的记录条数，是落库对账的上游口径",
	}, []string{"signal", "tenant"})

	m.acceptedBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "accepted_bytes_total",
		Help: "已投递下游的解压后字节数",
	}, []string{"signal", "tenant"})

	m.rejectedRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "rejected_records_total",
		Help: "E9 partial success 中被拒的记录条数",
	}, []string{"signal", "tenant", "reason"})

	m.throttled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "throttled_total",
		Help: "限流拒绝次数，按 bytes / events 维度区分（DD-006 §3.3 维度 1）",
	}, []string{"tenant", "cluster", "dimension"})

	m.clientClockSkew = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "client_clock_skew_total",
		Help: "客户端事件时间戳落在网关时钟之后的请求数，collector_batch 段的可信度指标",
	}, []string{"signal"})

	m.identityOverride = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "client_identity_override_total",
		Help: "客户端自报身份属性被网关覆盖的次数，按字段区分（接入错误的治理信号）",
	}, []string{"field"})

	m.inflightBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "inflight_bytes",
		Help: "正在处理中的请求体字节数，E8 过载保护的判定依据",
	})

	reg.MustRegister(
		m.stage, m.requests, m.errors,
		m.acceptedRecords, m.acceptedBytes, m.rejectedRecords,
		m.throttled, m.clientClockSkew, m.identityOverride,
		m.inflightBytes,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// 预先实例化网关负责的三段，使它们在零流量时也出现在 /metrics 上。
	// PLAN §2.4 的验收是"五段各自有数"——若某段只在第一次观测后才出现，
	// 验收时看不到它与"这段没埋点"在面板上是同一个现象。
	for _, spec := range latency.StagesFor(latency.ComponentGateway) {
		for _, sig := range []telemetry.Signal{telemetry.SignalLogs, telemetry.SignalTraces} {
			m.stage.WithLabelValues(string(spec.Stage), string(latency.ComponentGateway), sig.String())
		}
	}
	return m
}

// Registry 暴露注册表，供 /metrics 处理器与测试使用。
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler 返回 /metrics 处理器。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// BindSources 把鉴权器与限流器的内部计数接成指标。
//
// 这两个组件各自已有并发安全的 Stats()，再复制一份计数器只会制造两个可能对不上的真相源，
// 因此这里用 CounterFunc / GaugeFunc 在抓取时现读。
func (m *Metrics) BindSources(res *authn.Resolver, lim *ratelimit.Limiter) {
	if res != nil {
		m.reg.MustRegister(
			gaugeFunc("authn_snapshot_version", "鉴权快照版本号（控制面全局单调版本）",
				func() float64 { return float64(res.Stats().SnapshotVersion) }),
			gaugeFunc("authn_snapshot_age_seconds", "鉴权快照年龄，X-001 的\"快照陈旧\"告警依据",
				func() float64 { return float64(res.Stats().SnapshotAgeSeconds) }),
			gaugeFunc("authn_snapshot_stale", "鉴权快照是否超过 Class-S TTL（1 表示此刻吊销未被执行）",
				func() float64 {
					if res.Stats().Stale {
						return 1
					}
					return 0
				}),
			counterFunc("authn_degraded_decisions_total", "使用陈旧快照做出的鉴权判定次数",
				func() float64 { return float64(res.Stats().DegradedDecisions) }),
		)
	}
	if lim != nil {
		m.reg.MustRegister(
			gaugeFunc("ratelimit_buckets", "存活限流桶数，与 evicted 一起判断内存是否在增长",
				func() float64 { return float64(lim.Stats().Buckets) }),
			counterFunc("ratelimit_buckets_evicted_total", "累计回收的空闲限流桶数",
				func() float64 { return float64(lim.Stats().EvictedBuckets) }),
			gaugeFunc("quota_snapshot_version", "配额快照版本号",
				func() float64 { return float64(lim.Stats().SnapshotVersion) }),
		)
	}
}

func gaugeFunc(name, help string, fn func() float64) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: subsystem, Name: name, Help: help,
	}, fn)
}

func counterFunc(name, help string, fn func() float64) prometheus.Collector {
	return prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: name, Help: help,
	}, fn)
}

// ObserveStage 记录一段耗时。负值被丢弃而不是记成 0：负值只可能来自时钟问题，
// 把它记成 0 会让分位数向下偏，掩盖真实延迟。
func (m *Metrics) ObserveStage(stage latency.Stage, sig telemetry.Signal, d time.Duration) {
	if d < 0 {
		return
	}
	m.stage.WithLabelValues(string(stage), string(latency.ComponentGateway), sig.String()).
		Observe(d.Seconds())
}

// Outcome 是一次请求的结局，作为 requests_total 的 outcome 标签。
const (
	OutcomeAccepted = "accepted"
	OutcomePartial  = "partial"
)

// RequestDone 记录一次请求的结局。outcome 为 IF-1 码或 OutcomeAccepted / OutcomePartial。
func (m *Metrics) RequestDone(sig telemetry.Signal, outcome string) {
	m.requests.WithLabelValues(sig.String(), outcome).Inc()
}

// Error 记录一次 IF-1 错误。租户未知（鉴权失败时）时标签取 "unknown"，
// 刻意不省略该序列——"鉴权失败的请求有多少"本身是容量与攻击面分析要看的数。
func (m *Metrics) Error(code string, key identity.QuotaKey, sig telemetry.Signal) {
	tenant, cluster := "unknown", "unknown"
	if key.TenantID != "" {
		tenant = string(key.TenantID)
	}
	if key.ClusterID != "" {
		cluster = string(key.ClusterID)
	}
	m.errors.WithLabelValues(code, tenant, cluster, sig.String()).Inc()
}

// Accepted 记录一次成功投递的量。
func (m *Metrics) Accepted(sig telemetry.Signal, tenant identity.TenantID, records int64, bytes int64) {
	m.acceptedRecords.WithLabelValues(sig.String(), string(tenant)).Add(float64(records))
	m.acceptedBytes.WithLabelValues(sig.String(), string(tenant)).Add(float64(bytes))
}

// RejectedRecords 记录 E9 的部分拒绝。
func (m *Metrics) RejectedRecords(sig telemetry.Signal, tenant identity.TenantID, reason string, n int64) {
	if n <= 0 {
		return
	}
	m.rejectedRecords.WithLabelValues(sig.String(), string(tenant), reason).Add(float64(n))
}

// Throttled 记录一次限流拒绝。
func (m *Metrics) Throttled(key identity.QuotaKey, dim ratelimit.Dimension) {
	m.throttled.WithLabelValues(string(key.TenantID), string(key.ClusterID), dim.String()).Inc()
}

// ClientClockSkew 记录一次客户端时钟超前。
func (m *Metrics) ClientClockSkew(sig telemetry.Signal) {
	m.clientClockSkew.WithLabelValues(sig.String()).Inc()
}

// IdentityOverride 记录一次客户端自报身份被覆盖。
func (m *Metrics) IdentityOverride(field string) {
	m.identityOverride.WithLabelValues(field).Inc()
}

// AddInflight 调整处理中字节数。
func (m *Metrics) AddInflight(delta int64) { m.inflightBytes.Add(float64(delta)) }
