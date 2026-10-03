// Package selfmon 暴露 otlp-loader 的自监控指标（X-001 §2.6、PLAN B6）。
//
// 与网关共用指标名 ops_ingest_stage_duration_seconds，只以 stage 与 component
// 区分。loader 负责 loader_batch 与 stream_load 两段，各段独立 Observe，不接受
// 只给总和。桶边界与网关相同（latency.UnionBuckets），否则「超预算占比可直接相除」
// 在拼链路时会断掉。
package selfmon

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mokaz/ops-system/internal/loader/flatten"
	"github.com/mokaz/ops-system/internal/loader/streamload"
	"github.com/mokaz/ops-system/pkg/latency"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

const (
	namespace = "ops"
	subsystem = "otlp_loader"
)

// Metrics 是 loader 的指标集合。
type Metrics struct {
	reg *prometheus.Registry

	stage *prometheus.HistogramVec

	headerErrors  *prometheus.CounterVec
	flattenErrors *prometheus.CounterVec
	conflicts     *prometheus.CounterVec
	oversized     *prometheus.CounterVec
	loads         *prometheus.CounterVec
	loadedRows    *prometheus.CounterVec
}

// New 创建指标集合。
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	m.stage = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "ingest_stage_duration_seconds",
		Help:      "SD-000 §8.1 摄入延迟分段耗时，按 stage 分段上报",
		Buckets:   latency.UnionBuckets(),
	}, []string{"stage", "component", "signal"})

	m.headerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "header_errors_total",
		Help: "Kafka 消息头缺失或非法的条数；这些消息不从 payload 猜 tenant",
	}, []string{"topic"})

	m.flattenErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "flatten_errors_total",
		Help: "OTLP 展开失败的消息数",
	}, []string{"signal"})

	m.conflicts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "variant_type_conflict_total",
		Help: "typed path 强制转换失败并旁路到影子路径的次数（零容忍）",
	}, []string{"tenant", "path", "declared_type"})

	m.oversized = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "oversized_record_total",
		Help: "单条展开后 encoded_bytes ≥ 64MiB、独立成批的次数",
	}, []string{"signal"})

	m.loads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "stream_load_total",
		Help: "Stream Load 次数，按 outcome 区分（ok / duplicate / retry / fatal）",
	}, []string{"signal", "outcome"})

	m.loadedRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem,
		Name: "loaded_rows_total",
		Help: "Stream Load 成功（含 label already exists）写入的行数，落库对账的下游口径",
	}, []string{"signal"})

	reg.MustRegister(
		m.stage, m.headerErrors, m.flattenErrors, m.conflicts,
		m.oversized, m.loads, m.loadedRows,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	for _, spec := range latency.StagesFor(latency.ComponentLoader) {
		for _, sig := range []telemetry.Signal{telemetry.SignalLogs, telemetry.SignalTraces} {
			m.stage.WithLabelValues(string(spec.Stage), string(latency.ComponentLoader), sig.String())
		}
	}
	return m
}

// Registry 供测试使用。
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler 返回 /metrics。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveStage 记录 loader 负责的一段。负值丢弃。
func (m *Metrics) ObserveStage(stage latency.Stage, sig telemetry.Signal, d time.Duration) {
	if d < 0 {
		return
	}
	m.stage.WithLabelValues(string(stage), string(latency.ComponentLoader), sig.String()).
		Observe(d.Seconds())
}

func (m *Metrics) HeaderError(topic string) {
	if topic == "" {
		topic = "unknown"
	}
	m.headerErrors.WithLabelValues(topic).Inc()
}

func (m *Metrics) FlattenError(sig telemetry.Signal) {
	m.flattenErrors.WithLabelValues(sig.String()).Inc()
}

func (m *Metrics) Conflict(c flatten.Conflict) {
	tenant := c.Tenant
	if tenant == "" {
		tenant = "unknown"
	}
	m.conflicts.WithLabelValues(tenant, c.Path, string(c.Declared)).Inc()
}

func (m *Metrics) Oversized(sig telemetry.Signal) {
	m.oversized.WithLabelValues(sig.String()).Inc()
}

func (m *Metrics) LoadDone(sig telemetry.Signal, outcome streamload.Outcome, rows int64) {
	m.loads.WithLabelValues(sig.String(), outcome.String()).Inc()
	if outcome.Succeeded() && rows > 0 {
		m.loadedRows.WithLabelValues(sig.String()).Add(float64(rows))
	}
}
