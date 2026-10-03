// Package telemetry 定义三信号的标识，供摄入链路各层共用。
//
// 单独成包而不放在某个网关子包里，是因为信号这一维横跨 OTLP 线格式解析、Kafka topic
// 路由、自监控标签三处。放在其中任何一处都会让另外两处反向依赖它。
package telemetry

// Signal 是遥测信号的类别。
type Signal uint8

const (
	// SignalUnknown 零值。显式区分于三个有效值，避免未初始化的请求被当作日志处理。
	SignalUnknown Signal = iota
	SignalLogs
	SignalTraces
	SignalMetrics
)

// String 返回信号名。该值直接作为指标标签与 Kafka 消息头的取值，
// 变更会同时影响监控查询与下游 loader 的解析，不要随手改。
func (s Signal) String() string {
	switch s {
	case SignalLogs:
		return "logs"
	case SignalTraces:
		return "traces"
	case SignalMetrics:
		return "metrics"
	default:
		return "unknown"
	}
}

// ParseSignal 是 String 的逆操作。
func ParseSignal(s string) (Signal, bool) {
	switch s {
	case "logs":
		return SignalLogs, true
	case "traces":
		return SignalTraces, true
	case "metrics":
		return SignalMetrics, true
	default:
		return SignalUnknown, false
	}
}

// Valid 报告是否为三个有效信号之一。
func (s Signal) Valid() bool {
	return s == SignalLogs || s == SignalTraces || s == SignalMetrics
}

// BufferedByKafka 报告该信号是否经 Kafka 缓冲。
//
// Metrics 是 SD-000 §5.1 分层原则 1 的唯一例外：直连 vminsert，不过 Kafka。
// 这个判定放在类型上，使"顺手把 metrics 也投进 Kafka"这类改动需要先改这里。
func (s Signal) BufferedByKafka() bool {
	return s == SignalLogs || s == SignalTraces
}
