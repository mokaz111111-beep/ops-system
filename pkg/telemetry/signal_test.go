package telemetry

import "testing"

func TestParseRoundTrip(t *testing.T) {
	for _, s := range []Signal{SignalLogs, SignalTraces, SignalMetrics} {
		got, ok := ParseSignal(s.String())
		if !ok || got != s {
			t.Errorf("ParseSignal(%q) = (%v, %v)，期望 (%v, true)", s.String(), got, ok, s)
		}
		if !s.Valid() {
			t.Errorf("%s 应 Valid", s)
		}
	}
}

func TestUnknownIsInvalid(t *testing.T) {
	if SignalUnknown.Valid() {
		t.Error("零值不得被视为有效信号——未初始化的请求会被当成日志处理")
	}
	if _, ok := ParseSignal("unknown"); ok {
		t.Error(`ParseSignal("unknown") 不应成功`)
	}
	if _, ok := ParseSignal(""); ok {
		t.Error("空串不应解析为有效信号")
	}
}

func TestBufferedByKafka(t *testing.T) {
	if !SignalLogs.BufferedByKafka() || !SignalTraces.BufferedByKafka() {
		t.Error("logs/traces 必须经 Kafka（SD-000 §5.1 分层原则 1）")
	}
	if SignalMetrics.BufferedByKafka() {
		t.Error("metrics 是分层原则 1 的唯一例外，不得经 Kafka")
	}
}
