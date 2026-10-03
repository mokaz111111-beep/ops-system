package selfmon

import (
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/latency"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func gather(t *testing.T, m *Metrics) string {
	t.Helper()
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, f := range fams {
		b.WriteString(*f.Name)
		b.WriteByte('\n')
		for _, mt := range f.Metric {
			for _, l := range mt.Label {
				b.WriteString(*l.Name)
				b.WriteByte('=')
				b.WriteString(*l.Value)
				b.WriteByte(' ')
			}
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func TestGatewayStagesPreRegistered(t *testing.T) {
	m := New()
	text := gather(t, m)
	for _, spec := range latency.StagesFor(latency.ComponentGateway) {
		needle := "stage=" + string(spec.Stage)
		if !strings.Contains(text, needle) {
			t.Errorf("零流量时也必须出现 %s，否则验收时与未埋点无法区分", needle)
		}
	}
	if strings.Contains(text, "stage="+string(latency.StageLoaderBatch)) {
		t.Error("网关不得预注册 loader 段——那是 otlp-loader 的职责")
	}
}

func TestObserveStageAndErrors(t *testing.T) {
	m := New()
	m.ObserveStage(latency.StageGateway, telemetry.SignalLogs, 12*time.Millisecond)
	m.ObserveStage(latency.StageGateway, telemetry.SignalLogs, -time.Second) // 负值丢弃
	m.Accepted(telemetry.SignalLogs, "org-1", 4, 128)
	m.Error("E5", identity.QuotaKey{TenantID: "org-1", ClusterID: "c1"}, telemetry.SignalLogs)
	m.Throttled(identity.QuotaKey{TenantID: "org-1", ClusterID: "c1"}, ratelimit.DimBytes)
	m.RequestDone(telemetry.SignalLogs, OutcomeAccepted)

	text := gather(t, m)
	for _, want := range []string{
		"ops_ingest_gateway_accepted_records_total",
		"ops_ingest_gateway_errors_total",
		"ops_ingest_gateway_throttled_total",
		"code=E5",
		"dimension=bytes",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("缺少 %q", want)
		}
	}
}

func TestStageHistogramHasNoTenantLabel(t *testing.T) {
	m := New()
	m.ObserveStage(latency.StageKafka, telemetry.SignalTraces, time.Millisecond)
	text := gather(t, m)
	// 直方图序列里不应出现 tenant= —— 基数是标签组合 × 桶数。
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "stage=") && strings.Contains(line, "tenant=") {
			t.Fatalf("延迟直方图不得带租户标签: %s", line)
		}
	}
}
