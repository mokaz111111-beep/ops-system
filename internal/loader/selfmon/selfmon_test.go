package selfmon

import (
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/internal/loader/flatten"
	"github.com/mokaz/ops-system/internal/loader/streamload"
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

func TestLoaderStagesPreRegisteredIndependently(t *testing.T) {
	m := New()
	text := gather(t, m)
	for _, spec := range latency.StagesFor(latency.ComponentLoader) {
		if !strings.Contains(text, "stage="+string(spec.Stage)) {
			t.Errorf("零流量时必须出现 %s", spec.Stage)
		}
		if spec.EmittedBy != latency.ComponentLoader {
			t.Errorf("%s 不应由 loader 预注册", spec.Stage)
		}
	}
	if strings.Contains(text, "stage="+string(latency.StageGateway)) {
		t.Error("loader 不得预注册网关段")
	}
	if !strings.Contains(text, "component=otlp-loader") {
		t.Error("必须带 component=otlp-loader，才能与网关拼成一条链路")
	}
}

func TestStagesAreObservedSeparately(t *testing.T) {
	m := New()
	m.ObserveStage(latency.StageLoaderBatch, telemetry.SignalLogs, 200*time.Millisecond)
	m.ObserveStage(latency.StageStreamLoad, telemetry.SignalLogs, 50*time.Millisecond)
	m.ObserveStage(latency.StageLoaderBatch, telemetry.SignalLogs, -time.Second)

	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	sum := map[string]float64{}
	for _, f := range fams {
		if f.GetName() != "ops_ingest_stage_duration_seconds" {
			continue
		}
		for _, mt := range f.Metric {
			h := mt.GetHistogram()
			if h == nil {
				continue
			}
			stage := ""
			for _, l := range mt.GetLabel() {
				if l.GetName() == "stage" {
					stage = l.GetValue()
				}
			}
			if stage != "" {
				sum[stage] += h.GetSampleSum()
			}
		}
	}
	loaderSum := sum[string(latency.StageLoaderBatch)]
	streamSum := sum[string(latency.StageStreamLoad)]
	if loaderSum < 0.2-1e-9 || streamSum < 0.05-1e-9 {
		t.Fatalf("两段必须各自出数: %v", sum)
	}
	if loaderSum == streamSum {
		t.Fatal("两段被加成同一个数，违反「不接受只给总和」")
	}
}

func TestLoaderSpecificCounters(t *testing.T) {
	m := New()
	m.HeaderError("logs.raw")
	m.FlattenError(telemetry.SignalLogs)
	m.Conflict(flatten.Conflict{Tenant: "org-1", Path: "http.status_code", Declared: flatten.TypeInt})
	m.Oversized(telemetry.SignalLogs)
	m.LoadDone(telemetry.SignalLogs, streamload.OutcomeDuplicate, 3)

	text := gather(t, m)
	for _, want := range []string{
		"ops_otlp_loader_header_errors_total",
		"ops_otlp_loader_variant_type_conflict_total",
		"outcome=duplicate",
		"path=http.status_code",
		"ops_otlp_loader_loaded_rows_total",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("缺少 %q\n%s", want, text)
		}
	}
}
