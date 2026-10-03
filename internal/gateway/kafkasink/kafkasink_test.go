package kafkasink

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingesterr"
	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func meta(sig telemetry.Signal, service string) ingestmsg.Meta {
	return ingestmsg.Meta{
		TenantID:    "org-1",
		ProjectID:   "prj-prod",
		ClusterID:   "c-sh-01",
		Signal:      sig,
		RecordCount: 3,
		ReceivedAt:  time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC),
		ServiceName: service,
	}
}

func TestRouteLogsKeyAndHeaders(t *testing.T) {
	r := &Router{Topics: DefaultTopics()}
	rec, err := r.Route(meta(telemetry.SignalLogs, "checkout"), []byte("otlp"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != TopicLogsRaw {
		t.Errorf("topic=%s", rec.Topic)
	}
	if string(rec.Key) != "org-1|checkout" {
		t.Errorf("logs 分区键必须是 tenant|service，得到 %q", rec.Key)
	}
	if string(rec.Value) != "otlp" {
		t.Error("payload 必须原样转发")
	}
	m, err := ingestmsg.ParseMeta(rec.Headers)
	if err != nil {
		t.Fatal(err)
	}
	if m.TenantID != "org-1" || m.ProjectID != "prj-prod" || m.ClusterID != "c-sh-01" {
		t.Errorf("身份必须进消息头: %+v", m)
	}
}

func TestRouteTracesUsesClusterNotTraceID(t *testing.T) {
	r := &Router{Topics: DefaultTopics()}
	rec, err := r.Route(meta(telemetry.SignalTraces, "checkout"), []byte("spans"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != TopicTracesRaw {
		t.Errorf("topic=%s", rec.Topic)
	}
	if string(rec.Key) != "org-1|c-sh-01" {
		t.Errorf("M1 traces 分区键是 tenant|cluster，得到 %q", rec.Key)
	}
}

func TestRouteUnknownServicePlaceholder(t *testing.T) {
	r := &Router{Topics: DefaultTopics()}
	rec, err := r.Route(meta(telemetry.SignalLogs, ""), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Key) != "org-1|unknown" {
		t.Errorf("缺 service.name 必须落到可见占位符，得到 %q", rec.Key)
	}
}

func TestRouteMetricsHasNoTopic(t *testing.T) {
	r := &Router{Topics: DefaultTopics()}
	_, err := r.Route(meta(telemetry.SignalMetrics, "m"), []byte("x"))
	if !errors.Is(err, ErrNoTopic) {
		t.Fatalf("metrics 不过 Kafka: %v", err)
	}
}

func TestKeyBucketsRotate(t *testing.T) {
	r := &Router{
		Topics:     DefaultTopics(),
		KeyBuckets: map[identity.TenantID]int{"org-1": 3},
	}
	seen := map[string]int{}
	for i := 0; i < 9; i++ {
		rec, err := r.Route(meta(telemetry.SignalLogs, "svc"), []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		seen[string(rec.Key)]++
	}
	if len(seen) != 3 {
		t.Fatalf("应轮转 3 个 bucket，得到 %v", seen)
	}
	for k := range seen {
		if !strings.HasPrefix(k, "org-1|svc#") {
			t.Errorf("打散键格式不对: %s", k)
		}
	}
}

func TestFakeRecordsAndErrorInjection(t *testing.T) {
	f := NewFake()
	rec := Record{Topic: "t", Key: []byte("k"), Value: []byte("v")}
	if err := f.Produce(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if f.Len() != 1 {
		t.Fatalf("Len=%d", f.Len())
	}
	f.SetErr(ingesterr.New(ingesterr.CodeSinkUnavailable, "down"))
	if err := f.Produce(context.Background(), rec); err == nil {
		t.Fatal("注入错误必须返回")
	}
	if f.Len() != 1 {
		t.Fatal("失败的 Produce 不得写入记录")
	}
}

func TestClassify(t *testing.T) {
	if Classify(nil) != nil {
		t.Fatal("nil 应原样返回")
	}

	var ie *ingesterr.Error
	err := Classify(errors.New("boom"))
	if !errors.As(err, &ie) || ie.Code() != ingesterr.CodeSinkUnavailable || !ie.Retryable() {
		t.Fatalf("未知错误必须是可重试 E6: %v", err)
	}
}
