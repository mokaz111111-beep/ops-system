package ingest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mokaz/ops-system/internal/gateway/authn"
	"github.com/mokaz/ops-system/internal/gateway/kafkasink"
	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/internal/gateway/selfmon"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingesterr"
	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

const goodToken = "tok-pipeline-abcdef"

func varint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func tag(field, wt int) []byte { return varint(uint64(field)<<3 | uint64(wt)) }

func lenField(field int, payload []byte) []byte {
	var b bytes.Buffer
	b.Write(tag(field, 2))
	b.Write(varint(uint64(len(payload))))
	b.Write(payload)
	return b.Bytes()
}

func keyValue(key, val string) []byte {
	return append(lenField(1, []byte(key)), lenField(2, lenField(1, []byte(val)))...)
}

func logsBody(service string, records int, extra ...[]byte) []byte {
	var attrs []byte
	attrs = append(attrs, lenField(1, keyValue("service.name", service))...)
	for _, kv := range extra {
		attrs = append(attrs, lenField(1, kv)...)
	}
	var recs []byte
	for i := 0; i < records; i++ {
		recs = append(recs, lenField(2, []byte{0x08, byte(i)})...)
	}
	scope := lenField(2, recs)
	res := append(lenField(1, attrs), scope...)
	return lenField(1, res)
}

func testPipeline(t *testing.T, lim *ratelimit.Snapshot, prod kafkasink.Producer) (*Pipeline, *kafkasink.Fake, *authn.Resolver) {
	t.Helper()
	hash, err := identity.HashToken(goodToken)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	snap := &authn.Snapshot{
		Version:   1,
		FetchedAt: now,
		Bindings: map[identity.TokenHash]identity.Binding{
			hash: {
				TenantID: "org-1", ProjectID: "prj-prod", ClusterID: "c-sh-01",
				TokenHash: hash, TenantStatus: identity.StatusActive, ClusterStatus: identity.StatusActive,
			},
		},
	}
	resolver := authn.NewResolver(snap, authn.WithClock(func() time.Time { return now }))
	if lim == nil {
		lim = &ratelimit.Snapshot{FetchedAt: now}
	}
	limiter := ratelimit.New(lim, ratelimit.WithClock(func() time.Time { return now }))
	fake, _ := prod.(*kafkasink.Fake)
	if fake == nil && prod == nil {
		fake = kafkasink.NewFake()
		prod = fake
	}
	if prod == nil {
		prod = fake
	}
	p := New(resolver, limiter, &kafkasink.Router{Topics: kafkasink.DefaultTopics()}, prod, selfmon.New(),
		WithClock(func() time.Time { return now }),
		WithMaxBodyBytes(1024),
		WithMaxInflightBytes(4096),
	)
	return p, fake, resolver
}

func accept(p *Pipeline, body []byte) (Result, error) {
	return p.Accept(context.Background(), Request{
		Signal:     telemetry.SignalLogs,
		Token:      goodToken,
		Body:       body,
		ReceivedAt: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC),
	})
}

func codeOf(err error) ingesterr.Code {
	var ie *ingesterr.Error
	if errors.As(err, &ie) {
		return ie.Code()
	}
	return ""
}

func TestAcceptHappyPathInjectsIdentity(t *testing.T) {
	p, fake, _ := testPipeline(t, nil, nil)
	body := logsBody("checkout", 2)
	res, err := accept(p, body)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if res.RecordCount != 2 || res.Binding.TenantID != "org-1" {
		t.Errorf("结果不对: %+v", res)
	}
	recs := fake.Records()
	if len(recs) != 1 {
		t.Fatalf("应投递 1 条，得到 %d", len(recs))
	}
	meta, err := ingestmsg.ParseMeta(recs[0].Headers)
	if err != nil {
		t.Fatal(err)
	}
	if meta.TenantID != "org-1" || meta.ProjectID != "prj-prod" || meta.ClusterID != "c-sh-01" {
		t.Errorf("身份必须强制注入消息头: %+v", meta)
	}
	if string(recs[0].Value) != string(body) {
		t.Error("payload 不得被改写")
	}
	if string(recs[0].Key) != "org-1|checkout" {
		t.Errorf("分区键=%s", recs[0].Key)
	}
}

func TestAcceptErrorCodes(t *testing.T) {
	p, fake, _ := testPipeline(t, nil, nil)

	_, err := p.Accept(context.Background(), Request{Signal: telemetry.SignalLogs, ReceivedAt: time.Now()})
	if codeOf(err) != ingesterr.CodeTokenInvalid {
		t.Errorf("空 Token 应为 E1，得到 %v", err)
	}

	_, err = accept(p, []byte{0x0a, 0x05})
	if codeOf(err) != ingesterr.CodeMalformed {
		t.Errorf("畸形体应为 E3，得到 %v", err)
	}

	_, err = p.Accept(context.Background(), Request{
		Signal: telemetry.SignalLogs, Token: goodToken, Body: bytes.Repeat([]byte{1}, 2048),
		ReceivedAt: time.Now(),
	})
	if codeOf(err) != ingesterr.CodeBodyTooLarge {
		t.Errorf("超体积应为 E4，得到 %v", err)
	}

	fake.SetErr(ingesterr.New(ingesterr.CodeSinkUnavailable, "down").WithRetryAfter(time.Second))
	_, err = accept(p, logsBody("svc", 1))
	if codeOf(err) != ingesterr.CodeSinkUnavailable {
		t.Errorf("下游不可用应为 E6，得到 %v", err)
	}

	fake.SetErr(ingesterr.New(ingesterr.CodeBackpressure, "full").WithRetryAfter(time.Second))
	_, err = accept(p, logsBody("svc", 1))
	if codeOf(err) != ingesterr.CodeBackpressure {
		t.Errorf("背压应为 E7，得到 %v", err)
	}

	_, err = p.Accept(context.Background(), Request{
		Signal: telemetry.SignalMetrics, Token: goodToken, Body: logsBody("svc", 1),
		ReceivedAt: time.Now(),
	})
	if codeOf(err) != ingesterr.CodeSinkUnavailable {
		t.Errorf("未配置的信号应为 E6，得到 %v", err)
	}
}

func TestAcceptQuotaAndNeverFits(t *testing.T) {
	lim := &ratelimit.Snapshot{Default: ratelimit.Limits{
		BytesPerSec: 100, BytesBurst: 100, EventsPerSec: 2, EventsBurst: 2,
	}}
	p, _, _ := testPipeline(t, lim, nil)

	_, err := accept(p, logsBody("svc", 3))
	if codeOf(err) != ingesterr.CodeBodyTooLarge {
		t.Errorf("单批超过突发应为 E4 而非 E5，得到 %v", err)
	}

	p2, _, _ := testPipeline(t, lim, nil)
	_, err = accept(p2, logsBody("svc", 2))
	if err != nil {
		t.Fatalf("第一批应通过: %v", err)
	}
	_, err = accept(p2, logsBody("svc", 2))
	if codeOf(err) != ingesterr.CodeQuotaExceeded {
		t.Errorf("超配额应为 E5，得到 %v", err)
	}
}

func TestAcceptDoesNotFoldSinkError(t *testing.T) {
	p, fake, _ := testPipeline(t, nil, nil)
	fake.SetErr(errors.New("raw kafka"))
	_, err := accept(p, logsBody("svc", 1))
	var ie *ingesterr.Error
	if !errors.As(err, &ie) {
		t.Fatal("未分类错误也必须带 IF-1 码")
	}
	if ie.HTTPStatus() >= 200 && ie.HTTPStatus() < 300 {
		t.Fatal("下游错误绝不能变成 2xx")
	}
}

func TestAsIngestErrUnknownIsRetryable(t *testing.T) {
	ie := asIngestErr(errors.New("mystery"))
	if ie.Code() != ingesterr.CodeGatewayOverloaded || !ie.Retryable() {
		t.Fatalf("未知错误必须是可重试 E8，得到 %s retryable=%v", ie.Code(), ie.Retryable())
	}
}
