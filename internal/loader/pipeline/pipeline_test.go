package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/internal/loader/flatten"
	"github.com/mokaz/ops-system/internal/loader/kafkasrc"
	"github.com/mokaz/ops-system/internal/loader/streamload"
	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/loadbatch"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func metaHeaders() []ingestmsg.Header {
	return ingestmsg.Meta{
		TenantID:    "org-1",
		ClusterID:   "c-1",
		Signal:      telemetry.SignalLogs,
		RecordCount: 1,
		ReceivedAt:  time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC),
		ServiceName: "checkout",
	}.Headers()
}

func oneLog() []byte {
	// 最小 ExportLogsServiceRequest：1 resource / 1 record。
	rec := lenF(5, lenF(1, []byte("hi")))
	scope := lenF(2, rec)
	block := lenF(2, scope)
	return lenF(1, block)
}

func varint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func tag(field, wt int) []byte { return varint(uint64(field)<<3 | uint64(wt)) }

func lenF(field int, payload []byte) []byte {
	var b bytes.Buffer
	b.Write(tag(field, 2))
	b.Write(varint(uint64(len(payload))))
	b.Write(payload)
	return b.Bytes()
}

func msg(offset int64, hs []ingestmsg.Header, body []byte) kafkasrc.Message {
	return kafkasrc.Message{
		Topic: "logs.raw", Partition: 0, Offset: offset,
		Headers: hs, Value: body,
	}
}

func newPipe(src *kafkasrc.Fake, load *streamload.Fake) *Pipeline {
	return New(src, load, Config{
		Params:     loadbatch.Params{Stride: 4, MaxEncodedBytes: 64 << 20},
		IdleSubmit: 2 * time.Second,
		Flatten:    flatten.DefaultOptions(),
		TableLogs:  "logs",
		TableSpans: "spans",
		RetryWait:  time.Millisecond,
	}, Nop{}, nil)
}

func TestSameOffsetsSameLabel(t *testing.T) {
	src := kafkasrc.NewFake()
	load := streamload.NewFake()
	p := newPipe(src, load)
	body := oneLog()
	hs := metaHeaders()
	src.Push(msg(0, hs, body), msg(1, hs, body), msg(2, hs, body), msg(3, hs, body))
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if labels := load.Labels(); len(labels) != 1 || labels[0] != "logs.raw-0-0-4" {
		t.Fatalf("label=%v", labels)
	}
	commits := src.Commits()
	if len(commits) != 1 || commits[0].NextOffset != 4 {
		t.Fatalf("commit=%v", commits)
	}

	// 重放同一段：label already exists，仍 commit 到同一 end。
	src2 := kafkasrc.NewFake()
	p2 := newPipe(src2, load)
	src2.Push(msg(0, hs, body), msg(1, hs, body), msg(2, hs, body), msg(3, hs, body))
	if err := p2.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if labels := load.Labels(); len(labels) != 2 || labels[0] != labels[1] {
		t.Fatalf("重放换了 label: %v", labels)
	}
	if !load.Calls()[1].Result.Outcome.Succeeded() {
		t.Fatal("duplicate 必须成功并提交")
	}
}

func TestMissingHeadersNotGuessedFromPayload(t *testing.T) {
	src := kafkasrc.NewFake()
	load := streamload.NewFake()
	p := newPipe(src, load)
	// payload 里即使将来带 tenant，也不该被采用——这里连头都没有。
	src.Push(msg(0, nil, oneLog()), msg(1, nil, oneLog()), msg(2, nil, oneLog()), msg(3, nil, oneLog()))
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(load.Calls()); n != 0 {
		t.Fatalf("缺头不得写 Doris（会猜 tenant）: %d 次 Load", n)
	}
	if commits := src.Commits(); len(commits) != 1 || commits[0].NextOffset != 4 {
		t.Fatalf("缺头仍应消费 offset，否则毒消息堵分区: %v", commits)
	}
}

func TestIdleTwoSecondsDoesNotCutGrowingLog(t *testing.T) {
	src := kafkasrc.NewFake()
	load := streamload.NewFake()
	p := newPipe(src, load)
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }

	hs := metaHeaders()
	src.Push(msg(0, hs, oneLog()), msg(1, hs, oneLog())) // stride=4，未对齐
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Second)
	src.SetHWM("logs.raw", 0, 99) // 水位还在前面，不是 HWM 短批
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(load.Calls()) != 0 {
		t.Fatal("2s 不得在未到格子/体积帽/HWM 时决定 endOffset")
	}
}

func TestHWMIdleSubmitsShortBatch(t *testing.T) {
	src := kafkasrc.NewFake()
	load := streamload.NewFake()
	p := newPipe(src, load)
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }

	hs := metaHeaders()
	src.Push(msg(0, hs, oneLog()), msg(1, hs, oneLog()))
	src.SetHWM("logs.raw", 0, 2)
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(load.Calls()) != 0 {
		t.Fatal("刚追上 HWM 应等空闲窗口")
	}
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if labels := load.Labels(); len(labels) != 1 || labels[0] != "logs.raw-0-0-2" {
		t.Fatalf("HWM 短批 label=%v", labels)
	}
}

func TestRetryKeepsLabelAndDoesNotCommitOn235(t *testing.T) {
	src := kafkasrc.NewFake()
	load := streamload.NewFake()
	p := newPipe(src, load)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	load.Hook = func(req streamload.Request) (streamload.Result, error) {
		cancel()
		return streamload.Result{Outcome: streamload.OutcomeRetry, Status: "Fail", Message: "[-235] too many tablet versions", Label: req.Range.Label()}, nil
	}
	hs := metaHeaders()
	src.Push(msg(0, hs, oneLog()), msg(1, hs, oneLog()), msg(2, hs, oneLog()), msg(3, hs, oneLog()))
	_ = p.Step(ctx)
	if len(src.Commits()) != 0 {
		t.Fatal("-235 不得 commit")
	}
	if labels := load.Labels(); len(labels) == 0 || labels[0] != "logs.raw-0-0-4" {
		t.Fatalf("必须带着原 label 去打: %v", labels)
	}
}

func TestLoadedRowsUseHeaderTenant(t *testing.T) {
	src := kafkasrc.NewFake()
	load := streamload.NewFake()
	p := newPipe(src, load)
	hs := metaHeaders()
	src.Push(msg(0, hs, oneLog()), msg(1, hs, oneLog()), msg(2, hs, oneLog()), msg(3, hs, oneLog()))
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	body := string(load.Calls()[0].Request.Body)
	if !strings.Contains(body, "org-1") || strings.Contains(body, "forged") {
		t.Fatalf("写入行必须用头里的 tenant: %s", body)
	}
}
