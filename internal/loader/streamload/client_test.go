package streamload

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mokaz/ops-system/pkg/loadbatch"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func sampleReq() Request {
	return Request{
		Range:  loadbatch.Range{Topic: "logs.raw", Partition: 0, Start: 0, End: 4096},
		Signal: telemetry.SignalLogs,
		Body:   []byte(`[{"ts":"2026-10-04 00:00:00.000","tenant_id":"org-1"}]`),
		Table:  "logs",
	}
}

func TestFakeLabelAlreadyExistsIsSuccess(t *testing.T) {
	f := NewFake()
	ctx := context.Background()
	req := sampleReq()

	first, err := f.Load(ctx, req)
	if err != nil || first.Outcome != OutcomeOK {
		t.Fatalf("首次: %+v %v", first, err)
	}
	second, err := f.Load(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Outcome != OutcomeDuplicate || !second.Outcome.Succeeded() {
		t.Fatalf("label already exists 必须视为成功: %+v", second)
	}
	if second.Label != req.Range.Label() {
		t.Fatalf("重试不得换 label: %s vs %s", second.Label, req.Range.Label())
	}
}

func TestFakeRejectsEmptyBody(t *testing.T) {
	f := NewFake()
	req := sampleReq()
	req.Body = nil
	_, err := f.Load(context.Background(), req)
	if err == nil {
		t.Fatal("空 Body 配非空 label 会制造静默缺口")
	}
}

func TestRetryKeepsSameLabel(t *testing.T) {
	f := NewFake()
	req := sampleReq()
	f.Next = &Result{Outcome: OutcomeRetry, Status: "Fail", Message: "[-235] too many tablet versions"}

	r1, err := f.Load(context.Background(), req)
	if err != nil || r1.Outcome != OutcomeRetry || r1.Outcome.Succeeded() {
		t.Fatalf("-235 不得 commit: %+v %v", r1, err)
	}
	// 重试必须仍用同一个 Range，因而同一个 label。
	r2, err := f.Load(context.Background(), req)
	if err != nil || r2.Outcome != OutcomeOK {
		t.Fatalf("同一 label 重试应成功: %+v %v", r2, err)
	}
	labels := f.Labels()
	if len(labels) != 2 || labels[0] != labels[1] || labels[0] != "logs.raw-0-0-4096" {
		t.Fatalf("重试换了 label: %v", labels)
	}
}

func TestClassifyStatus(t *testing.T) {
	cases := []struct {
		status, msg string
		want        Outcome
	}{
		{"Success", "OK", OutcomeOK},
		{"Publish Timeout", "timeout", OutcomeOK},
		{"Label Already Exists", "already exists", OutcomeDuplicate},
		{"Fail", "label already exists", OutcomeDuplicate},
		{"Fail", "[-235] too many tablet versions", OutcomeRetry},
		{"Fail", "[-238] too many segments", OutcomeRetry},
		{"Fail", "mem exceeded", OutcomeRetry},
		{"Fail", "quality failed", OutcomeFatal},
	}
	for _, tc := range cases {
		if got := ClassifyStatus(tc.status, tc.msg); got != tc.want {
			t.Errorf("Classify(%q, %q) = %s，期望 %s", tc.status, tc.msg, got, tc.want)
		}
	}
}

func TestHTTPLabelAlreadyExistsAndRetryHeaders(t *testing.T) {
	var labels []string
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("方法 %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/api/ops/logs/_stream_load") {
			t.Errorf("路径 %s", r.URL.Path)
		}
		if r.Header.Get("format") != "json" || r.Header.Get("strip_outer_array") != "true" {
			t.Errorf("缺 Stream Load 约定头: %v", r.Header)
		}
		if r.Header.Get("load_to_single_tablet") != "true" {
			t.Error("RANDOM 分桶必须配套 load_to_single_tablet")
		}
		labels = append(labels, r.Header.Get("label"))
		body, _ := io.ReadAll(r.Body)
		if len(body) == 0 {
			t.Error("空 body")
		}
		n++
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Status": "Fail", "Message": "[-235] too many tablet versions",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Status": "Label Already Exists", "Message": "label already exists",
			"ExistingJobStatus": "FINISHED",
		})
	}))
	defer srv.Close()

	cl, err := NewHTTP(HTTPConfig{
		FE: srv.URL, Database: "ops", TableLogs: "logs", User: "root",
		Client: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}

	req := sampleReq()
	first, err := cl.Load(context.Background(), req)
	if err != nil || first.Outcome != OutcomeRetry {
		t.Fatalf("首次 -235: %+v %v", first, err)
	}
	second, err := cl.Load(context.Background(), req)
	if err != nil || second.Outcome != OutcomeDuplicate || !second.Outcome.Succeeded() {
		t.Fatalf("duplicate 应成功: %+v %v", second, err)
	}
	if len(labels) != 2 || labels[0] != labels[1] || labels[0] != req.Range.Label() {
		t.Fatalf("HTTP 重试换了 label: %v", labels)
	}
}

func TestOutcomeCommitRule(t *testing.T) {
	if !OutcomeOK.Succeeded() || !OutcomeDuplicate.Succeeded() {
		t.Fatal("成功与 duplicate 都必须 commit 到 label.end")
	}
	if OutcomeRetry.Succeeded() || OutcomeFatal.Succeeded() {
		t.Fatal("失败不得 commit")
	}
}
