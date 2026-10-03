package otlphttp

import (
	"bytes"
	"compress/gzip"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mokaz/ops-system/internal/gateway/authn"
	"github.com/mokaz/ops-system/internal/gateway/ingest"
	"github.com/mokaz/ops-system/internal/gateway/kafkasink"
	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/internal/gateway/selfmon"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingesterr"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

const testToken = "tok-http-abcdef"

func protoBody(records int) []byte {
	var recs []byte
	for i := 0; i < records; i++ {
		recs = append(recs, 0x12, 0x02, 0x08, byte(i)) // scope field 2 = empty-ish record
	}
	scope := recs
	// resourceLogs field 2 = scopeLogs; wrap as LEN
	enc := func(field int, v []byte) []byte {
		tag := byte(field<<3 | 2)
		return append([]byte{tag, byte(len(v))}, v...)
	}
	res := enc(2, enc(2, scope))
	return enc(1, res)
}

func setupMux(t *testing.T, fake *kafkasink.Fake) http.Handler {
	t.Helper()
	hash, err := identity.HashToken(testToken)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	resolver := authn.NewResolver(&authn.Snapshot{
		Version: 1, FetchedAt: now,
		Bindings: map[identity.TokenHash]identity.Binding{
			hash: {
				TenantID: "org-1", ProjectID: "p", ClusterID: "c1",
				TokenHash: hash, TenantStatus: identity.StatusActive, ClusterStatus: identity.StatusActive,
			},
		},
	}, authn.WithClock(func() time.Time { return now }))
	if fake == nil {
		fake = kafkasink.NewFake()
	}
	p := ingest.New(resolver, ratelimit.New(nil),
		&kafkasink.Router{Topics: kafkasink.DefaultTopics()},
		fake, selfmon.New(),
		ingest.WithClock(func() time.Time { return now }),
	)
	return NewMux(p, []telemetry.Signal{telemetry.SignalLogs, telemetry.SignalTraces})
}

func doPOST(mux http.Handler, path string, hdr map[string]string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentTypeProtobuf)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestHTTPSuccessEmptyBody(t *testing.T) {
	mux := setupMux(t, nil)
	w := doPOST(mux, PathLogs, map[string]string{"Authorization": "Bearer " + testToken}, protoBody(1))
	if w.Code != 200 {
		t.Fatalf("状态 %d body=%s", w.Code, w.Body.Bytes())
	}
	if w.Body.Len() != 0 {
		t.Errorf("全批接受必须是空 protobuf，得到 %d 字节", w.Body.Len())
	}
}

func TestHTTPAuthAndMalformed(t *testing.T) {
	mux := setupMux(t, nil)

	w := doPOST(mux, PathLogs, nil, protoBody(1))
	if w.Code != 401 {
		t.Errorf("无 Token 应为 401，得到 %d", w.Code)
	}

	w = doPOST(mux, PathLogs, map[string]string{
		"Authorization": "Bearer " + testToken,
		"Content-Type":  "application/json",
	}, []byte(`{}`))
	if w.Code != 400 {
		t.Errorf("JSON 应为 E3/400，得到 %d", w.Code)
	}

	w = doPOST(mux, PathLogs, map[string]string{"Authorization": "Bearer " + testToken}, []byte{0x0a, 0xff})
	if w.Code != 400 {
		t.Errorf("畸形 protobuf 应为 400，得到 %d", w.Code)
	}
}

func TestHTTPRetryableNever2xx(t *testing.T) {
	fake := kafkasink.NewFake()
	fake.SetErr(ingesterr.New(ingesterr.CodeSinkUnavailable, "down").WithRetryAfter(1500 * time.Millisecond))
	mux := setupMux(t, fake)
	w := doPOST(mux, PathLogs, map[string]string{"Authorization": "Bearer " + testToken}, protoBody(1))
	if w.Code != 503 {
		t.Fatalf("E6 应为 503，得到 %d", w.Code)
	}
	if w.Header().Get("Retry-After") != "2" {
		t.Errorf("1.5s 应向上取整为 2，得到 %q", w.Header().Get("Retry-After"))
	}
}

func TestHTTPMetricsNotRegistered(t *testing.T) {
	mux := setupMux(t, nil)
	w := doPOST(mux, PathMetrics, map[string]string{"Authorization": "Bearer " + testToken}, protoBody(1))
	if w.Code != 404 {
		t.Errorf("未启用的信号必须 404 而不是可重试 5xx，得到 %d", w.Code)
	}
}

func TestHTTPMethodNotAllowed(t *testing.T) {
	mux := setupMux(t, nil)
	req := httptest.NewRequest(http.MethodGet, PathLogs, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET 应为 405，得到 %d", w.Code)
	}
}

func TestHTTPGzipAndPlatformTokenHeader(t *testing.T) {
	mux := setupMux(t, nil)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(protoBody(1))
	_ = zw.Close()
	w := doPOST(mux, PathTraces, map[string]string{
		headerIngestToken:  testToken,
		"Content-Encoding": "gzip",
	}, buf.Bytes())
	if w.Code != 200 {
		t.Fatalf("gzip + 平台头应成功，得到 %d %s", w.Code, w.Body.Bytes())
	}
}

func TestEncodePartialSuccess(t *testing.T) {
	if encodeExportResponse(ingesterr.PartialSuccess{}) != nil {
		t.Fatal("全批接受必须是空消息")
	}
	b := encodeExportResponse(ingesterr.PartialSuccess{RejectedRecords: 3, Reason: "drop"})
	if len(b) == 0 {
		t.Fatal("部分拒绝必须带 partial_success")
	}
}

func TestEncodeStatusHasGRPCCode(t *testing.T) {
	b := encodeStatus(ingesterr.GRPCUnauthenticated, "no")
	if len(b) < 2 {
		t.Fatal("Status 编码过短")
	}
}

func TestWriteErrUnclassified(t *testing.T) {
	h := &Handler{pipeline: ingest.New(authn.NewResolver(nil), ratelimit.New(nil),
		&kafkasink.Router{}, kafkasink.NewFake(), selfmon.New())}
	w := httptest.NewRecorder()
	h.writeErr(w, errors.New("x"))
	if w.Code != 503 {
		t.Errorf("未分类错误应为 E8/503，得到 %d", w.Code)
	}
}

func TestReadBodyRejectsUnknownEncoding(t *testing.T) {
	mux := setupMux(t, nil)
	w := doPOST(mux, PathLogs, map[string]string{
		"Authorization":    "Bearer " + testToken,
		"Content-Encoding": "br",
	}, protoBody(1))
	if w.Code != 400 {
		t.Errorf("不支持的压缩应为 E3，得到 %d", w.Code)
	}
}

func TestTokenBareAuthorization(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, PathLogs, nil)
	req.Header.Set("Authorization", testToken)
	if got := token(req); got != testToken {
		t.Errorf("裸 Authorization 也应接受，得到 %q", got)
	}
}
