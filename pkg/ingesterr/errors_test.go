package ingesterr

import (
	"errors"
	"testing"
	"time"
)

// TestRetryableNeverMapsTo2xx 是 IF-1 第一条不可协商规则的回归测试。
//
// 这条规则在文档里是一句话，在这里是一个会红的测试：任何可重试错误一旦被映射到
// 2xx，客户侧的持久队列就不会触发，兜底退化为静默丢数。
func TestRetryableNeverMapsTo2xx(t *testing.T) {
	for _, code := range Codes() {
		e := New(code, "")
		if !e.Retryable() {
			continue
		}
		if s := e.HTTPStatus(); s >= 200 && s < 300 {
			t.Errorf("%s 可重试但 HTTP 状态为 %d —— 客户端不会重试，数据会静默丢失", code, s)
		}
		if e.GRPCCode() == GRPCOK {
			t.Errorf("%s 可重试但 gRPC 状态为 OK", code)
		}
	}
}

// TestPlatformSideErrorsAreRetryable 固定 DD-001 的分类：平台侧故障必须可重试，
// 因为重试是数据不丢的唯一途径。
func TestPlatformSideErrorsAreRetryable(t *testing.T) {
	platformSide := []Code{
		CodeQuotaExceeded, CodeSinkUnavailable, CodeBackpressure, CodeGatewayOverloaded,
	}
	for _, code := range platformSide {
		if !New(code, "").Retryable() {
			t.Errorf("%s 属平台侧故障，必须可重试", code)
		}
	}
}

// TestClientSideErrorsAreNotRetryable 固定另一半：客户端错误不可重试，
// 重试只会制造毒消息风暴。
func TestClientSideErrorsAreNotRetryable(t *testing.T) {
	clientSide := []Code{
		CodeTokenInvalid, CodeTenantDisabled, CodeMalformed, CodeBodyTooLarge,
	}
	for _, code := range clientSide {
		if New(code, "").Retryable() {
			t.Errorf("%s 属客户端错误，不应可重试", code)
		}
	}
}

// TestRetryAfterOnlyForRetryable 确认给不可重试码配退避时长会被挡住。
func TestRetryAfterOnlyForRetryable(t *testing.T) {
	for _, code := range Codes() {
		code := code
		e := New(code, "")
		_, want := e.RetryAfter()
		if want != e.Retryable() {
			t.Errorf("%s: 可重试=%v 但 Retry-After 适用性=%v，两者应一致",
				code, e.Retryable(), want)
		}

		if e.Retryable() {
			continue
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s 不可重试，WithRetryAfter 应 panic", code)
				}
			}()
			e.WithRetryAfter(time.Second)
		}()
	}
}

func TestStatusMappingMatchesSpec(t *testing.T) {
	// 取自 DD-001 §3.1 的 IF-1 码表，逐行核对。
	want := map[Code]struct {
		http int
		grpc int
	}{
		CodeTokenInvalid:      {401, GRPCUnauthenticated},
		CodeTenantDisabled:    {403, GRPCPermissionDenied},
		CodeMalformed:         {400, GRPCInvalidArgument},
		CodeBodyTooLarge:      {413, GRPCInvalidArgument},
		CodeQuotaExceeded:     {429, GRPCResourceExhausted},
		CodeSinkUnavailable:   {503, GRPCUnavailable},
		CodeBackpressure:      {429, GRPCResourceExhausted},
		CodeGatewayOverloaded: {503, GRPCUnavailable},
	}
	if len(want) != len(Codes()) {
		t.Fatalf("码表数量不一致：期望 %d 条，Codes() 返回 %d 条", len(want), len(Codes()))
	}
	for code, exp := range want {
		e := New(code, "")
		if got := e.HTTPStatus(); got != exp.http {
			t.Errorf("%s HTTP: 期望 %d，得到 %d", code, exp.http, got)
		}
		if got := e.GRPCCode(); got != exp.grpc {
			t.Errorf("%s gRPC: 期望 %d，得到 %d", code, exp.grpc, got)
		}
	}
}

func TestUnknownCodePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("未定义的码应 panic")
		}
	}()
	New(Code("E99"), "")
}

func TestErrorsIsMatchesByCode(t *testing.T) {
	err := Wrap(CodeSinkUnavailable, "kafka write timeout", errors.New("dial tcp: timeout"))

	if !errors.Is(err, New(CodeSinkUnavailable, "")) {
		t.Error("同码应匹配")
	}
	if errors.Is(err, New(CodeQuotaExceeded, "")) {
		t.Error("异码不应匹配")
	}
	if got := errors.Unwrap(err); got == nil || got.Error() != "dial tcp: timeout" {
		t.Errorf("底层错误应可取出，得到 %v", got)
	}
}

// TestPartialSuccessIsNotAnError 是 IF-1 第二条不可协商规则的结构性断言：
// E9 必须无法表达为 Error，否则单条记录被拒就可能导致整批重发。
func TestPartialSuccessIsNotAnError(t *testing.T) {
	var p any = PartialSuccess{RejectedRecords: 3, Reason: "pii drop"}
	if _, isErr := p.(error); isErr {
		t.Fatal("PartialSuccess 不得实现 error —— E9 必须返回成功")
	}

	if !(PartialSuccess{RejectedRecords: 1}).IsPartial() {
		t.Error("有记录被拒时 IsPartial 应为 true")
	}
	if (PartialSuccess{}).IsPartial() {
		t.Error("零拒绝时 IsPartial 应为 false")
	}
}
