// Package ingesterr 实现 IF-1 的摄入侧错误码语义（DD-001 §3.1）。
//
// 整条兜底链路（客户侧持久队列 → 重发）只有在每一跳都如实返回可重试错误时才成立。
// 任何一跳"吞掉错误返回成功"，客户侧的持久队列就永远不会被触发，兜底退化为静默丢数。
//
// 本包用类型结构保证 IF-1 的两条不可协商规则，而不是靠调用方自觉：
//
//  1. E6/E7/E8 绝不折叠成 2xx —— HTTP 与 gRPC 状态码由码表派生，调用方无法覆写；
//     TestRetryableNeverMapsTo2xx 把这条钉成编译期之外的回归测试。
//  2. E9 不是错误 —— 它是 PartialSuccess 而非 Error，因此在类型上就无法让单条记录
//     被拒导致整批失败。
package ingesterr

import (
	"errors"
	"fmt"
	"time"
)

// Code 是 IF-1 码表的编号。
type Code string

const (
	// CodeTokenInvalid E1：Token 缺失、无效或已吊销。接入配置错误，重试无意义。
	CodeTokenInvalid Code = "E1"
	// CodeTenantDisabled E2：租户或集群被停用（欠费、退租、熔断）。
	CodeTenantDisabled Code = "E2"
	// CodeMalformed E3：请求体无法解析或不符合 OTLP / remote-write 规范。
	CodeMalformed Code = "E3"
	// CodeBodyTooLarge E4：单请求体积超过上限。客户端拆小 batch 后再发。
	CodeBodyTooLarge Code = "E4"
	// CodeQuotaExceeded E5：租户限流超配额（bytes/s 或 events/s）。
	CodeQuotaExceeded Code = "E5"
	// CodeSinkUnavailable E6：下游 Kafka 不可用或写入超时。
	CodeSinkUnavailable Code = "E6"
	// CodeBackpressure E7：下游 sending_queue 满。
	CodeBackpressure Code = "E7"
	// CodeGatewayOverloaded E8：网关自身过载（内存限流触发、副本重启中）。
	CodeGatewayOverloaded Code = "E8"
)

// gRPC 状态码的数值。此处内联而不引入 grpc 依赖：这些值是 gRPC 线上协议的一部分，
// 不会变动，而摄入网关在 M1 阶段没有其他理由引入整个 grpc 运行时。
const (
	GRPCOK                = 0
	GRPCInvalidArgument   = 3
	GRPCPermissionDenied  = 7
	GRPCResourceExhausted = 8
	GRPCUnavailable       = 14
	GRPCUnauthenticated   = 16
)

// spec 是 IF-1 码表的唯一事实来源。字段不导出，调用方无法构造或覆写一条语义。
type spec struct {
	httpStatus int
	grpcCode   int
	retryable  bool
	// retryAfter 表示该码是否应携带 Retry-After。只有可重试的码才有意义。
	retryAfter bool
	summary    string
}

var table = map[Code]spec{
	CodeTokenInvalid: {
		httpStatus: 401, grpcCode: GRPCUnauthenticated,
		retryable: false, retryAfter: false,
		summary: "Token 缺失、无效或已吊销",
	},
	CodeTenantDisabled: {
		httpStatus: 403, grpcCode: GRPCPermissionDenied,
		retryable: false, retryAfter: false,
		summary: "租户或集群已停用",
	},
	CodeMalformed: {
		httpStatus: 400, grpcCode: GRPCInvalidArgument,
		retryable: false, retryAfter: false,
		summary: "请求体无法解析或不符合规范",
	},
	CodeBodyTooLarge: {
		httpStatus: 413, grpcCode: GRPCInvalidArgument,
		retryable: false, retryAfter: false,
		summary: "单请求体积超过上限",
	},
	CodeQuotaExceeded: {
		httpStatus: 429, grpcCode: GRPCResourceExhausted,
		retryable: true, retryAfter: true,
		summary: "租户限流超配额",
	},
	CodeSinkUnavailable: {
		httpStatus: 503, grpcCode: GRPCUnavailable,
		retryable: true, retryAfter: true,
		summary: "下游缓冲层不可用",
	},
	CodeBackpressure: {
		httpStatus: 429, grpcCode: GRPCResourceExhausted,
		retryable: true, retryAfter: true,
		summary: "下游背压",
	},
	CodeGatewayOverloaded: {
		httpStatus: 503, grpcCode: GRPCUnavailable,
		retryable: true, retryAfter: true,
		summary: "网关过载",
	},
}

// Codes 按 E1~E8 顺序返回全部码，供测试与文档生成遍历。
func Codes() []Code {
	return []Code{
		CodeTokenInvalid, CodeTenantDisabled, CodeMalformed, CodeBodyTooLarge,
		CodeQuotaExceeded, CodeSinkUnavailable, CodeBackpressure, CodeGatewayOverloaded,
	}
}

// Error 是一个 IF-1 摄入错误。
//
// 它故意不暴露修改状态码的途径：HTTPStatus / GRPCCode / Retryable 全部从码表派生。
// 这样"把 503 改成 200 让客户端别重试了"这类看似善意的改动无处落脚。
type Error struct {
	code Code
	// retryAfter 仅对可重试码有效，零值表示由调用方的默认退避决定。
	retryAfter time.Duration
	cause      error
	detail     string
}

// New 构造一个 IF-1 错误。code 不在码表中会 panic——这是编程错误，不是运行时状况。
func New(code Code, detail string) *Error {
	if _, ok := table[code]; !ok {
		panic(fmt.Sprintf("ingesterr: 未定义的码 %q", code))
	}
	return &Error{code: code, detail: detail}
}

// Wrap 在 New 的基础上保留底层错误，便于日志排查。
func Wrap(code Code, detail string, cause error) *Error {
	e := New(code, detail)
	e.cause = cause
	return e
}

// WithRetryAfter 设置 Retry-After。对不可重试的码调用会 panic：
// 给 401 配退避时间是语义矛盾，会误导客户端进入无意义的重试循环。
func (e *Error) WithRetryAfter(d time.Duration) *Error {
	if !table[e.code].retryAfter {
		panic(fmt.Sprintf("ingesterr: %s 不可重试，不应携带 Retry-After", e.code))
	}
	e.retryAfter = d
	return e
}

func (e *Error) Code() Code      { return e.code }
func (e *Error) HTTPStatus() int { return table[e.code].httpStatus }
func (e *Error) GRPCCode() int   { return table[e.code].grpcCode }
func (e *Error) Retryable() bool { return table[e.code].retryable }
func (e *Error) Unwrap() error   { return e.cause }
func (e *Error) Detail() string  { return e.detail }

// RetryAfter 返回建议退避时长，以及该码是否应携带该头。
func (e *Error) RetryAfter() (time.Duration, bool) {
	if !table[e.code].retryAfter {
		return 0, false
	}
	return e.retryAfter, true
}

func (e *Error) Error() string {
	s := fmt.Sprintf("%s: %s", e.code, table[e.code].summary)
	if e.detail != "" {
		s += " (" + e.detail + ")"
	}
	if e.cause != nil {
		s += ": " + e.cause.Error()
	}
	return s
}

// Is 让调用方可以用 errors.Is(err, ingesterr.New(code, "")) 做码级比较。
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return e.code == t.code
}

// PartialSuccess 对应 E9：批内部分记录被脱敏规则 drop 或因字段非法被拒。
//
// 它刻意不是 Error。OTLP 的 partial success 语义要求整批仍返回成功，否则脱敏规则
// 一旦命中高频字段，就会把客户端拖进"整批重发 → 再次部分被拒"的循环。把 E9 建成
// 独立类型，使"让单条记录失败整批"在类型上就无法表达。
type PartialSuccess struct {
	// RejectedRecords 被拒记录数，0 表示全批接受。
	RejectedRecords int64
	// Reason 面向客户端的简短说明，进入 OTLP partial success 响应体。
	Reason string
}

// IsPartial 报告是否确有记录被拒。为 false 时调用方不应在响应中带 partial success 体。
func (p PartialSuccess) IsPartial() bool { return p.RejectedRecords > 0 }
