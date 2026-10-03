// Package otlphttp 是 OTLP/HTTP 的协议适配层：把 HTTP 请求喂给 ingest.Pipeline，
// 再把 IF-1 错误码编成符合 OTLP 规范的响应。
//
// # 只做适配，不做判定
//
// 本包不包含任何准入判定——鉴权、限流、体积上限全在 Pipeline 里。它只负责四件 HTTP 才有的
// 事：路由与方法校验、Content-Type 协商、gzip 解压、以及把错误编成 OTLP 规定的响应形态。
// gRPC 适配层（M2）将以同样的方式复用 Pipeline。
//
// # 响应体形态
//
// OTLP/HTTP 规定：成功返回 200 + Export*ServiceResponse（protobuf），失败返回对应状态码 +
// google.rpc.Status（protobuf）。两者都手写编码，不引入 OTLP proto 依赖——需要编的只有
// 两个消息、四个字段，而引入依赖会把 otlpwire 刻意避开的那套反序列化拉回进程里。
package otlphttp

import (
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mokaz/ops-system/internal/gateway/ingest"
	"github.com/mokaz/ops-system/pkg/ingesterr"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// OTLP/HTTP 的标准路径（OTLP 规范 1.x）。
const (
	PathLogs    = "/v1/logs"
	PathTraces  = "/v1/traces"
	PathMetrics = "/v1/metrics"
)

// contentTypeProtobuf 是本网关唯一接受的请求编码。
//
// OTLP/HTTP 同时允许 JSON，M1 不支持：JSON 负载无法走 otlpwire 的线级快速路径，支持它
// 等于在热路径上引入第二套解析实现，而 OTel Collector 与各语言 SDK 的默认编码都是 protobuf。
// 拒绝时用 E3（400 不可重试）而不是 415：IF-1 码表没有"编码不支持"这一码，与其自造第九码，
// 不如落到语义最近的 E3——两者对客户端的要求相同（不要重试，改配置）。该缺口已记入 DD-001。
const contentTypeProtobuf = "application/x-protobuf"

// 凭证头。
//
// IF-1 只写了"per-tenant token"，没有规定头名（DD-001 §3.1）。这里同时接受标准的
// Authorization: Bearer 与一个平台专用头：前者是 OTel 各语言 exporter 的默认可配项，
// 后者便于客户在已有 Authorization 用于其他网关的环境里接入。
const (
	headerAuthorization = "Authorization"
	headerIngestToken   = "X-Ops-Ingest-Token"
	bearerPrefix        = "Bearer "
)

// defaultRetryAfter 是可重试错误未指定退避时长时的建议值。
const defaultRetryAfter = time.Second

// Handler 适配一个信号的 OTLP/HTTP 端点。
type Handler struct {
	pipeline *ingest.Pipeline
	signal   telemetry.Signal
	now      func() time.Time
}

// Option 配置 Handler。
type Option func(*Handler)

// WithClock 注入时钟，供测试使用。
func WithClock(fn func() time.Time) Option {
	return func(h *Handler) { h.now = fn }
}

// NewHandler 创建单个信号的处理器。
func NewHandler(p *ingest.Pipeline, sig telemetry.Signal, opts ...Option) *Handler {
	h := &Handler{pipeline: p, signal: sig, now: time.Now}
	for _, o := range opts {
		o(h)
	}
	return h
}

// NewMux 按信号注册端点。
//
// 只注册给定信号：M1 不接 metrics（metrics-ingest 属 Track C，尚未存在），此时
// /v1/metrics 返回 404 而不是一个可重试的 5xx。这是刻意的——对一个本版本根本不提供的
// 端点返回 503 会让客户端把数据堆进持久队列等一个永远不会到来的恢复。
func NewMux(p *ingest.Pipeline, signals []telemetry.Signal, opts ...Option) *http.ServeMux {
	mux := http.NewServeMux()
	for _, sig := range signals {
		var path string
		switch sig {
		case telemetry.SignalLogs:
			path = PathLogs
		case telemetry.SignalTraces:
			path = PathTraces
		case telemetry.SignalMetrics:
			path = PathMetrics
		default:
			continue
		}
		mux.Handle(path, NewHandler(p, sig, opts...))
	}
	return mux
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	receivedAt := h.now()

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		// 405 不在 IF-1 码表内，它是 HTTP 层的协议错误而非摄入故障，响应体仍按 OTLP 规范
		// 用 google.rpc.Status 编码，以免客户端拿到一个无法解析的响应体。
		writeStatus(w, http.StatusMethodNotAllowed, ingesterr.GRPCInvalidArgument,
			"仅支持 POST")
		return
	}
	if ct := r.Header.Get("Content-Type"); !acceptableContentType(ct) {
		h.writeErr(w, ingesterr.New(ingesterr.CodeMalformed,
			"Content-Type 必须为 "+contentTypeProtobuf+"，收到 "+quote(ct)))
		return
	}

	body, err := h.readBody(r)
	if err != nil {
		h.writeErr(w, err)
		return
	}

	res, err := h.pipeline.Accept(r.Context(), ingest.Request{
		Signal:     h.signal,
		Token:      token(r),
		Body:       body,
		ReceivedAt: receivedAt,
	})
	if err != nil {
		h.writeErr(w, err)
		return
	}

	w.Header().Set("Content-Type", contentTypeProtobuf)
	w.WriteHeader(http.StatusOK)
	// 全批接受时响应体是一个空的 Export*ServiceResponse（零字节即合法编码）。
	_, _ = w.Write(encodeExportResponse(res.Partial))
}

// readBody 读取并按需解压请求体，超限返回 E4。
//
// 解压后的体积上限与未压缩请求一致：否则一个 10 KB 的 gzip 炸弹能换来几百 MB 的内存占用，
// 而网关的在途字节保护（E8）要等 Body 读完才生效。
func (h *Handler) readBody(r *http.Request) ([]byte, error) {
	limit := h.pipeline.MaxBodyBytes()

	var src io.Reader = r.Body
	switch enc := strings.TrimSpace(r.Header.Get("Content-Encoding")); enc {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(io.LimitReader(r.Body, limit+1))
		if err != nil {
			return nil, ingesterr.Wrap(ingesterr.CodeMalformed, "gzip 解压失败", err)
		}
		defer zr.Close()
		src = zr
	default:
		return nil, ingesterr.New(ingesterr.CodeMalformed, "不支持的 Content-Encoding "+quote(enc))
	}

	// 多读 1 字节用于区分"正好等于上限"与"超过上限"。
	body, err := io.ReadAll(io.LimitReader(src, limit+1))
	if err != nil {
		// 读取中断（客户端断连、gzip 流损坏）。客户端断连时响应已无人接收，
		// 仍按 E3 处理以保证计数口径统一。
		return nil, ingesterr.Wrap(ingesterr.CodeMalformed, "读取请求体失败", err)
	}
	if int64(len(body)) > limit {
		return nil, ingesterr.New(ingesterr.CodeBodyTooLarge,
			"请求体超过 "+strconv.FormatInt(limit, 10)+" 字节上限")
	}
	return body, nil
}

func (h *Handler) writeErr(w http.ResponseWriter, err error) {
	var ie *ingesterr.Error
	if !errors.As(err, &ie) {
		ie = ingesterr.Wrap(ingesterr.CodeGatewayOverloaded, "未分类的内部错误", err)
	}

	if d, ok := ie.RetryAfter(); ok {
		if d <= 0 {
			d = defaultRetryAfter
		}
		// Retry-After 的 delta-seconds 形态，向上取整到整秒（0 等于邀请客户端忙等）。
		secs := int64(d / time.Second)
		if d%time.Second != 0 {
			secs++
		}
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
	}
	writeStatus(w, ie.HTTPStatus(), ie.GRPCCode(), ie.Error())
}

func writeStatus(w http.ResponseWriter, httpStatus, grpcCode int, msg string) {
	w.Header().Set("Content-Type", contentTypeProtobuf)
	w.WriteHeader(httpStatus)
	_, _ = w.Write(encodeStatus(grpcCode, msg))
}

// acceptableContentType 允许带参数的 Content-Type（如 ...;charset=utf-8）。
func acceptableContentType(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.EqualFold(strings.TrimSpace(ct), contentTypeProtobuf)
}

// token 取出客户端凭证。两个来源都为空时返回空串，由 authn 报 E1。
func token(r *http.Request) string {
	if v := r.Header.Get(headerAuthorization); v != "" {
		if len(v) > len(bearerPrefix) && strings.EqualFold(v[:len(bearerPrefix)], bearerPrefix) {
			return strings.TrimSpace(v[len(bearerPrefix):])
		}
		// 不带 Bearer 前缀的裸 Token 也接受：真实接入里这类配置错误极常见，
		// 而把它判成"未携带 Token"会让排查方向完全跑偏（客户会以为凭证没下发）。
		return strings.TrimSpace(v)
	}
	return strings.TrimSpace(r.Header.Get(headerIngestToken))
}

func quote(s string) string {
	if s == "" {
		return `""`
	}
	return strconv.Quote(s)
}
