// Package ingest 是摄入网关的协议无关核心：鉴权 → 限流 → 协议归一 → 投递。
//
// # 为什么核心与协议分离
//
// IF-1 要求同时支持 OTLP/HTTP 与 OTLP/gRPC（SD-000 §3.1 F1）。两者的差别只在传输层：
// 路径与方法、压缩协商、错误如何编码（HTTP 状态码 vs gRPC status）。鉴权、限流、身份注入、
// 分区键、投递语义完全一致。把这些放进 Pipeline，协议层只做"字节进、错误出"的适配，
// 于是 M1 只实现 HTTP、M2 补 gRPC 时不会出现两份判定逻辑——两份判定逻辑意味着两份
// IF-1 语义，而 IF-1 的全部价值在于每一跳的语义一致。
//
// M1 只实现 HTTP 适配层（internal/gateway/otlphttp）。gRPC 适配层待补，见下方 Accept 的
// 错误约定：它返回的一律是 *ingesterr.Error，gRPC 侧用 GRPCCode() 取码即可，不需要翻译表。
//
// # 处理顺序不是随意的
//
//  1. 过载准入（E8）：在做任何实际工作之前。过载时最省的做法就是尽早拒绝。
//  2. 鉴权（E1/E2）：必须早于限流。限流桶以租户为键，而租户只能由 Token 反查得到；
//     若顺序颠倒，任何人都能用伪造的身份在网关内存里创建桶，桶表基数就不再受快照约束。
//  3. 线级扫描（E3）：限流需要记录条数，条数只能从 payload 得到。
//  4. 限流（E5/E4）：两个维度一起判，见 ratelimit 包。
//  5. 投递（E6/E7）：错误如实上报，绝不折叠成 2xx。
package ingest

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/mokaz/ops-system/internal/gateway/authn"
	"github.com/mokaz/ops-system/internal/gateway/kafkasink"
	"github.com/mokaz/ops-system/internal/gateway/otlpwire"
	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/internal/gateway/selfmon"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingesterr"
	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/latency"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// DefaultMaxBodyBytes 是单请求解压后体积上限，超限为 E4。
//
// DD-001 的 IF-1 码表定义了 E4 但没有给出阈值。取 4 MiB 与 gRPC 的默认最大消息尺寸一致：
// 同一个客户端 SDK 切换 HTTP / gRPC 两种传输时，不应该在一种传输上成功、另一种上被拒。
// 该缺口已记入 DD-001 §3.1。
const DefaultMaxBodyBytes = 4 << 20

// DefaultMaxInflightBytes 是处理中请求体的总字节上限，超限为 E8。
//
// 它是 DD-001 §3.2 里 memory_limiter 在本服务中的等价物：网关是无状态副本，内存里堆积的
// 请求在 OOM 时全部丢失且客户端已经拿到了连接，因此宁可提前 503 让客户端进本地持久队列。
// 64 MiB 对应 16 个 4 MiB 请求并发在途，远小于单副本内存，留足给 Kafka 生产缓冲。
const DefaultMaxInflightBytes = 64 << 20

// Pipeline 是摄入核心。并发安全。
type Pipeline struct {
	resolver *authn.Resolver
	limiter  *ratelimit.Limiter
	router   *kafkasink.Router
	producer kafkasink.Producer
	metrics  *selfmon.Metrics

	now              func() time.Time
	maxBodyBytes     int64
	maxInflightBytes int64

	inflight atomic.Int64
}

// Option 配置 Pipeline。
type Option func(*Pipeline)

// WithClock 注入时钟，供测试使用。
func WithClock(fn func() time.Time) Option {
	return func(p *Pipeline) { p.now = fn }
}

// WithMaxBodyBytes 覆盖单请求体积上限。
func WithMaxBodyBytes(n int64) Option {
	return func(p *Pipeline) { p.maxBodyBytes = n }
}

// WithMaxInflightBytes 覆盖在途字节上限。
func WithMaxInflightBytes(n int64) Option {
	return func(p *Pipeline) { p.maxInflightBytes = n }
}

// New 组装 Pipeline。
func New(
	resolver *authn.Resolver,
	limiter *ratelimit.Limiter,
	router *kafkasink.Router,
	producer kafkasink.Producer,
	metrics *selfmon.Metrics,
	opts ...Option,
) *Pipeline {
	p := &Pipeline{
		resolver:         resolver,
		limiter:          limiter,
		router:           router,
		producer:         producer,
		metrics:          metrics,
		now:              time.Now,
		maxBodyBytes:     DefaultMaxBodyBytes,
		maxInflightBytes: DefaultMaxInflightBytes,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// MaxBodyBytes 暴露体积上限，供协议层在读取请求体时提前截断。
func (p *Pipeline) MaxBodyBytes() int64 { return p.maxBodyBytes }

// Request 是一次归一化前的摄入请求。
type Request struct {
	Signal telemetry.Signal
	// Token 是客户端凭证明文。Pipeline 只用它做一次摘要比对，不保留、不记日志。
	Token string
	// Body 是解压后的 OTLP protobuf 字节。Pipeline 不改写它（见 pkg/ingestmsg 的说明）。
	Body []byte
	// ReceivedAt 是协议层收到请求的时刻，作为 §8.1 分段计时的原点。
	ReceivedAt time.Time
}

// Result 是一次成功摄入的结果。
type Result struct {
	Binding     identity.Binding
	RecordCount int
	// Partial 对应 E9。M1 恒为零值：脱敏实现在 M2（PLAN §3），此刻没有任何会丢弃单条记录的
	// 环节。链路保留在此，是因为 E9 的响应体编码必须与成功响应共用一条路径，否则 M2 补上
	// 脱敏时会顺手新开一条返回路径，而那条路径很容易被写成"整批失败"。
	Partial ingesterr.PartialSuccess
}

// Accept 执行一次完整摄入。
//
// 返回的错误一律是 *ingesterr.Error，协议层据此取 HTTP 状态码或 gRPC status。
func (p *Pipeline) Accept(ctx context.Context, req Request) (Result, error) {
	sig := req.Signal
	size := int64(len(req.Body))

	if !sig.Valid() {
		return Result{}, p.fail(sig, identity.QuotaKey{},
			ingesterr.New(ingesterr.CodeMalformed, "未知信号类型"))
	}
	if size > p.maxBodyBytes {
		return Result{}, p.fail(sig, identity.QuotaKey{},
			ingesterr.New(ingesterr.CodeBodyTooLarge, "请求体超过上限"))
	}

	// 1. 过载准入。
	inflight := p.inflight.Add(size)
	p.metrics.AddInflight(size)
	defer func() {
		p.inflight.Add(-size)
		p.metrics.AddInflight(-size)
	}()
	if inflight > p.maxInflightBytes {
		return Result{}, p.fail(sig, identity.QuotaKey{},
			ingesterr.New(ingesterr.CodeGatewayOverloaded, "网关在途请求超过上限").
				WithRetryAfter(time.Second))
	}

	// 2. 鉴权与身份注入。
	auth, err := p.resolver.Resolve(req.Token)
	if err != nil {
		return Result{}, p.fail(sig, identity.QuotaKey{}, err)
	}
	key := identity.QuotaKey{TenantID: auth.Binding.TenantID, ClusterID: auth.Binding.ClusterID}

	// 3. 协议归一：线级扫描取齐限流与分区键所需的量。
	summary, err := otlpwire.Scan(req.Body, sig)
	if err != nil {
		return Result{}, p.fail(sig, key,
			ingesterr.Wrap(ingesterr.CodeMalformed, "OTLP 负载无法解析", err))
	}
	p.countSelfReported(summary.SelfReported)

	// 4. 限流。
	if d := p.limiter.Allow(key, size, int64(summary.RecordCount)); !d.OK {
		p.metrics.Throttled(key, d.Dimension)
		if d.NeverFits {
			// 单批就超过桶容量，退避重试永远不会成功。见 ratelimit.Decision.NeverFits。
			return Result{}, p.fail(sig, key, ingesterr.New(ingesterr.CodeBodyTooLarge,
				"单批 "+d.Dimension.String()+" 超过租户突发配额，需拆小批次"))
		}
		return Result{}, p.fail(sig, key, ingesterr.New(ingesterr.CodeQuotaExceeded,
			"租户 "+d.Dimension.String()+" 配额超限").WithRetryAfter(d.RetryAfter))
	}

	// §8.1 第一段的代理观测。放在限流之后：被限流拒绝的请求不代表一次成功的链路，
	// 把它计入延迟分布会让 Collector 攒批段的分位数被重试流量污染。
	p.observeClientLag(sig, req.ReceivedAt, summary.EventUnixNano)

	meta := ingestmsg.Meta{
		TenantID:    auth.Binding.TenantID,
		ProjectID:   auth.Binding.ProjectID,
		ClusterID:   auth.Binding.ClusterID,
		Signal:      sig,
		RecordCount: int64(summary.RecordCount),
		ReceivedAt:  req.ReceivedAt,
		ServiceName: summary.ServiceName,
	}

	// 5. 投递。
	rec, err := p.router.Route(meta, req.Body)
	if err != nil {
		// 该信号没有配置下游（M1 的 metrics 即如此）。报 E6 而不是 E3：问题在平台侧配置，
		// 客户端的请求本身合法，等平台补齐链路后重试就能成功。
		return Result{}, p.fail(sig, key,
			ingesterr.Wrap(ingesterr.CodeSinkUnavailable, "该信号的下游尚未配置", err).
				WithRetryAfter(30*time.Second))
	}

	produceStart := p.now()
	if err := p.producer.Produce(ctx, rec); err != nil {
		p.metrics.ObserveStage(latency.StageKafka, sig, p.now().Sub(produceStart))
		return Result{}, p.fail(sig, key, asIngestErr(err))
	}
	produceEnd := p.now()

	// 分段打点：Kafka 段与网关自身处理段分别出数。
	//
	// 网关段刻意扣掉投递耗时：§8.1 把网关与 Kafka 分成两段预算，若网关段含投递耗时，
	// Kafka 超支会同时把网关段也顶到超支，PLAN §2.4 要定位的"该优化谁"就失效了。
	p.metrics.ObserveStage(latency.StageKafka, sig, produceEnd.Sub(produceStart))
	p.metrics.ObserveStage(latency.StageGateway, sig,
		produceEnd.Sub(req.ReceivedAt)-produceEnd.Sub(produceStart))

	p.metrics.Accepted(sig, auth.Binding.TenantID, int64(summary.RecordCount), size)
	p.metrics.RequestDone(sig, selfmon.OutcomeAccepted)

	return Result{Binding: auth.Binding, RecordCount: summary.RecordCount}, nil
}

// fail 记录指标并返回错误本身，使调用处是单行的 return。
func (p *Pipeline) fail(sig telemetry.Signal, key identity.QuotaKey, err error) error {
	ie := asIngestErr(err)
	p.metrics.Error(string(ie.Code()), key, sig)
	p.metrics.RequestDone(sig, string(ie.Code()))
	return ie
}

// asIngestErr 保证进入响应路径的错误一定带 IF-1 码。
//
// 未分类的错误归到 E8（503，可重试）而不是 E3（400，不可重试）：把平台内部的未知故障
// 报成客户端错误会让客户端丢弃本可成功的数据，这是 IF-1 里唯一不可挽回的错误方向。
func asIngestErr(err error) *ingesterr.Error {
	var ie *ingesterr.Error
	if errors.As(err, &ie) {
		return ie
	}
	return ingesterr.Wrap(ingesterr.CodeGatewayOverloaded, "未分类的内部错误", err).
		WithRetryAfter(time.Second)
}

// observeClientLag 用客户端事件时间戳代理观测 §8.1 的 Collector 攒批段。
//
// 时间戳来自客户集群，受其时钟偏移影响。负值（客户端时钟超前）不记入直方图而单独计数：
// 把它压成 0 会让该段分位数向下偏，掩盖真实攒批时长，而这段预算有 1.0s、是五段里最大的
// 一段，偏差会直接影响 M1 的预算校准结论。
func (p *Pipeline) observeClientLag(sig telemetry.Signal, receivedAt time.Time, eventNanos uint64) {
	if eventNanos == 0 {
		return
	}
	lag := receivedAt.Sub(time.Unix(0, int64(eventNanos)))
	if lag < 0 {
		p.metrics.ClientClockSkew(sig)
		return
	}
	p.metrics.ObserveStage(latency.StageCollectorBatch, sig, lag)
}

func (p *Pipeline) countSelfReported(s otlpwire.SelfReported) {
	if s.TenantID {
		p.metrics.IdentityOverride("tenant_id")
	}
	if s.ProjectID {
		p.metrics.IdentityOverride("project_id")
	}
	if s.ClusterID {
		p.metrics.IdentityOverride("cluster_id")
	}
}
