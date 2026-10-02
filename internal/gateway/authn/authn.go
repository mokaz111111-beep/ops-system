// Package authn 实现摄入网关的 Token 鉴权与 fail-static 语义（DD-001 §3.1、DD-006 §3.6）。
//
// # 本地快照是权威读取源
//
// 鉴权只读本地快照，绝不在请求路径上同步查询控制面。任何同步依赖都会把控制面的可用性
// 乘进数据面——控制面 99.99% 会让数据面从 99.99% 掉到 99.98%，而这正是分层原则 4
// 要避免的（DD-006 §3.6.1）。
//
// # 关于 Class-S TTL 的一处实现期修正
//
// DD-006 §3.6.3 原本规定：安全类配置（Token 吊销）快照超过 TTL 后，"对新出现的 Token
// 拒绝，对快照中已知且未被吊销的 Token 继续放行"。
//
// 实现时发现这条规则是空转的：快照里没有的 Token 在 TTL 之内也一样会被拒（它本就不是
// 有效凭证），所以 TTL 并没有改变任何一条判定路径。
//
// 真正的风险是另一个方向——控制面宕机期间被吊销的 Token，快照里仍记为有效。而这件事
// 无法靠 TTL 解决：TTL 到期时我们依然无法区分"仍然有效"与"一小时前已被吊销"，能选的
// 只有继续服务（已吊销凭证长期可用）或停止服务（全站拒收）。
//
// 本实现的选择是：**继续服务，但把暴露变成可观测的**。超过 TTL 后鉴权结果带
// Degraded 标记并计数，驱动 X-001 的"快照陈旧"告警，让运维明确知道此刻吊销未被执行。
// 这是 fail-static 与有界吊销延迟之间不可兼得时的取舍，而不是把矛盾藏起来。
package authn

import (
	"sync/atomic"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingesterr"
)

// DefaultClassSTTL 是安全类配置的陈旧阈值。
//
// 取值依据尚不充分（DD-006 OQ-3 / X-001 OQ-1 / SD-000 Q-13）：它是"控制面最长可容忍
// 宕机时长"与"被吊销凭证最长可用时长"之间的权衡。X-001 从控制面 RTO ≤ 1 小时推出建议
// 4 小时，安全侧上限待定。此处取 4 小时作为可配置默认值。
const DefaultClassSTTL = 4 * time.Hour

// Snapshot 是一次控制面配置同步的全量结果。
//
// 刻意是全量而非增量：增量下发在丢事件时会产生静默的状态分歧，而配置规模
// （租户数 × 配置项）完全放得下全量（DD-006 §3.6.1）。
type Snapshot struct {
	// Version 是控制面的全局单调版本号，上报给控制面用于计算"落后实例数"。
	Version uint64
	// FetchedAt 是本快照的生成时刻，用于判定陈旧度。
	FetchedAt time.Time
	// Bindings 以 Token 摘要为键。平台不持有 Token 明文。
	Bindings map[identity.TokenHash]identity.Binding
}

// Result 是一次鉴权的结果。
type Result struct {
	Binding identity.Binding
	// Degraded 为 true 表示本次判定使用了超过 Class-S TTL 的陈旧快照，
	// 此刻的 Token 吊销可能未被执行。数据仍然接收（fail-static），但暴露必须可见。
	Degraded bool
	// SnapshotAge 是本次判定所用快照的年龄，进监控。
	SnapshotAge time.Duration
}

// Stats 是鉴权器的累计计数，汇总进 X-001 §2.6.1 的控制面 SLI。
type Stats struct {
	SnapshotVersion    uint64
	SnapshotAgeSeconds int64
	Stale              bool
	// DegradedDecisions 是使用陈旧快照做出的判定次数。
	DegradedDecisions uint64
	// RejectedUnknown / RejectedRevoked / RejectedDisabled 分别对应 E1 / E1 / E2。
	RejectedUnknown  uint64
	RejectedRevoked  uint64
	RejectedDisabled uint64
}

// Resolver 把 Token 解析为租户身份。并发安全。
type Resolver struct {
	// snap 以原子指针保存，使鉴权热路径无锁：读一次指针即可，
	// 快照替换是整体替换而非逐条修改。
	snap atomic.Pointer[Snapshot]

	classSTTL time.Duration
	now       func() time.Time

	degraded         atomic.Uint64
	rejectedUnknown  atomic.Uint64
	rejectedRevoked  atomic.Uint64
	rejectedDisabled atomic.Uint64
}

// Option 配置 Resolver。
type Option func(*Resolver)

// WithClassSTTL 覆盖安全类陈旧阈值。
func WithClassSTTL(d time.Duration) Option {
	return func(r *Resolver) { r.classSTTL = d }
}

// WithClock 注入时钟，供测试使用。
func WithClock(fn func() time.Time) Option {
	return func(r *Resolver) { r.now = fn }
}

// NewResolver 创建鉴权器。初始快照可为 nil（尚未完成首次同步）。
func NewResolver(initial *Snapshot, opts ...Option) *Resolver {
	r := &Resolver{classSTTL: DefaultClassSTTL, now: time.Now}
	for _, o := range opts {
		o(r)
	}
	if initial != nil {
		r.snap.Store(initial)
	}
	return r
}

// Replace 原子替换快照。
//
// 刻意不做"版本号必须递增"的校验并拒绝旧版本：那会让一次控制面回滚变成数据面拒绝
// 接受回滚后的配置。版本号用于可观测性，不用于准入。
func (r *Resolver) Replace(s *Snapshot) {
	if s == nil {
		return
	}
	r.snap.Store(s)
}

// Resolve 用明文 Token 解析身份。
//
// 错误一律为 *ingesterr.Error，语义遵循 IF-1 码表。
func (r *Resolver) Resolve(token string) (Result, error) {
	hash, err := identity.HashToken(token)
	if err != nil {
		r.rejectedUnknown.Add(1)
		return Result{}, ingesterr.New(ingesterr.CodeTokenInvalid, "未携带 Token")
	}

	snap := r.snap.Load()
	if snap == nil {
		// 首次同步尚未完成。这是 E8（网关自身尚不可服务）而非 E1——
		// 把它报成 401 会让客户端判定为配置错误并丢弃数据，而实际上重试就能成功。
		r.rejectedUnknown.Add(1)
		return Result{}, ingesterr.New(ingesterr.CodeGatewayOverloaded,
			"网关尚未完成首次配置同步").WithRetryAfter(5 * time.Second)
	}

	age := r.now().Sub(snap.FetchedAt)
	if age < 0 {
		age = 0
	}
	degraded := age > r.classSTTL

	binding, ok := snap.Bindings[hash]
	if !ok {
		r.rejectedUnknown.Add(1)
		if degraded {
			r.degraded.Add(1)
		}
		return Result{}, ingesterr.New(ingesterr.CodeTokenInvalid, "Token 未知")
	}

	// 摘要已作为 map 键命中，此处用常量时间比较复核 Binding 自带的摘要，
	// 防止快照构造错误导致身份串用。
	if !identity.EqualHash(binding.TokenHash, hash) {
		r.rejectedUnknown.Add(1)
		return Result{}, ingesterr.New(ingesterr.CodeTokenInvalid, "Token 绑定不一致")
	}

	if degraded {
		r.degraded.Add(1)
	}

	if binding.Revoked {
		r.rejectedRevoked.Add(1)
		return Result{}, ingesterr.New(ingesterr.CodeTokenInvalid, "Token 已吊销")
	}
	if !binding.TenantStatus.Servable() || !binding.ClusterStatus.Servable() {
		r.rejectedDisabled.Add(1)
		return Result{}, ingesterr.New(ingesterr.CodeTenantDisabled,
			"租户状态 "+binding.TenantStatus.String()+"，集群状态 "+binding.ClusterStatus.String())
	}
	if !binding.Valid() {
		// 配置本身不完整。报 E8 而非 E1：问题在平台侧，客户重试在配置修好后能成功。
		return Result{}, ingesterr.New(ingesterr.CodeGatewayOverloaded,
			"租户绑定配置不完整").WithRetryAfter(30 * time.Second)
	}

	return Result{Binding: binding, Degraded: degraded, SnapshotAge: age}, nil
}

// Stats 返回当前计数，供指标导出。
func (r *Resolver) Stats() Stats {
	s := Stats{
		DegradedDecisions: r.degraded.Load(),
		RejectedUnknown:   r.rejectedUnknown.Load(),
		RejectedRevoked:   r.rejectedRevoked.Load(),
		RejectedDisabled:  r.rejectedDisabled.Load(),
	}
	if snap := r.snap.Load(); snap != nil {
		age := r.now().Sub(snap.FetchedAt)
		if age < 0 {
			age = 0
		}
		s.SnapshotVersion = snap.Version
		s.SnapshotAgeSeconds = int64(age.Seconds())
		s.Stale = age > r.classSTTL
	}
	return s
}
