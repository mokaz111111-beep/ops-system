// Package ratelimit 实现摄入网关的租户级限流桶（DD-001 §3.1、DD-006 §3.3 维度 1）。
//
// # 桶的粒度是 Cluster，不是 Organization
//
// DD-001 §3.1 的措辞是"每租户独立限流桶"，DD-006 §3.3 的配额矩阵维度 1 写的是
// "bytes/s、EPS（按 Cluster）"。两处不一致，本实现按 DD-006：配额值的定义权归 DD-006，
// 摄入网关只负责执行（DD-001 §0 自己也这么划分范围）。更重要的是 DD-006 §3.1 把 Cluster
// 定义为"故障与限流边界"——某集群异常刷量只应熔断该集群，不应波及同租户的其他集群。
// 桶键用 identity.QuotaKey（tenant + cluster），跨租户同名 cluster_id 不会相撞。
// DD-001 §3.1 的措辞已按此修正。
//
// # 两个维度必须一起成功或一起不扣
//
// bytes/s 与 events/s 是两个独立的桶，但一次请求要么同时通过、要么一个令牌都不扣。
// 若按"逐个维度扣减"实现，一个因 EPS 超限被拒的请求仍会扣掉字节令牌，等于对被拒流量
// 重复计费一次限额，实际吞吐上限会低于配额值，且低多少取决于客户端的重试节奏——这种
// 限流器无法标定。TestNoPartialDeduction 把这条钉住。
//
// # 配额缺失时放行
//
// 快照里没有某个 key、或速率配置为 0，一律视为不限流。这是 fail-static 的直接推论
// （SD-000 §5.1 分层原则 4）：控制面还没下发配额不等于配额为零，把它读成零会让一次
// 配置同步故障变成全站 429。
package ratelimit

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
)

// Dimension 是限流维度。取值直接作为指标标签。
type Dimension uint8

const (
	// DimNone 表示未因限流被拒。
	DimNone Dimension = iota
	// DimBytes 是 bytes/s 维度。
	DimBytes
	// DimEvents 是 events/s（EPS）维度。
	DimEvents
)

func (d Dimension) String() string {
	switch d {
	case DimBytes:
		return "bytes"
	case DimEvents:
		return "events"
	default:
		return "none"
	}
}

// DefaultIdleTTL 是空闲桶的回收阈值。
//
// 取 10 分钟而不是更短：回收一个未满的桶等于给该 key 发一次免费突发（见 Sweep），
// 因此回收只对"真的不再发数据"的 key 有意义，而判定"真的不再发"需要一个明显大于
// 客户端重试周期的窗口。
const DefaultIdleTTL = 10 * time.Minute

// MaxRetryAfter 是 Retry-After 的上限。
//
// 更长的建议值没有意义：客户侧持久队列按 ≥2 小时设计（DD-001 §3.3），而让客户端一次
// 等上几分钟只会把重试流量堆成尖峰。
const MaxRetryAfter = 30 * time.Second

// shardCount 是桶表的分片数。取 64：在 16 核规模上足以把锁竞争降到可忽略，
// 同时让单分片的 map 仍然足够大，不至于每个分片都为几个 key 维护一张哈希表。
const shardCount = 64

// Limits 是一个 key 的双维度配额。速率 ≤ 0 表示该维度不限流。
type Limits struct {
	BytesPerSec  float64
	BytesBurst   float64
	EventsPerSec float64
	EventsBurst  float64
}

// Normalize 补齐未设置的突发量。
//
// 默认突发 = 1 秒速率。再小会让单个正常批次打不进桶（OTel Collector 默认攒批 1 秒，
// 一个批次恰好约等于 1 秒的量），那时客户端无论怎么退避都会被永久拒绝——
// Allow 会把这种情形报成 NeverFits 以免客户端陷入无意义的重试循环。
func (l Limits) Normalize() Limits {
	if l.BytesPerSec > 0 && l.BytesBurst <= 0 {
		l.BytesBurst = l.BytesPerSec
	}
	if l.EventsPerSec > 0 && l.EventsBurst <= 0 {
		l.EventsBurst = l.EventsPerSec
	}
	return l
}

// Unlimited 报告两个维度是否都不限流。
func (l Limits) Unlimited() bool { return l.BytesPerSec <= 0 && l.EventsPerSec <= 0 }

// Snapshot 是一次配额下发的全量结果，与 authn.Snapshot 同构（IF-9，fail-static）。
type Snapshot struct {
	Version   uint64
	FetchedAt time.Time
	// Default 适用于 PerKey 未覆盖的 key。
	Default Limits
	PerKey  map[identity.QuotaKey]Limits
}

func (s *Snapshot) limitsFor(key identity.QuotaKey) Limits {
	if s == nil {
		return Limits{}
	}
	if l, ok := s.PerKey[key]; ok {
		return l.Normalize()
	}
	return s.Default.Normalize()
}

// Decision 是一次限流判定的结果。
type Decision struct {
	OK bool
	// Dimension 是先打满的维度，仅 OK 为 false 时有意义。
	Dimension Dimension
	// RetryAfter 是建议退避时长，来自令牌缺口除以速率。
	RetryAfter time.Duration
	// NeverFits 为 true 表示该请求即使桶全满也装不下（请求量 > 突发量）。
	//
	// 这种情形下退避重试永远不会成功，调用方应返回 E4（413，拆小 batch）而不是
	// E5（429，退避重试）——否则客户端会带着一个注定失败的批次无限重试，
	// 而本该被接收的后续数据在它的持久队列里排队到过期。
	NeverFits bool
}

// Stats 是限流器的累计计数，进 X-001 §2.6.1 的租户配额水位面板。
type Stats struct {
	SnapshotVersion uint64
	// Allowed / RejectedBytes / RejectedEvents 为自启动以来的累计次数。
	Allowed        uint64
	RejectedBytes  uint64
	RejectedEvents uint64
	// Buckets 是当前存活桶数，EvictedBuckets 是累计回收数。
	// 两者一起看才能判断内存是否在增长，只看其一会误判。
	Buckets        int64
	EvictedBuckets uint64
}

type tokens struct {
	avail float64
	last  time.Time
}

// refill 把令牌补到 now，返回补充后的可用量。rate ≤ 0 时不参与判定。
func (t *tokens) refill(now time.Time, rate, burst float64) {
	if t.last.IsZero() {
		t.avail = burst
		t.last = now
		return
	}
	elapsed := now.Sub(t.last).Seconds()
	if elapsed <= 0 {
		// 时钟回拨：不补也不罚。把 last 前移会让回拨期间的流量不受限，
		// 不前移则回拨结束后一次补满，两者都比按负数扣减安全。
		return
	}
	t.avail = math.Min(burst, t.avail+elapsed*rate)
	t.last = now
}

type bucket struct {
	bytes    tokens
	events   tokens
	lastSeen time.Time
}

type shard struct {
	mu sync.Mutex
	m  map[identity.QuotaKey]*bucket
}

// Limiter 是双维度令牌桶。所有方法并发安全。
type Limiter struct {
	snap    atomic.Pointer[Snapshot]
	now     func() time.Time
	idleTTL time.Duration

	shards [shardCount]shard

	allowed        atomic.Uint64
	rejectedBytes  atomic.Uint64
	rejectedEvents atomic.Uint64
	buckets        atomic.Int64
	evicted        atomic.Uint64
}

// Option 配置 Limiter。
type Option func(*Limiter)

// WithClock 注入时钟，供测试使用。
func WithClock(fn func() time.Time) Option {
	return func(l *Limiter) { l.now = fn }
}

// WithIdleTTL 覆盖空闲桶回收阈值。
func WithIdleTTL(d time.Duration) Option {
	return func(l *Limiter) { l.idleTTL = d }
}

// New 创建限流器。initial 可为 nil（尚未完成首次配置同步，此时不限流）。
func New(initial *Snapshot, opts ...Option) *Limiter {
	l := &Limiter{now: time.Now, idleTTL: DefaultIdleTTL}
	for _, o := range opts {
		o(l)
	}
	for i := range l.shards {
		l.shards[i].m = make(map[identity.QuotaKey]*bucket)
	}
	if initial != nil {
		l.snap.Store(initial)
	}
	return l
}

// Replace 原子替换配额快照。
//
// 与 authn.Replace 一致地不校验版本递增：控制面回滚配额时数据面必须接受回滚后的值。
// 已存在的桶保留其当前令牌量——配额调整不应顺带清空或灌满桶，那会让一次调参变成一次
// 突发放行或一次瞬时拒绝。
func (l *Limiter) Replace(s *Snapshot) {
	if s == nil {
		return
	}
	l.snap.Store(s)
}

// Allow 判定一次请求是否放行，并在放行时扣减两个维度的令牌。
//
// bytes 为请求的解压后字节数，events 为记录条数。两者都应是实际将投递的量——
// 在扫描出记录条数之后调用，而不是用请求头里的估算值。
func (l *Limiter) Allow(key identity.QuotaKey, bytes, events int64) Decision {
	lim := l.snap.Load().limitsFor(key)
	if lim.Unlimited() {
		l.allowed.Add(1)
		return Decision{OK: true}
	}

	now := l.now()
	needBytes := float64(bytes)
	needEvents := float64(events)

	// 请求本身就大于桶容量：这不是"现在没令牌"，而是"永远不可能有"。
	// 在加锁之前判掉，避免为注定失败的请求创建桶。
	if lim.BytesPerSec > 0 && needBytes > lim.BytesBurst {
		l.rejectedBytes.Add(1)
		return Decision{Dimension: DimBytes, NeverFits: true}
	}
	if lim.EventsPerSec > 0 && needEvents > lim.EventsBurst {
		l.rejectedEvents.Add(1)
		return Decision{Dimension: DimEvents, NeverFits: true}
	}

	sh := &l.shards[shardIndex(key)]
	sh.mu.Lock()
	b, ok := sh.m[key]
	if !ok {
		b = &bucket{}
		sh.m[key] = b
		l.buckets.Add(1)
	}
	b.lastSeen = now

	// 先各自补齐，再统一判定：两个维度的缺口都要算出来，才能把更紧的那个作为
	// Retry-After 的依据。
	var waitBytes, waitEvents time.Duration
	if lim.BytesPerSec > 0 {
		b.bytes.refill(now, lim.BytesPerSec, lim.BytesBurst)
		waitBytes = deficitWait(b.bytes.avail, needBytes, lim.BytesPerSec)
	}
	if lim.EventsPerSec > 0 {
		b.events.refill(now, lim.EventsPerSec, lim.EventsBurst)
		waitEvents = deficitWait(b.events.avail, needEvents, lim.EventsPerSec)
	}

	if waitBytes == 0 && waitEvents == 0 {
		if lim.BytesPerSec > 0 {
			b.bytes.avail -= needBytes
		}
		if lim.EventsPerSec > 0 {
			b.events.avail -= needEvents
		}
		sh.mu.Unlock()
		l.allowed.Add(1)
		return Decision{OK: true}
	}
	sh.mu.Unlock()

	// 报告等待更久的那个维度：它才是实际的瓶颈，也是容量分析时该看的那一维。
	dim, wait := DimBytes, waitBytes
	if waitEvents > waitBytes {
		dim, wait = DimEvents, waitEvents
	}
	if dim == DimBytes {
		l.rejectedBytes.Add(1)
	} else {
		l.rejectedEvents.Add(1)
	}
	return Decision{Dimension: dim, RetryAfter: clampRetryAfter(wait)}
}

// deficitWait 返回补足缺口所需时长，0 表示当前令牌已足够。
func deficitWait(avail, need, rate float64) time.Duration {
	if avail >= need {
		return 0
	}
	sec := (need - avail) / rate
	return time.Duration(sec * float64(time.Second))
}

// clampRetryAfter 把等待时长收敛到 [1s, MaxRetryAfter]。
//
// 下界是 1 秒而不是真实缺口：HTTP Retry-After 以整秒为单位，向下取整会得到 0，
// 而 Retry-After: 0 等于邀请客户端立刻重试，把限流变成忙等。
func clampRetryAfter(d time.Duration) time.Duration {
	// 向上取整到整秒，保证转成 Retry-After 头后不短于真实缺口。
	sec := d / time.Second
	if d%time.Second != 0 {
		sec++
	}
	out := sec * time.Second
	if out < time.Second {
		return time.Second
	}
	if out > MaxRetryAfter {
		return MaxRetryAfter
	}
	return out
}

// Sweep 回收空闲桶，返回回收数量。调用方应周期性调用（见 cmd/ingest-gateway）。
//
// 只回收"空闲足够久且两个维度都已补满"的桶。未满就回收等于给该 key 发一次免费突发：
// 下次请求会新建一个满桶，于是一个正在被限流的 key 只要停发一个 TTL 就能重置限额。
// TestSweepDoesNotGrantFreeBurst 固定这一条。
func (l *Limiter) Sweep() int {
	now := l.now()
	snap := l.snap.Load()
	evicted := 0

	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for key, b := range sh.m {
			if now.Sub(b.lastSeen) <= l.idleTTL {
				continue
			}
			lim := snap.limitsFor(key)
			if !bucketFull(b, lim, now) {
				continue
			}
			delete(sh.m, key)
			evicted++
		}
		sh.mu.Unlock()
	}

	if evicted > 0 {
		l.buckets.Add(int64(-evicted))
		l.evicted.Add(uint64(evicted))
	}
	return evicted
}

func bucketFull(b *bucket, lim Limits, now time.Time) bool {
	if lim.BytesPerSec > 0 {
		b.bytes.refill(now, lim.BytesPerSec, lim.BytesBurst)
		if b.bytes.avail < lim.BytesBurst {
			return false
		}
	}
	if lim.EventsPerSec > 0 {
		b.events.refill(now, lim.EventsPerSec, lim.EventsBurst)
		if b.events.avail < lim.EventsBurst {
			return false
		}
	}
	return true
}

// Stats 返回当前计数，供指标导出。
func (l *Limiter) Stats() Stats {
	s := Stats{
		Allowed:        l.allowed.Load(),
		RejectedBytes:  l.rejectedBytes.Load(),
		RejectedEvents: l.rejectedEvents.Load(),
		Buckets:        l.buckets.Load(),
		EvictedBuckets: l.evicted.Load(),
	}
	if snap := l.snap.Load(); snap != nil {
		s.SnapshotVersion = snap.Version
	}
	return s
}

// shardIndex 对 key 做 FNV-1a 哈希取分片。
//
// 手写而不用 hash/fnv：那个实现要求先把 key 拼成字符串或字节切片，而拼接会在限流热路径上
// 每次请求多分配一次内存。
func shardIndex(key identity.QuotaKey) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key.TenantID); i++ {
		h ^= uint64(key.TenantID[i])
		h *= prime64
	}
	h ^= '/'
	h *= prime64
	for i := 0; i < len(key.ClusterID); i++ {
		h ^= uint64(key.ClusterID[i])
		h *= prime64
	}
	return h % shardCount
}
