// Package kafkasink 实现 IF-2：摄入网关 → Kafka 的 topic、分区键与投递语义。
//
// # 接口 + 真实实现 + 内存 fake
//
// Producer 是接口，Kafka 实现与内存 Fake 并列。这不只是为了测试：M1 的摄入压测（PLAN B7）
// 需要能把 Kafka 这一跳摘掉单独标定网关自身的 0.5s 预算，否则网关超支与 Kafka 超支无法
// 区分——而这正是 PLAN §2.4 要求分段打点的理由。
//
// # 分区键
//
// IF-2 规定 logs.raw 的分区键为 tenant_id + service、traces.raw 为 trace_id。
// 本实现对 logs 照办；traces 在 M1 偏离为 tenant_id + cluster_id，理由见 Router.Route。
package kafkasink

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/mokaz/ops-system/internal/kafka/topics"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// IF-2 的 topic 名与分区数。权威定义在 internal/kafka/topics，由 ensure-kafka-topics 建出来。
// 网关只引用，避免生产者与建 topic 命令各写一份数字之后漂成两个集群。
const (
	TopicLogsRaw   = topics.LogsRaw
	TopicTracesRaw = topics.TracesRaw

	PartitionsLogsRaw   = topics.LogsPartitions
	PartitionsTracesRaw = topics.TracesPartitions
)

// ErrNoTopic 表示该信号没有配置 topic。
//
// Metrics 命中这条属于正常情形：SD-000 §5.1 分层原则 1 规定 metrics 不过 Kafka。
var ErrNoTopic = errors.New("kafkasink: 该信号未配置 topic")

// Record 是一条待投递的 Kafka 消息。
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []ingestmsg.Header
}

// Producer 投递消息。实现必须是并发安全的。
//
// Produce 返回时消息应已被 acks=all 确认——"写入成功"必须意味着 ISR 已落盘，否则
// SLO-6（摄入确认后丢失率 < 0.01%）在网关这一跳就已经不成立：我们会在数据只存在于
// 单个 broker 的内存里时就给客户端回 200。
type Producer interface {
	Produce(ctx context.Context, rec Record) error
	Close() error
}

// TopicConfig 把信号映射到 topic。
type TopicConfig struct {
	Logs   string
	Traces string
}

// DefaultTopics 返回 IF-2 的默认映射。
func DefaultTopics() TopicConfig {
	return TopicConfig{Logs: TopicLogsRaw, Traces: TopicTracesRaw}
}

// For 返回该信号的 topic。
func (c TopicConfig) For(sig telemetry.Signal) (string, error) {
	switch sig {
	case telemetry.SignalLogs:
		if c.Logs == "" {
			return "", ErrNoTopic
		}
		return c.Logs, nil
	case telemetry.SignalTraces:
		if c.Traces == "" {
			return "", ErrNoTopic
		}
		return c.Traces, nil
	default:
		return "", ErrNoTopic
	}
}

// Router 按 IF-2 把一条摄入请求组装成 Kafka 消息。
type Router struct {
	Topics TopicConfig

	// KeyBuckets 为指定租户在分区键上追加 bucket 后缀，取值为后缀基数。
	//
	// 对应 DD-001 §3.3 的热点打散条款：超大租户单 key 可能打爆单分区（水位 4 万 EPS）。
	// 打散后该租户失去同服务有序性——DD-001 OQ-2 正是在问有序性是否为硬要求，尚无结论，
	// 因此这里默认不打散，只给出按租户逐个开启的开关，而不是全局策略。
	KeyBuckets map[identity.TenantID]int

	rr atomic.Uint64
}

// Route 组装消息。payload 为客户原始的 OTLP protobuf 字节，不做任何改写。
func (r *Router) Route(meta ingestmsg.Meta, payload []byte) (Record, error) {
	topic, err := r.Topics.For(meta.Signal)
	if err != nil {
		return Record{}, err
	}
	return Record{
		Topic:   topic,
		Key:     r.key(meta),
		Value:   payload,
		Headers: meta.Headers(),
	}, nil
}

// key 计算分区键。
//
// logs：tenant_id + service，照 IF-2。注意一个 OTLP 批次可能含多个 service，这里取首个
// resource 的 service.name——批次内混多个服务时，有序性保证退化为"按首个服务归并"。
//
// traces：IF-2 规定用 trace_id，但 M1 做不到，改用 tenant_id + cluster_id。
// 原因是一个 ExportTraceServiceRequest 通常含多条 trace，要按 trace_id 分区就必须把批次
// 拆成每 trace 一条消息——那需要完整反序列化 + 按 trace 重新编码，与网关 0.5s 预算和
// otlpwire 的快速路径直接冲突，还会把消息尺寸打碎成几 KB 级别，压缩率与吞吐都显著变差。
// DD-001 §3.2 原设计里做这件事的是平台侧 Collector 的 kafka_exporter，而 M1 没有这一跳。
//
// 按 trace_id 分区唯一的硬需求来自"平台侧全局 tail sampling"，DD-001 §3.7 已把它列为二期。
// 该偏离已记入 DD-001 §3.3 与 OQ-6。
func (r *Router) key(meta ingestmsg.Meta) []byte {
	var tail string
	switch meta.Signal {
	case telemetry.SignalTraces:
		tail = string(meta.ClusterID)
	default:
		tail = meta.ServiceName
		if tail == "" {
			// 空串会让所有缺 service.name 的租户数据落到同一分区，而缺失恰恰常见于
			// 埋点不规范的新接入租户。用固定占位符使这种流量在分区分布上可见。
			tail = "unknown"
		}
	}

	key := make([]byte, 0, len(meta.TenantID)+len(tail)+8)
	key = append(key, meta.TenantID...)
	key = append(key, '|')
	key = append(key, tail...)

	if n := r.KeyBuckets[meta.TenantID]; n > 1 {
		// 轮转而非随机：轮转在低 QPS 下也能均匀铺开，随机要靠大数定律。
		b := r.rr.Add(1) % uint64(n)
		key = append(key, '#')
		key = strconv.AppendUint(key, b, 10)
	}
	return key
}

// Fake 是内存 Producer，供测试与无 Kafka 环境下的压测基线使用。
type Fake struct {
	mu      sync.Mutex
	records []Record
	closed  bool

	// Err 非 nil 时所有 Produce 直接返回它，用于验证错误如实传播（E6/E7 不得折叠成 200）。
	Err error
	// Hook 在记录消息前被调用，返回非 nil 错误则本次投递失败。用于注入延迟或按消息判定。
	Hook func(Record) error
}

// NewFake 创建内存 Producer。
func NewFake() *Fake { return &Fake{} }

func (f *Fake) Produce(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	hook, injected, closed := f.Hook, f.Err, f.closed
	f.mu.Unlock()

	if closed {
		return errors.New("kafkasink: Fake 已关闭")
	}
	if injected != nil {
		return injected
	}
	if hook != nil {
		if err := hook(rec); err != nil {
			return err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
	return nil
}

func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Records 返回已投递消息的快照。
func (f *Fake) Records() []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Record, len(f.records))
	copy(out, f.records)
	return out
}

// Len 返回已投递消息数。
func (f *Fake) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

// SetErr 设置注入的错误。
func (f *Fake) SetErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Err = err
}
