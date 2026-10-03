package kafkasink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mokaz/ops-system/pkg/ingesterr"
)

// 为什么选 franz-go
//
// 需要四件事：acks=all、lz4、显式的"缓冲区满"信号、以及幂等生产。前两项各家都有；
// 第三项是本实现依赖 franz-go 的真正原因——IF-1 把"下游不可用"（E6）与"下游背压"（E7）
// 分成两个码，而要如实区分它们，客户端必须能在缓冲区满时立刻失败而不是阻塞。
// franz-go 的 TryProduce 精确提供这个语义（ErrMaxBuffered）；阻塞式客户端只能靠超时
// 把背压伪装成超时，于是 E7 永远不会出现，DD-001 §3.5 要求的端到端背压就只剩纸面。
// 第四项（幂等生产）顺带降低重试造成的重复，与下游 Stream Load label 幂等叠加。

// KafkaConfig 是生产者配置。
type KafkaConfig struct {
	Brokers  []string
	ClientID string

	// MaxBufferedRecords 是生产端缓冲的消息条数上限，即背压（E7）的触发点。
	//
	// 这个值是一次权衡：太小会在 Kafka 正常抖动时就对客户端报 429，太大则把客户端的
	// 持久队列搬到网关内存里——而网关是无状态副本，内存里的消息在重启时直接丢失，
	// 这正好违反 SLO-6。宁可偏小：客户侧 file_storage 队列按 ≥2 小时设计（DD-001 §3.3），
	// 比网关内存可靠得多。
	MaxBufferedRecords int

	// ProduceTimeout 是单次投递的上限，超时按 E6 上报。
	ProduceTimeout time.Duration

	// TLS 为 nil 表示明文连接（Kafka 与网关同在平台 VPC 内时的常见配置）。
	TLS *tls.Config
}

// 默认值。缓冲 20000 条约对应 2 万条待确认消息；按常态 3.5 万 EPS、单消息平均数百条记录
// 估算，它是秒级的缓冲而不是分钟级——分钟级缓冲应该在客户端的持久队列里，不在这里。
const (
	defaultMaxBufferedRecords = 20000
	defaultProduceTimeout     = 3 * time.Second
)

// Kafka 是基于 franz-go 的 Producer 实现。
type Kafka struct {
	cl      *kgo.Client
	timeout time.Duration
}

// NewKafka 创建生产者。它不连接 broker——franz-go 按需建连，这使网关启动不依赖 Kafka
// 可用（启动期拒绝服务会把一次 Kafka 抖动放大成一次网关不可用）。
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkasink: 未配置 broker")
	}
	maxBuf := cfg.MaxBufferedRecords
	if maxBuf <= 0 {
		maxBuf = defaultMaxBufferedRecords
	}
	timeout := cfg.ProduceTimeout
	if timeout <= 0 {
		timeout = defaultProduceTimeout
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// IF-2：acks=all。min.insync.replicas 是 topic 配置，由 ensure-kafka-topics 按
		// profile 写入（prod=2，单节点 dev=1）。不调用 AllowAutoTopicCreation：缺 topic
		// 必须变成 E6，而不是默默建出 1 分区的 logs.raw。
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.MaxBufferedRecords(maxBuf),
		kgo.ProduceRequestTimeout(timeout),
		// 分区键由 Router 给出，这里用 sticky key 分区器：同 key 必落同分区，
		// 这是 IF-2"保证同租户同服务有序"所依赖的前提。
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		// 投递超时与 ProduceTimeout 对齐：超过即让 promise 带错返回，由调用方转成 E6。
		// 不设则 franz-go 会无限重试，请求挂在网关上直到客户端超时——那会让客户端看到
		// 一个没有状态码的连接错误，而非一个明确可重试的 503。
		kgo.RecordDeliveryTimeout(timeout),
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "ingest-gateway"
	}
	opts = append(opts, kgo.ClientID(clientID))
	if cfg.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLS))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkasink: 创建客户端失败: %w", err)
	}
	return &Kafka{cl: cl, timeout: timeout}, nil
}

// Produce 同步投递一条消息，返回时已被 acks=all 确认。
//
// 用 TryProduce 而非 ProduceSync：后者在缓冲区满时阻塞，阻塞会把背压变成延迟，
// 而 IF-1 要求背压表现为 E7（429）让客户端进本地队列。见本文件顶部的选型说明。
func (k *Kafka) Produce(ctx context.Context, rec Record) error {
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()

	kr := &kgo.Record{Topic: rec.Topic, Key: rec.Key, Value: rec.Value}
	if len(rec.Headers) > 0 {
		kr.Headers = make([]kgo.RecordHeader, 0, len(rec.Headers))
		for _, h := range rec.Headers {
			kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
		}
	}

	done := make(chan error, 1)
	k.cl.TryProduce(ctx, kr, func(_ *kgo.Record, err error) { done <- err })

	select {
	case err := <-done:
		return Classify(err)
	case <-ctx.Done():
		// 不等 promise 了：请求侧必须有界。消息可能仍会被投出去，于是客户端重试会产生
		// 重复——这正是摄入链路选择 at-least-once + Stream Load label 幂等的原因
		// （DD-001 §3.3 末段），重复在下游被吸收，而丢数不可恢复。
		return ingesterr.Wrap(ingesterr.CodeSinkUnavailable, "Kafka 投递超时", ctx.Err()).
			WithRetryAfter(time.Second)
	}
}

// Close 关闭客户端，等待缓冲区内的消息被投出。
func (k *Kafka) Close() error {
	k.cl.Close()
	return nil
}

// Classify 把 Kafka 侧错误映射为 IF-1 错误码。
//
// 这个映射是 SD-000 §7.3 那条契约的实际落点，三条分支各有不可妥协的理由：
//
//   - 缓冲区满 → E7（429）：这是背压，不是故障。折叠成 E6 也能工作，但会让"下游真的挂了"
//     与"下游跟不上"在监控上无法区分，而两者的处置动作完全不同（一个叫人，一个扩容）。
//   - 消息超过 broker 上限 → E4（413）：Kafka 判定这类错误不可重试，重试只会制造毒消息
//     风暴。它表现为客户端批次过大，正是 E4 的语义。
//   - 其余一律 E6（503）且可重试：包括未知错误。默认可重试是刻意的——把未知错误判成
//     不可重试等于让客户端丢数据，而这是本平台唯一不可逆的失败。
func Classify(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, kgo.ErrMaxBuffered) {
		return ingesterr.Wrap(ingesterr.CodeBackpressure, "Kafka 生产缓冲已满", err).
			WithRetryAfter(time.Second)
	}
	if errors.Is(err, kerr.MessageTooLarge) || errors.Is(err, kerr.RecordListTooLarge) {
		return ingesterr.Wrap(ingesterr.CodeBodyTooLarge, "单条消息超过 Kafka 上限", err)
	}
	return ingesterr.Wrap(ingesterr.CodeSinkUnavailable, "Kafka 投递失败", err).
		WithRetryAfter(time.Second)
}
