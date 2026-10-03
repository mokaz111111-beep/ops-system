package kafkasrc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/mokaz/ops-system/pkg/ingestmsg"
)

// KafkaConfig 是消费端配置。
type KafkaConfig struct {
	Brokers    []string
	GroupID    string
	ClientID   string
	Topics     []string
	TLS        *tls.Config
	PollTimeout time.Duration
}

// Kafka 是基于 franz-go 的 consumer group。
type Kafka struct {
	cl      *kgo.Client
	timeout time.Duration
}

// NewKafka 创建消费者。不在此时强连 broker，理由与网关生产者相同。
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkasrc: 未配置 broker")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("kafkasrc: 未配置 consumer group")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("kafkasrc: 未配置 topic")
	}
	timeout := cfg.PollTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	clientID := cfg.ClientID
	if clientID == "" {
		clientID = "otlp-loader"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ClientID(clientID),
		// offset 由管线在 Stream Load 成功后显式提交。自动提交会在 label
		// 尚未落库时把 offset 推过，正好踩中「声称写了、实际没写」的缺口。
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(timeout),
	}
	if cfg.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLS))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkasrc: 创建客户端失败: %w", err)
	}
	return &Kafka{cl: cl, timeout: timeout}, nil
}

func (k *Kafka) Poll(ctx context.Context) (Batch, error) {
	fetches := k.cl.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return Batch{}, context.Canceled
	}
	if err := fetches.Err0(); err != nil && !errors.Is(err, context.Canceled) {
		// 单分区错误不丢整批：其余分区的消息仍要切。
		if fetches.NumRecords() == 0 {
			return Batch{}, err
		}
	}

	var out Batch
	fetches.EachRecord(func(r *kgo.Record) {
		hs := make([]ingestmsg.Header, 0, len(r.Headers))
		for _, h := range r.Headers {
			hs = append(hs, ingestmsg.Header{Key: h.Key, Value: h.Value})
		}
		out.Messages = append(out.Messages, Message{
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Headers:   hs,
			Value:     r.Value,
		})
	})
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if p.Err != nil {
			return
		}
		// HighWatermark 是 exclusive。
		out.Watermarks = append(out.Watermarks, Watermark{
			Topic:     p.Topic,
			Partition: p.Partition,
			HWM:       p.HighWatermark,
		})
	})
	return out, nil
}

func (k *Kafka) Commit(ctx context.Context, topic string, partition int32, nextOffset int64) error {
	// franz-go 的 EpochOffset.Offset 即下一条要读的 offset。
	to := map[string]map[int32]kgo.EpochOffset{
		topic: {partition: {Offset: nextOffset}},
	}
	var commitErr error
	k.cl.CommitOffsetsSync(ctx, to, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
	})
	return commitErr
}

func (k *Kafka) Close() error {
	k.cl.Close()
	return nil
}
