// Package kafkasrc 是 otlp-loader 的 Kafka 消费面。
//
// 与 kafkasink 对称：Source 接口 + 真 Kafka + 内存 Fake。测试与无 broker 环境
// 走 Fake，生产走 franz-go consumer group。offset 是 loader 的全部状态。
package kafkasrc

import (
	"context"
	"time"

	"github.com/mokaz/ops-system/pkg/ingestmsg"
)

// Message 是一条已拉取、尚未 commit 的记录。
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Headers   []ingestmsg.Header
	Value     []byte
}

// Watermark 是一个分区的高水位（下一条尚未写出的 offset，与 Kafka HWM 同义）。
type Watermark struct {
	Topic     string
	Partition int32
	HWM       int64
}

// Batch 是一次 Poll 的结果。
type Batch struct {
	Messages   []Message
	Watermarks []Watermark
}

// Source 拉取并提交 offset。Commit 的 offset 是 exclusive（下一条要读的位置），
// 与 loadbatch.Range.End、Kafka 习惯一致。
type Source interface {
	Poll(ctx context.Context) (Batch, error)
	Commit(ctx context.Context, topic string, partition int32, nextOffset int64) error
	Close() error
}

// PartKey 标识一个分区。
type PartKey struct {
	Topic     string
	Partition int32
}

// HWMOf 返回该分区在本批里的高水位，没有则 ok=false。
func (b Batch) HWMOf(topic string, partition int32) (int64, bool) {
	for _, w := range b.Watermarks {
		if w.Topic == topic && w.Partition == partition {
			return w.HWM, true
		}
	}
	return 0, false
}

// IdleSubmit 是 HWM 短批的空闲窗口，对应 loader 段 2.0s 预算。
// 它不是切批函数的输入——只有已经停在合法 end（含 HWM）时才允许用它触发提交。
const IdleSubmit = 2 * time.Second
