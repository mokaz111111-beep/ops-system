package kafkasrc

import (
	"context"
	"sync"
)

// CommitRec 是 Fake 记下的一次 offset 提交。
type CommitRec struct {
	Topic      string
	Partition  int32
	NextOffset int64
}

// Fake 是内存 Source。
type Fake struct {
	mu      sync.Mutex
	queue   []Message
	hwm     map[PartKey]int64
	commits []CommitRec
	closed  bool
}

// NewFake 创建空 Source。
func NewFake() *Fake {
	return &Fake{hwm: make(map[PartKey]int64)}
}

// Push 追加可被 Poll 的消息，并按 max(offset+1) 抬高水位。
func (f *Fake) Push(msgs ...Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.queue = append(f.queue, m)
		k := PartKey{m.Topic, m.Partition}
		if next := m.Offset + 1; next > f.hwm[k] {
			f.hwm[k] = next
		}
	}
}

// SetHWM 显式设置高水位，用于「追上 HWM」的测试。
func (f *Fake) SetHWM(topic string, partition int32, hwm int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hwm[PartKey{topic, partition}] = hwm
}

func (f *Fake) Poll(ctx context.Context) (Batch, error) {
	if err := ctx.Err(); err != nil {
		return Batch{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return Batch{}, context.Canceled
	}
	out := Batch{Messages: f.queue}
	f.queue = nil
	for k, h := range f.hwm {
		out.Watermarks = append(out.Watermarks, Watermark{Topic: k.Topic, Partition: k.Partition, HWM: h})
	}
	return out, nil
}

func (f *Fake) Commit(ctx context.Context, topic string, partition int32, nextOffset int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, CommitRec{Topic: topic, Partition: partition, NextOffset: nextOffset})
	return nil
}

func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Commits 返回提交快照。
func (f *Fake) Commits() []CommitRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]CommitRec, len(f.commits))
	copy(out, f.commits)
	return out
}
