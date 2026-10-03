// Package pipeline 把切批纯函数、展平、Stream Load 与 offset 提交串成一条管线。
//
// # 切批 / offset / label 三处都只调用 loadbatch
//
// 会签要求同一段 Kafka 日志必须得到同一个 label。本包不另写切批条件：Complete
// 只来自 loadbatch.Cut；2s 只在已经停在 HWM 且本批非空时触发提交，不进入 Cut。
//
// # 身份只认头
//
// 缺头或头非法的消息计入 HeaderError，不从 payload 猜 tenant，也不写 Doris 行。
// offset 仍被纳入批次——否则毒消息会永久堵住分区。这与「label ⊆ 实际写入行」
// 针对的静默缺口不同：缺口是声称写了有效数据却没写；毒消息是显式丢弃并计量。
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/mokaz/ops-system/internal/kafka/topics"
	"github.com/mokaz/ops-system/internal/loader/flatten"
	"github.com/mokaz/ops-system/internal/loader/kafkasrc"
	"github.com/mokaz/ops-system/internal/loader/streamload"
	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/latency"
	"github.com/mokaz/ops-system/pkg/loadbatch"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// Metrics 是管线打点的最小面。selfmon 实现它；测试用 Nop。
type Metrics interface {
	ObserveStage(stage latency.Stage, sig telemetry.Signal, d time.Duration)
	HeaderError(topic string)
	FlattenError(sig telemetry.Signal)
	Conflict(c flatten.Conflict)
	Oversized(sig telemetry.Signal)
	LoadDone(sig telemetry.Signal, outcome streamload.Outcome, rows int64)
}

// Nop 是空实现。
type Nop struct{}

func (Nop) ObserveStage(latency.Stage, telemetry.Signal, time.Duration) {}
func (Nop) HeaderError(string)                                          {}
func (Nop) FlattenError(telemetry.Signal)                               {}
func (Nop) Conflict(flatten.Conflict)                                   {}
func (Nop) Oversized(telemetry.Signal)                                  {}
func (Nop) LoadDone(telemetry.Signal, streamload.Outcome, int64)        {}

// Config 是管线参数。
type Config struct {
	Params      loadbatch.Params
	IdleSubmit  time.Duration
	Flatten     flatten.Options
	TableLogs   string
	TableSpans  string
	RetryWait   time.Duration
	MaxRetryWait time.Duration
}

func (c Config) idle() time.Duration {
	if c.IdleSubmit <= 0 {
		return kafkasrc.IdleSubmit
	}
	return c.IdleSubmit
}

func (c Config) retryWait() time.Duration {
	if c.RetryWait <= 0 {
		return time.Second
	}
	return c.RetryWait
}

func (c Config) maxRetryWait() time.Duration {
	if c.MaxRetryWait <= 0 {
		return 30 * time.Second
	}
	return c.MaxRetryWait
}

// Pipeline 是单进程内的消费循环。
type Pipeline struct {
	src  kafkasrc.Source
	load streamload.Client
	cfg  Config
	met  Metrics
	log  *slog.Logger
	now  func() time.Time

	parts map[kafkasrc.PartKey]*partState
}

type pending struct {
	offset int64
	bytes  int64
	rows   []map[string]any
	sig    telemetry.Signal
	recvAt time.Time
}

type partState struct {
	start   int64
	ready   bool
	pending []pending
	firstAt time.Time
	idleAt  time.Time
	signal  telemetry.Signal
}

// New 创建管线。
func New(src kafkasrc.Source, load streamload.Client, cfg Config, met Metrics, log *slog.Logger) *Pipeline {
	if met == nil {
		met = Nop{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Pipeline{
		src:   src,
		load:  load,
		cfg:   cfg,
		met:   met,
		log:   log,
		now:   time.Now,
		parts: make(map[kafkasrc.PartKey]*partState),
	}
}

// Run 直到 ctx 取消。
func (p *Pipeline) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.Step(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			p.log.Error("消费步进失败", "err", err)
		}
	}
}

// Step 拉取一次并尽可能提交完整批次。测试直接调它。
func (p *Pipeline) Step(ctx context.Context) error {
	batch, err := p.src.Poll(ctx)
	if err != nil {
		return err
	}
	now := p.now()
	touched := map[kafkasrc.PartKey]bool{}
	for _, m := range batch.Messages {
		k := kafkasrc.PartKey{Topic: m.Topic, Partition: m.Partition}
		touched[k] = true
		p.ingest(m, now)
	}
	for k, st := range p.parts {
		if !touched[k] {
			if st.idleAt.IsZero() {
				st.idleAt = now
			}
		} else {
			st.idleAt = time.Time{}
		}
		if err := p.flush(ctx, k, st, batch, now); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) ingest(m kafkasrc.Message, now time.Time) {
	k := kafkasrc.PartKey{Topic: m.Topic, Partition: m.Partition}
	st := p.parts[k]
	if st == nil {
		st = &partState{start: m.Offset, signal: signalOf(m.Topic)}
		p.parts[k] = st
	}
	if !st.ready {
		st.start = m.Offset
		st.ready = true
		st.firstAt = now
	}
	if m.Offset < st.start {
		return
	}

	item := pending{offset: m.Offset, recvAt: now, sig: st.signal}
	meta, err := ingestmsg.ParseMeta(m.Headers)
	if err != nil {
		p.met.HeaderError(m.Topic)
		p.log.Error("消息头非法，不从 payload 猜 tenant", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "err", err)
		st.pending = append(st.pending, item)
		return
	}
	item.sig = meta.Signal
	st.signal = meta.Signal
	item.recvAt = meta.ReceivedAt

	out, ferr := flatten.Flatten(meta.Signal, m.Value, flatten.Identity{
		TenantID:  meta.TenantID,
		ProjectID: meta.ProjectID,
		ClusterID: meta.ClusterID,
	}, p.cfg.Flatten)
	if ferr != nil {
		p.met.FlattenError(meta.Signal)
		p.log.Error("OTLP 展开失败", "topic", m.Topic, "offset", m.Offset, "err", ferr)
		st.pending = append(st.pending, item)
		return
	}
	item.bytes = out.EncodedBytes
	item.rows = out.Rows
	for _, c := range out.Conflicts {
		p.met.Conflict(c)
	}
	st.pending = append(st.pending, item)
}

func (p *Pipeline) flush(ctx context.Context, k kafkasrc.PartKey, st *partState, batch kafkasrc.Batch, now time.Time) error {
	if len(st.pending) == 0 {
		return nil
	}
	entries := make([]loadbatch.Entry, len(st.pending))
	for i, it := range st.pending {
		entries[i] = loadbatch.Entry{Offset: it.offset, EncodedBytes: it.bytes}
	}
	cut := loadbatch.Cut(st.start, entries, p.cfg.Params)
	if cut.Oversized {
		p.met.Oversized(st.signal)
	}

	end := cut.End
	complete := cut.Complete
	if !complete {
		hwm, ok := batch.HWMOf(k.Topic, k.Partition)
		if ok && lastEnd(st) >= hwm && !st.idleAt.IsZero() && now.Sub(st.idleAt) >= p.cfg.idle() {
			// HWM 短批：合法 end = HWM，2s 只触发提交，不改切批函数。
			end = hwm
			complete = end > st.start
		}
	}
	if !complete || end <= st.start {
		return nil
	}
	return p.submit(ctx, k, st, end, now)
}

func lastEnd(st *partState) int64 {
	if len(st.pending) == 0 {
		return st.start
	}
	return st.pending[len(st.pending)-1].offset + 1
}

func (p *Pipeline) submit(ctx context.Context, k kafkasrc.PartKey, st *partState, end int64, now time.Time) error {
	var rows []map[string]any
	kept := st.pending[:0]
	var rest []pending
	for _, it := range st.pending {
		if it.offset >= st.start && it.offset < end {
			rows = append(rows, it.rows...)
			continue
		}
		if it.offset >= end {
			rest = append(rest, it)
		} else {
			kept = append(kept, it)
		}
	}
	_ = kept

	rng := loadbatch.Range{Topic: k.Topic, Partition: k.Partition, Start: st.start, End: end}
	if st.firstAt.IsZero() {
		st.firstAt = now
	}
	p.met.ObserveStage(latency.StageLoaderBatch, st.signal, now.Sub(st.firstAt))

	if len(rows) > 0 {
		body, err := json.Marshal(rows)
		if err != nil {
			return err
		}
		req := streamload.Request{
			Range:  rng,
			Signal: st.signal,
			Body:   body,
		}
		if table, err := streamload.TableFor(st.signal, p.cfg.TableLogs, p.cfg.TableSpans); err == nil {
			req.Table = table
		}
		if err := p.loadWithRetry(ctx, req, st.signal, int64(len(rows))); err != nil {
			return err
		}
	}

	if err := p.src.Commit(ctx, k.Topic, k.Partition, end); err != nil {
		return err
	}
	st.start = end
	st.pending = rest
	st.firstAt = now
	st.idleAt = time.Time{}
	if len(st.pending) == 0 {
		st.ready = false
	}
	return nil
}

func (p *Pipeline) loadWithRetry(ctx context.Context, req streamload.Request, sig telemetry.Signal, rows int64) error {
	wait := p.cfg.retryWait()
	for {
		start := p.now()
		res, err := p.load.Load(ctx, req)
		p.met.ObserveStage(latency.StageStreamLoad, sig, p.now().Sub(start))
		if err != nil && res.Outcome == 0 && !errors.Is(err, streamload.ErrEmptyBody) {
			res.Outcome = streamload.OutcomeRetry
		}
		if res.Outcome.Succeeded() {
			p.met.LoadDone(sig, res.Outcome, rows)
			return nil
		}
		p.met.LoadDone(sig, res.Outcome, 0)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// 不改 label / 不改 Range。退避后用同一 Request 再交。
		p.log.Error("Stream Load 将按同一 label 重试",
			"label", req.Range.Label(), "outcome", res.Outcome.String(), "msg", res.Message, "err", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if wait < p.cfg.maxRetryWait() {
			wait *= 2
			if wait > p.cfg.maxRetryWait() {
				wait = p.cfg.maxRetryWait()
			}
		}
	}
}

func signalOf(topic string) telemetry.Signal {
	switch topic {
	case topics.LogsRaw:
		return telemetry.SignalLogs
	case topics.TracesRaw:
		return telemetry.SignalTraces
	default:
		return telemetry.SignalLogs
	}
}
