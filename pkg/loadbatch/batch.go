// Package loadbatch 把 DD-001 §3.4 的 IF-3 切批与 label 规则编码为纯函数。
//
// # 为什么必须是纯函数
//
// Stream Load 的幂等依赖「同一段 offset 区间总是被切成同一个批次、因而算出同一个
// label」。consumer rebalance 之后，新 owner 从同一 startOffset 出发，只要 Kafka 日志
// 没变，就必须切出同一个 endOffset。墙钟、实例 ID、并发度、rebalance generation 一旦
// 进入切批，两边 label 对不上：重复只是 DUPLICATE 模型已接受的代价，把没写入的
// offset 标成已完成才是静默缺口。
//
// 2026-10-04 Owner 双签（DD-001 §2.1 第 1–5 条）把这条钉死：2s 超时不能单独决定
// endOffset；S 与 64MiB 帽都是日志上的确定性条件。本包的 API 因此不接受 time.Time。
//
// # 格子与体积帽
//
//	next_aligned(start) = ((start / S) + 1) * S     // exclusive
//
// start 本身不回退（禁止为凑对齐而改写已 commit 的 offset）。「start 向下对齐到 S
// 倍数」描述的是全局格子：end 不得超过下一格，再和 64MiB encoded_bytes 帽取更小。
//
// encoded_bytes 是该 offset 展开后准备交给 Stream Load 的 JSON 体积，由调用方算好
// 再传入。本包不解析 OTLP，这样切批测试不必依赖 flatten。
package loadbatch

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// DefaultStride 是 M1 默认格子，不是测量值。变更属 IF-3，须 DD-002 双签。
	DefaultStride int64 = 4096
	// DefaultMaxEncodedBytes 是展开后 JSON 体积帽（64 MiB）。变更须双签。
	DefaultMaxEncodedBytes int64 = 64 << 20
)

// Reason 说明一次 Cut 停在哪个合法边界，或还没停下来。
type Reason string

const (
	// ReasonIncomplete 已读日志尚未碰到对齐格或体积帽。HWM 短批由调用方在本包之外处理。
	ReasonIncomplete Reason = "incomplete"
	// ReasonAligned 读到 next_aligned(start)。
	ReasonAligned Reason = "aligned"
	// ReasonSizeCap 加上本条会超过 B，且本批已有数据：不含本条。
	ReasonSizeCap Reason = "size_cap"
	// ReasonOversized 单条 encoded_bytes ≥ B，该条单独成批。
	ReasonOversized Reason = "oversized"
)

// Entry 是切批函数看到的一条 Kafka 日志。
type Entry struct {
	Offset       int64
	EncodedBytes int64
}

// Result 是一次纯函数切批的结果。
//
// Complete=false 时 End 是「若此刻按 HWM 提交会用的 exclusive end」，不等于可以
// 生成 label 的切点——调用方必须另作 HWM/空闲判断，不得把 Incomplete 的 End 当成
// 对齐边界去换 label。
type Result struct {
	Start     int64
	End       int64
	Complete  bool
	Reason    Reason
	Oversized bool
}

// Params 是切批参数。零值回落到已签字的 S / B。
type Params struct {
	Stride           int64
	MaxEncodedBytes  int64
}

func (p Params) stride() int64 {
	if p.Stride <= 0 {
		return DefaultStride
	}
	return p.Stride
}

func (p Params) maxBytes() int64 {
	if p.MaxEncodedBytes <= 0 {
		return DefaultMaxEncodedBytes
	}
	return p.MaxEncodedBytes
}

// NextAligned 返回 start 之后的下一个全局格子（exclusive）。
//
//	start=0    → 4096
//	start=100  → 4096
//	start=4096 → 8192
func NextAligned(start, stride int64) int64 {
	if stride <= 0 {
		stride = DefaultStride
	}
	return (start/stride + 1) * stride
}

// FloorStride 把 start 向下对齐到格子。只用于描述格子，不改 label 里的 start。
func FloorStride(start, stride int64) int64 {
	if stride <= 0 {
		stride = DefaultStride
	}
	return (start / stride) * stride
}

// Cut 从 start 起顺序看 log，返回下一个合法 end。
//
// log 必须按 offset 升序。offset < start 的条目被忽略；出现空洞则停在空洞前并
// 报 Incomplete——Kafka 分区日志本身连续，空洞只可能来自调用方漏喂。
//
// 本函数不接收时间：把 2s 传进来就会让同一段日志在积压回放与实时消费下切出
// 不同的 end，而这正是会签禁止的事。
func Cut(start int64, log []Entry, p Params) Result {
	stride := p.stride()
	maxBytes := p.maxBytes()
	limit := NextAligned(start, stride)

	out := Result{Start: start, End: start, Reason: ReasonIncomplete}
	var acc int64
	expect := start

	for _, e := range log {
		if e.Offset < start {
			continue
		}
		if e.Offset > expect {
			// 空洞：还没到对齐格，也还没到体积帽。
			out.End = expect
			return out
		}
		if e.Offset >= limit {
			out.End = limit
			out.Complete = true
			out.Reason = ReasonAligned
			return out
		}

		if acc > 0 && acc+e.EncodedBytes > maxBytes {
			out.End = e.Offset
			out.Complete = true
			out.Reason = ReasonSizeCap
			return out
		}

		acc += e.EncodedBytes
		expect = e.Offset + 1
		out.End = expect

		if e.EncodedBytes >= maxBytes && acc == e.EncodedBytes {
			out.Complete = true
			out.Reason = ReasonOversized
			out.Oversized = true
			return out
		}
	}

	if out.End >= limit {
		out.End = limit
		out.Complete = true
		out.Reason = ReasonAligned
	}
	return out
}

// Label 按 IF-3 命名：{topic}-{partition}-{startOffset}-{endOffset}。
// endOffset 为 exclusive。同一段四元组必须得到同一个字符串。
func Label(topic string, partition int32, start, end int64) string {
	return topic + "-" +
		strconv.FormatInt(int64(partition), 10) + "-" +
		strconv.FormatInt(start, 10) + "-" +
		strconv.FormatInt(end, 10)
}

// Range 是一个已切出的批次区间。
type Range struct {
	Topic     string
	Partition int32
	Start     int64
	End       int64
}

// LabelOf 是 Range 的 label。
func (r Range) Label() string {
	return Label(r.Topic, r.Partition, r.Start, r.End)
}

// Contains 报告 offset 是否落在 [Start, End)。
func (r Range) Contains(offset int64) bool {
	return offset >= r.Start && offset < r.End
}

// Empty 报告区间是否为空。空区间不得拿去 Stream Load。
func (r Range) Empty() bool { return r.End <= r.Start }

// ParseLabel 是 Label 的逆操作，供测试与排障。
func ParseLabel(s string) (Range, error) {
	// topic 本身可能含 '-'（当前没有）或 '.'。从右侧拆三个数字字段。
	i := strings.LastIndexByte(s, '-')
	if i <= 0 {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	end, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	s = s[:i]
	i = strings.LastIndexByte(s, '-')
	if i <= 0 {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	start, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	s = s[:i]
	i = strings.LastIndexByte(s, '-')
	if i <= 0 {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	part, err := strconv.ParseInt(s[i+1:], 10, 32)
	if err != nil {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	topic := s[:i]
	if topic == "" {
		return Range{}, fmt.Errorf("loadbatch: 非法 label %q", s)
	}
	return Range{Topic: topic, Partition: int32(part), Start: start, End: end}, nil
}
