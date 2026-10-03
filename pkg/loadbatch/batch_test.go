package loadbatch

import (
	"testing"
)

func entries(start, n int64, bytes int64) []Entry {
	out := make([]Entry, n)
	for i := int64(0); i < n; i++ {
		out[i] = Entry{Offset: start + i, EncodedBytes: bytes}
	}
	return out
}

func TestNextAlignedGrid(t *testing.T) {
	cases := []struct {
		start int64
		want  int64
	}{
		{0, 4096},
		{1, 4096},
		{100, 4096},
		{4095, 4096},
		{4096, 8192},
		{4097, 8192},
		{8192, 12288},
	}
	for _, tc := range cases {
		if got := NextAligned(tc.start, DefaultStride); got != tc.want {
			t.Errorf("NextAligned(%d) = %d，期望 %d", tc.start, got, tc.want)
		}
		if FloorStride(tc.start, DefaultStride) > tc.start {
			t.Errorf("FloorStride(%d) 上溢", tc.start)
		}
	}
}

func TestSameOffsetRangeSameLabel(t *testing.T) {
	// 会签第 1 条：同一段 (topic, partition, start, end) 必须算出同一个 label。
	a := Label("logs.raw", 3, 100, 4096)
	b := Label("logs.raw", 3, 100, 4096)
	if a != b || a != "logs.raw-3-100-4096" {
		t.Fatalf("label 不稳定: %q vs %q", a, b)
	}
	got, err := ParseLabel(a)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "logs.raw" || got.Partition != 3 || got.Start != 100 || got.End != 4096 {
		t.Errorf("ParseLabel 往返失败: %+v", got)
	}
	if Label("logs.raw", 3, 100, 200) == a {
		t.Error("不同 end 不得共用 label——换 end 重打会拆掉幂等")
	}
}

func TestCutAlignsToNextGrid(t *testing.T) {
	// start=100 向下对齐到 0，end 不得超过 4096。
	log := entries(100, 3996, 16) // 100..4095
	got := Cut(100, log, Params{})
	if !got.Complete || got.Reason != ReasonAligned || got.End != 4096 {
		t.Fatalf("期望对齐到 4096，得到 %+v", got)
	}
	if Label("logs.raw", 0, 100, got.End) != "logs.raw-0-100-4096" {
		t.Error("未对齐 start 不得被回写成格子起点")
	}
}

func TestCutDoesNotRewindStart(t *testing.T) {
	log := entries(4096, 10, 16)
	got := Cut(4096, log, Params{})
	if got.Start != 4096 {
		t.Fatalf("禁止为凑对齐回退 start，得到 %d", got.Start)
	}
	if got.Complete {
		t.Fatalf("10 条远未到 8192，不应 Complete: %+v", got)
	}
	if NextAligned(got.Start, DefaultStride) != 8192 {
		t.Error("下一格应为 8192")
	}
}

func TestSizeCapCutsAtBoundary(t *testing.T) {
	// 每条 6MiB，第 11 条会把累计顶过 64MiB：应在 offset=10 切开（不含该条）。
	const six = 6 << 20
	log := entries(0, 20, six)
	got := Cut(0, log, Params{})
	if !got.Complete || got.Reason != ReasonSizeCap {
		t.Fatalf("期望体积帽切开，得到 %+v", got)
	}
	if got.End != 10 {
		t.Fatalf("体积帽切点应为 10，得到 %d", got.End)
	}
	// 同一段日志再切一次必须得到同一 end——与墙钟无关。
	again := Cut(0, log, Params{})
	if again.End != got.End || again.Reason != got.Reason {
		t.Fatalf("同一段日志切出不同批次: %+v vs %+v", got, again)
	}
}

func TestSizeCapDoesNotSplitFirstRecord(t *testing.T) {
	// 单条 ≥ B 是唯一允许在格子内再切一刀的情况，且由日志内容决定。
	log := []Entry{{Offset: 7, EncodedBytes: DefaultMaxEncodedBytes + 1}}
	got := Cut(7, log, Params{})
	if !got.Complete || !got.Oversized || got.Reason != ReasonOversized || got.End != 8 {
		t.Fatalf("超大单条应独立成批 end=8，得到 %+v", got)
	}
}

func TestTwoSecondsCannotChangeEnd(t *testing.T) {
	// API 没有时间参数：同一 log 无论「调用了多少次、中间隔了多久」end 不变。
	// 这比「传入 fake clock」更硬——2s 根本进不了函数。
	log := entries(0, 100, 32)
	first := Cut(0, log, Params{})
	second := Cut(0, log, Params{})
	if first != second {
		t.Fatalf("Cut 不是纯函数: %+v vs %+v", first, second)
	}
	if first.Complete {
		t.Fatal("100 条未到 4096 也未到 64MiB，2s 不得把它变成 Complete")
	}
	if first.End != 100 {
		t.Fatalf("Incomplete 的试探 end 应为 100，得到 %d", first.End)
	}
}

func TestIncompleteUntilLogArrives(t *testing.T) {
	small := entries(0, 3, 8)
	got := Cut(0, small, Params{})
	if got.Complete || got.Reason != ReasonIncomplete {
		t.Fatalf("日志不够时必须 Incomplete: %+v", got)
	}

	full := entries(0, 4096, 8)
	got = Cut(0, full, Params{})
	if !got.Complete || got.End != 4096 || got.Reason != ReasonAligned {
		t.Fatalf("喂满一格应对齐: %+v", got)
	}
}

func TestGapStopsIncomplete(t *testing.T) {
	log := []Entry{
		{Offset: 0, EncodedBytes: 8},
		{Offset: 1, EncodedBytes: 8},
		{Offset: 5, EncodedBytes: 8}, // 漏了 2,3,4
	}
	got := Cut(0, log, Params{})
	if got.Complete || got.End != 2 {
		t.Fatalf("空洞应停在 2: %+v", got)
	}
}

func TestSkipsAlreadyCommittedPrefix(t *testing.T) {
	log := entries(0, 50, 8)
	got := Cut(10, log, Params{})
	if got.Start != 10 || got.End != 50 || got.Complete {
		t.Fatalf("应从 10 起算且不回退: %+v", got)
	}
}

func TestRangeSubsetHelpers(t *testing.T) {
	r := Range{Topic: "traces.raw", Partition: 1, Start: 10, End: 20}
	if r.Empty() || !r.Contains(10) || !r.Contains(19) || r.Contains(20) {
		t.Fatalf("区间语义错: %+v", r)
	}
	if r.Label() != "traces.raw-1-10-20" {
		t.Errorf("label = %s", r.Label())
	}
}
