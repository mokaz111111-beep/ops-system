// Package streamload 实现 IF-3 的 Doris Stream Load 提交与幂等语义。
//
// # 接口 + HTTP + Fake
//
// Client 是接口。HTTP 实现对接真 Doris；Fake 让 `go test ./...` 在没有 FE 的
// 机器上仍能验证「label already exists 视为成功、禁止换 label 重打」。这与
// kafkasink 的 Producer / Kafka / Fake 同一结构：没有下游不得变成编译失败或
// 测试红。
//
// # 会签钉死的两条
//
//  1. Status=Label Already Exists 一律视为成功，调用方 commit 到该 label 的 end。
//     超时后重试必须带同一个 label，不得改 end 再交（DD-001 §2.1 第 4 条）。
//  2. label 声称的 [start, end) 必须 ⊆ 实际写入行。本包不切批，只拒绝空 Body——
//     空 Body 配上非空 label 会制造「声称写了、实际没写」的静默缺口。
package streamload

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mokaz/ops-system/pkg/loadbatch"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// Outcome 是一次提交对 consumer 而言的处置。
type Outcome uint8

const (
	// OutcomeOK 写入成功，或 Publish Timeout（数据会被可见）。
	OutcomeOK Outcome = iota
	// OutcomeDuplicate 同名 label 已存在。按会签视为成功。
	OutcomeDuplicate
	// OutcomeRetry -235 / -238 / 内存超限 / 超时 / 5xx。不 commit，不改 label。
	OutcomeRetry
	// OutcomeFatal 数据或参数错误，重试同一 Body 不会变好。仍不改 label。
	OutcomeFatal
)

func (o Outcome) String() string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeRetry:
		return "retry"
	case OutcomeFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// Succeeded 报告调用方是否应当 commit Kafka offset 到本次 label 的 end。
func (o Outcome) Succeeded() bool {
	return o == OutcomeOK || o == OutcomeDuplicate
}

// Request 是一次 Stream Load。
type Request struct {
	Range   loadbatch.Range
	Signal  telemetry.Signal
	Body    []byte
	Table   string
	Timeout int // 秒；零值由实现给默认
}

// Result 是 Doris 的分类结果。
type Result struct {
	Outcome Outcome
	Status  string
	Message string
	Label   string
	TxnID   int64
	Loaded  int64
	Total   int64
	HTTP    int
}

// ErrEmptyBody 表示空批次不得提交：那会让 label 覆盖一个没写入的区间。
var ErrEmptyBody = errors.New("streamload: 空 Body 不得提交——label 范围必须 ⊆ 实际写入行")

// ErrEmptyLabel 表示缺少 IF-3 label。
var ErrEmptyLabel = errors.New("streamload: 缺少 Stream Load label")

// Client 提交一批扁平行。实现必须是并发安全的。
type Client interface {
	Load(ctx context.Context, req Request) (Result, error)
	Close() error
}

// Validate 检查提交纪律里本包能强制的部分。
func Validate(req Request) error {
	if req.Range.Empty() {
		return fmt.Errorf("%w: %s", ErrEmptyLabel, req.Range.Label())
	}
	if strings.TrimSpace(req.Range.Label()) == "" {
		return ErrEmptyLabel
	}
	if len(req.Body) == 0 {
		return ErrEmptyBody
	}
	return nil
}

// ClassifyStatus 把 Doris JSON 的 Status / Message 映射为 Outcome。
//
// Label Already Exists 不论 ExistingJobStatus 都视为成功——会签原文没有给「还在
// PREPARE 就当失败」的例外。换 label 重打才是禁止项。
func ClassifyStatus(status, message string) Outcome {
	s := strings.ToLower(strings.TrimSpace(status))
	switch s {
	case "success", "publish timeout":
		return OutcomeOK
	case "label already exists":
		return OutcomeDuplicate
	}

	msg := strings.ToLower(message)
	if strings.Contains(msg, "label already exists") {
		return OutcomeDuplicate
	}
	if isRetryableMessage(msg) {
		return OutcomeRetry
	}
	if s == "fail" || s == "failed" {
		return OutcomeFatal
	}
	// 未知状态默认可重试：判成 Fatal 等于让 consumer 卡死或丢数。
	return OutcomeRetry
}

func isRetryableMessage(msg string) bool {
	needles := []string{
		"-235", "too many tablet versions",
		"-238",
		"memory", "mem exceeded", "mem_limit",
		"timeout",
	}
	for _, n := range needles {
		if strings.Contains(msg, n) {
			return true
		}
	}
	return false
}

// TableFor 按信号选表。metrics 不走本路径。
func TableFor(sig telemetry.Signal, logs, spans string) (string, error) {
	switch sig {
	case telemetry.SignalLogs:
		if logs == "" {
			return "", errors.New("streamload: 未配置 logs 表")
		}
		return logs, nil
	case telemetry.SignalTraces:
		if spans == "" {
			return "", errors.New("streamload: 未配置 spans 表")
		}
		return spans, nil
	default:
		return "", fmt.Errorf("streamload: 信号 %s 不走 Stream Load", sig)
	}
}
