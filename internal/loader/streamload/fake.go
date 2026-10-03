package streamload

import (
	"context"
	"sync"

	"github.com/mokaz/ops-system/pkg/loadbatch"
)

// LoadCall 是 Fake 记录的一次提交。
type LoadCall struct {
	Request Request
	Result  Result
}

// Fake 是内存 Client，供测试与无 Doris 环境使用。
//
// 默认语义：同一 label 第二次提交返回 OutcomeDuplicate，不改 Body。这把会签
// 「禁止换 label 重打同一批」落成可断言的行为——测试若换了 label 再交，Fake
// 会当成两个不同的作业，重复行就会出现在 Loads 里。
type Fake struct {
	mu     sync.Mutex
	seen   map[string]Result
	calls  []LoadCall
	closed bool

	// Next 非 nil 时覆盖下一次（或按 Hook 判定的）结果。用完即清。
	Next *Result
	// Hook 在记录前被调用。返回的 Result.Outcome 若非零值以外的自定义，原样采用；
	// 返回 error 则本次失败且不记 seen。
	Hook func(Request) (Result, error)
}

// NewFake 创建内存 Client。
func NewFake() *Fake {
	return &Fake{seen: make(map[string]Result)}
}

func (f *Fake) Load(ctx context.Context, req Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{Outcome: OutcomeRetry, Message: err.Error()}, err
	}
	if err := Validate(req); err != nil {
		return Result{Outcome: OutcomeFatal, Message: err.Error()}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return Result{Outcome: OutcomeRetry, Message: "Fake 已关闭"}, context.Canceled
	}

	label := req.Range.Label()
	if f.Hook != nil {
		res, err := f.Hook(req)
		if err != nil {
			return res, err
		}
		if res.Label == "" {
			res.Label = label
		}
		f.record(req, res)
		if res.Outcome.Succeeded() {
			f.seen[label] = res
		}
		return res, nil
	}
	if f.Next != nil {
		res := *f.Next
		f.Next = nil
		if res.Label == "" {
			res.Label = label
		}
		f.record(req, res)
		if res.Outcome.Succeeded() {
			f.seen[label] = res
		}
		return res, nil
	}

	if prev, ok := f.seen[label]; ok {
		res := Result{
			Outcome: OutcomeDuplicate,
			Status:  "Label Already Exists",
			Message: "label already exists",
			Label:   label,
			TxnID:   prev.TxnID,
		}
		f.record(req, res)
		return res, nil
	}

	res := Result{
		Outcome: OutcomeOK,
		Status:  "Success",
		Message: "OK",
		Label:   label,
		Loaded:  1,
		Total:   1,
		TxnID:   int64(len(f.seen) + 1),
	}
	f.seen[label] = res
	f.record(req, res)
	return res, nil
}

func (f *Fake) record(req Request, res Result) {
	f.calls = append(f.calls, LoadCall{Request: req, Result: res})
}

func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Calls 返回提交快照。
func (f *Fake) Calls() []LoadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]LoadCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// Labels 按提交顺序列出 label。
func (f *Fake) Labels() []string {
	calls := f.Calls()
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.Request.Range.Label())
	}
	return out
}

// Succeeded 报告该区间是否已被视为成功（含 duplicate）。
func (f *Fake) Succeeded(r loadbatch.Range) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.seen[r.Label()]
	return ok
}
