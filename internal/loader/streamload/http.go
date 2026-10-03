package streamload

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPConfig 是对接 Doris FE 的参数。密码不得写进仓库里的示例文件。
type HTTPConfig struct {
	FE          string
	Database    string
	User        string
	Password    string
	TableLogs   string
	TableSpans  string
	HTTPTimeout time.Duration
	// Client 非空时覆盖默认 http.Client，供测试注入 httptest.Server。
	Client *http.Client
}

const defaultHTTPTimeout = 60 * time.Second

// HTTP 是面向 Doris FE 的 Stream Load 客户端。
type HTTP struct {
	cfg    HTTPConfig
	client *http.Client
}

// NewHTTP 创建客户端。FE 不可达不在此时失败——与网关不在启动期依赖 Kafka 同一理由：
// 一次 FE 抖动不得放大成 loader 进程起不来；失败发生在 Load，按 OutcomeRetry 回吐。
func NewHTTP(cfg HTTPConfig) (*HTTP, error) {
	if strings.TrimSpace(cfg.FE) == "" {
		return nil, fmt.Errorf("streamload: 未配置 FE 地址")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("streamload: 未配置 database")
	}
	timeout := cfg.HTTPTimeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	cl := cfg.Client
	if cl == nil {
		cl = &http.Client{Timeout: timeout}
	}
	return &HTTP{cfg: cfg, client: cl}, nil
}

type dorisResp struct {
	TxnID             int64  `json:"TxnId"`
	Label             string `json:"Label"`
	Status            string `json:"Status"`
	Message           string `json:"Message"`
	ExistingJobStatus string `json:"ExistingJobStatus"`
	NumberTotalRows   int64  `json:"NumberTotalRows"`
	NumberLoadedRows  int64  `json:"NumberLoadedRows"`
	ErrorURL          string `json:"ErrorURL"`
}

func (h *HTTP) Load(ctx context.Context, req Request) (Result, error) {
	if err := Validate(req); err != nil {
		return Result{Outcome: OutcomeFatal, Message: err.Error()}, err
	}
	table := req.Table
	if table == "" {
		var err error
		table, err = TableFor(req.Signal, h.cfg.TableLogs, h.cfg.TableSpans)
		if err != nil {
			return Result{Outcome: OutcomeFatal, Message: err.Error()}, err
		}
	}

	url := strings.TrimRight(h.cfg.FE, "/") + "/api/" + h.cfg.Database + "/" + table + "/_stream_load"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(req.Body))
	if err != nil {
		return Result{Outcome: OutcomeRetry, Message: err.Error()}, err
	}

	label := req.Range.Label()
	httpReq.Header.Set("Expect", "100-continue")
	httpReq.Header.Set("format", "json")
	httpReq.Header.Set("strip_outer_array", "true")
	httpReq.Header.Set("load_to_single_tablet", "true")
	httpReq.Header.Set("label", label)
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Timeout > 0 {
		httpReq.Header.Set("timeout", fmt.Sprintf("%d", req.Timeout))
	}
	if h.cfg.User != "" {
		token := base64.StdEncoding.EncodeToString([]byte(h.cfg.User + ":" + h.cfg.Password))
		httpReq.Header.Set("Authorization", "Basic "+token)
	}

	resp, err := h.client.Do(httpReq)
	if err != nil {
		return Result{Outcome: OutcomeRetry, Label: label, Message: err.Error()}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Result{Outcome: OutcomeRetry, Label: label, HTTP: resp.StatusCode, Message: err.Error()}, err
	}

	var parsed dorisResp
	_ = json.Unmarshal(raw, &parsed)
	if parsed.Label == "" {
		parsed.Label = label
	}
	if parsed.Status == "" && resp.StatusCode >= 500 {
		return Result{
			Outcome: OutcomeRetry,
			Label:   label,
			HTTP:    resp.StatusCode,
			Message: string(raw),
		}, nil
	}

	out := Result{
		Outcome: ClassifyStatus(parsed.Status, parsed.Message+" "+parsed.ExistingJobStatus),
		Status:  parsed.Status,
		Message: parsed.Message,
		Label:   parsed.Label,
		TxnID:   parsed.TxnID,
		Loaded:  parsed.NumberLoadedRows,
		Total:   parsed.NumberTotalRows,
		HTTP:    resp.StatusCode,
	}
	if parsed.ErrorURL != "" && out.Message == "" {
		out.Message = parsed.ErrorURL
	}
	return out, nil
}

func (h *HTTP) Close() error { return nil }
