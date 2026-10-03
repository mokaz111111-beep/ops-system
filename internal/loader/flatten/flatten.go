// Package flatten 把一条 OTLP protobuf 导出请求展开成 Doris 扁平行。
//
// 身份列只来自 Kafka 消息头（调用方传入的 Identity）。payload 里的 tenant_id /
// cluster_id 只作为普通 resource 属性进入 VARIANT，绝不回填身份列——这是
// ingestmsg 选「头而不改写 payload」成立的前提（DD-001 §2.1 第 8 条）。
package flatten

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// Identity 是消息头还原出的身份。缺头的消息不应进入 Flatten。
type Identity struct {
	TenantID  identity.TenantID
	ProjectID identity.ProjectID
	ClusterID identity.ClusterID
}

// Options 控制展开。Coercer 为空时仍做 key 截断，不做类型强制。
type Options struct {
	LogAttrs  Coercer
	LogRes    Coercer
	SpanAttrs Coercer
	SpanRes   Coercer
}

// DefaultOptions 按 DD-002 当前主方案装上 Schema Template。A3 未结论，这是默认可替换实现。
func DefaultOptions() Options {
	return Options{
		LogAttrs:  SchemaCoercer{Paths: DefaultLogAttrSchema()},
		LogRes:    SchemaCoercer{Paths: DefaultResourceSchema()},
		SpanAttrs: SchemaCoercer{Paths: DefaultSpanAttrSchema()},
		SpanRes:   SchemaCoercer{Paths: DefaultResourceSchema()},
	}
}

// Result 是一条 Kafka 消息展开后的产物。
type Result struct {
	Rows          []map[string]any
	Encoded       []byte
	EncodedBytes  int64
	Conflicts     []Conflict
	TruncatedKeys int
}

// Flatten 按信号展开。metrics 不走本路径。
func Flatten(sig telemetry.Signal, payload []byte, id Identity, opt Options) (Result, error) {
	switch sig {
	case telemetry.SignalLogs:
		return flattenLogs(payload, id, opt)
	case telemetry.SignalTraces:
		return flattenSpans(payload, id, opt)
	default:
		return Result{}, fmt.Errorf("flatten: 信号 %s 不能展开进 Doris", sig)
	}
}

func flattenLogs(payload []byte, id Identity, opt Options) (Result, error) {
	var rows []map[string]any
	var conflicts []Conflict
	truncated := 0

	err := eachField(payload, func(field, wt int, raw []byte, u uint64) error {
		if field != 1 || wt != wireLen {
			return nil
		}
		return walkResourceLogs(raw, id, opt, &rows, &conflicts, &truncated)
	})
	if err != nil {
		return Result{}, err
	}
	return encodeRows(rows, conflicts, truncated)
}

func walkResourceLogs(buf []byte, id Identity, opt Options, rows *[]map[string]any, conflicts *[]Conflict, truncated *int) error {
	var res map[string]any
	return eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch {
		case field == 1 && wt == wireLen:
			m, err := decodeAttributes(raw)
			if err != nil {
				return err
			}
			res = m
		case field == 2 && wt == wireLen:
			return walkScopeLogs(raw, res, id, opt, rows, conflicts, truncated)
		}
		return nil
	})
}

func walkScopeLogs(buf []byte, res map[string]any, id Identity, opt Options, rows *[]map[string]any, conflicts *[]Conflict, truncated *int) error {
	return eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		if field != 2 || wt != wireLen {
			return nil
		}
		row, conf, cut, err := decodeLogRecord(raw, res, id, opt)
		if err != nil {
			return err
		}
		*rows = append(*rows, row)
		*conflicts = append(*conflicts, conf...)
		*truncated += cut
		return nil
	})
}

func decodeLogRecord(buf []byte, res map[string]any, id Identity, opt Options) (map[string]any, []Conflict, int, error) {
	var (
		tsNano, obsNano uint64
		severityText    string
		severityNum     int64
		body            any
		attrs           [][]byte
		traceID, spanID []byte
	)
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case 1: // time_unix_nano
			if wt == wireI64 {
				tsNano = u
			}
		case 2: // severity_number
			severityNum = int64(u)
		case 3: // severity_text
			severityText = string(raw)
		case 5: // body
			if wt == wireLen {
				v, err := decodeAny(raw)
				if err != nil {
					return err
				}
				body = v
			}
		case 6: // attributes
			if wt == wireLen {
				attrs = append(attrs, raw)
			}
		case 9:
			traceID = raw
		case 10:
			spanID = raw
		case 11: // observed_time_unix_nano
			if wt == wireI64 {
				obsNano = u
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, 0, err
	}

	attrMap, err := decodeAttrList(attrs)
	if err != nil {
		return nil, nil, 0, err
	}
	tenant := string(id.TenantID)
	logAttrs, conf1, cut1 := applyCoercer(attrMap, opt.LogAttrs, tenant)
	resource, conf2, cut2 := applyCoercer(cloneMap(res), opt.LogRes, tenant)

	ts := tsNano
	if ts == 0 {
		ts = obsNano
	}
	row := map[string]any{
		"ts":            formatTime(ts, 3),
		"tenant_id":     string(id.TenantID),
		"cluster_id":    string(id.ClusterID),
		"service":       stringAttr(res, "service.name"),
		"severity":      severityOf(severityText, severityNum),
		"trace_id":      hexID(traceID),
		"span_id":       hexID(spanID),
		"body":          bodyString(body),
		"log_attributes": logAttrs,
		"resource":      resource,
	}
	return row, append(conf1, conf2...), cut1 + cut2, nil
}

func flattenSpans(payload []byte, id Identity, opt Options) (Result, error) {
	var rows []map[string]any
	var conflicts []Conflict
	truncated := 0
	err := eachField(payload, func(field, wt int, raw []byte, u uint64) error {
		if field != 1 || wt != wireLen {
			return nil
		}
		return walkResourceSpans(raw, id, opt, &rows, &conflicts, &truncated)
	})
	if err != nil {
		return Result{}, err
	}
	return encodeRows(rows, conflicts, truncated)
}

func walkResourceSpans(buf []byte, id Identity, opt Options, rows *[]map[string]any, conflicts *[]Conflict, truncated *int) error {
	var res map[string]any
	return eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch {
		case field == 1 && wt == wireLen:
			m, err := decodeAttributes(raw)
			if err != nil {
				return err
			}
			res = m
		case field == 2 && wt == wireLen:
			return walkScopeSpans(raw, res, id, opt, rows, conflicts, truncated)
		}
		return nil
	})
}

func walkScopeSpans(buf []byte, res map[string]any, id Identity, opt Options, rows *[]map[string]any, conflicts *[]Conflict, truncated *int) error {
	var scopeName, scopeVer string
	return eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch {
		case field == 1 && wt == wireLen:
			name, ver, err := decodeScope(raw)
			if err != nil {
				return err
			}
			scopeName, scopeVer = name, ver
		case field == 2 && wt == wireLen:
			row, conf, cut, err := decodeSpan(raw, res, scopeName, scopeVer, id, opt)
			if err != nil {
				return err
			}
			*rows = append(*rows, row)
			*conflicts = append(*conflicts, conf...)
			*truncated += cut
		}
		return nil
	})
}

func decodeScope(buf []byte) (name, version string, err error) {
	err = eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case 1:
			name = string(raw)
		case 2:
			version = string(raw)
		}
		return nil
	})
	return name, version, err
}

func decodeSpan(buf []byte, res map[string]any, scopeName, scopeVer string, id Identity, opt Options) (map[string]any, []Conflict, int, error) {
	var (
		traceID, spanID, parent []byte
		name                    string
		kind                    int64
		start, end              uint64
		attrs                   [][]byte
		events                  []map[string]any
		links                   []map[string]any
		statusCode              int64
		statusMsg               string
	)
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case 1:
			traceID = raw
		case 2:
			spanID = raw
		case 4:
			parent = raw
		case 5:
			name = string(raw)
		case 6:
			kind = int64(u)
		case 7:
			if wt == wireI64 {
				start = u
			}
		case 8:
			if wt == wireI64 {
				end = u
			}
		case 9:
			if wt == wireLen {
				attrs = append(attrs, raw)
			}
		case 11:
			if wt == wireLen {
				ev, err := decodeEvent(raw)
				if err != nil {
					return err
				}
				events = append(events, ev)
			}
		case 13:
			if wt == wireLen {
				lk, err := decodeLink(raw)
				if err != nil {
					return err
				}
				links = append(links, lk)
			}
		case 15:
			if wt == wireLen {
				c, m, err := decodeStatus(raw)
				if err != nil {
					return err
				}
				statusCode, statusMsg = c, m
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, 0, err
	}

	attrMap, err := decodeAttrList(attrs)
	if err != nil {
		return nil, nil, 0, err
	}
	tenant := string(id.TenantID)
	spanAttrs, conf1, cut1 := applyCoercer(attrMap, opt.SpanAttrs, tenant)
	resAttrs, conf2, cut2 := applyCoercer(cloneMap(res), opt.SpanRes, tenant)

	svc := stringAttr(res, "service.name")
	if svc == "" {
		svc = "unknown"
	}
	var duration any
	if end >= start && start > 0 {
		duration = int64((end - start) / 1000)
	}
	row := map[string]any{
		"ts":                  formatTime(start, 6),
		"tenant_id":           string(id.TenantID),
		"cluster_id":          string(id.ClusterID),
		"service_name":        svc,
		"service_instance_id": stringAttr(res, "service.instance.id"),
		"trace_id":            hexID(traceID),
		"span_id":             hexID(spanID),
		"parent_span_id":      hexID(parent),
		"span_name":           name,
		"span_kind":           spanKind(kind),
		"end_time":            formatTime(end, 6),
		"duration":            duration,
		"status_code":         statusCodeName(statusCode),
		"status_message":      statusMsg,
		"span_attributes":     spanAttrs,
		"resource_attributes": resAttrs,
		"events":              events,
		"links":               links,
		"scope_name":          scopeName,
		"scope_version":       scopeVer,
	}
	return row, append(conf1, conf2...), cut1 + cut2, nil
}

func decodeEvent(buf []byte) (map[string]any, error) {
	var ts uint64
	var name string
	var attrs [][]byte
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case 1:
			if wt == wireI64 {
				ts = u
			}
		case 2:
			name = string(raw)
		case 3:
			if wt == wireLen {
				attrs = append(attrs, raw)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	am, err := decodeAttrList(attrs)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"timestamp":  formatTime(ts, 6),
		"name":       name,
		"attributes": stringifyMap(am),
	}, nil
}

func decodeLink(buf []byte) (map[string]any, error) {
	var traceID, spanID []byte
	var state string
	var attrs [][]byte
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case 1:
			traceID = raw
		case 2:
			spanID = raw
		case 3:
			state = string(raw)
		case 4:
			if wt == wireLen {
				attrs = append(attrs, raw)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	am, err := decodeAttrList(attrs)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"trace_id":    hexID(traceID),
		"span_id":     hexID(spanID),
		"trace_state": state,
		"attributes":  stringifyMap(am),
	}, nil
}

func decodeStatus(buf []byte) (int64, string, error) {
	var code int64
	var msg string
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case 2:
			msg = string(raw)
		case 3:
			code = int64(u)
		}
		return nil
	})
	return code, msg, err
}

func encodeRows(rows []map[string]any, conflicts []Conflict, truncated int) (Result, error) {
	if rows == nil {
		rows = []map[string]any{}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Rows:          rows,
		Encoded:       raw,
		EncodedBytes:  int64(len(raw)),
		Conflicts:     conflicts,
		TruncatedKeys: truncated,
	}, nil
}

func formatTime(unixNano uint64, prec int) string {
	if unixNano == 0 {
		return ""
	}
	t := time.Unix(0, int64(unixNano)).UTC()
	switch prec {
	case 6:
		return t.Format("2006-01-02 15:04:05.000000")
	default:
		return t.Format("2006-01-02 15:04:05.000")
	}
}

func severityOf(text string, num int64) string {
	if text != "" {
		return text
	}
	switch num {
	case 1, 2, 3, 4:
		return "TRACE"
	case 5, 6, 7, 8:
		return "DEBUG"
	case 9, 10, 11, 12:
		return "INFO"
	case 13, 14, 15, 16:
		return "WARN"
	case 17, 18, 19, 20:
		return "ERROR"
	case 21, 22, 23, 24:
		return "FATAL"
	default:
		return ""
	}
}

func bodyString(v any) any {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func spanKind(k int64) string {
	switch k {
	case 1:
		return "Internal"
	case 2:
		return "Server"
	case 3:
		return "Client"
	case 4:
		return "Producer"
	case 5:
		return "Consumer"
	default:
		return "Unspecified"
	}
}

func statusCodeName(c int64) string {
	switch c {
	case 1:
		return "Ok"
	case 2:
		return "Error"
	default:
		return "Unset"
	}
}

func stringifyMap(in map[string]any) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = rawString(v)
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
