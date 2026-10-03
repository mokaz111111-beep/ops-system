package flatten

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// DeclaredType 是 Schema Template 上的声明类型（DD-002 DDL）。
type DeclaredType string

const (
	TypeInt    DeclaredType = "INT"
	TypeBigInt DeclaredType = "BIGINT"
	TypeString DeclaredType = "STRING"
)

// Conflict 是一次类型强制失败。指标标签按 DD-001：tenant / path / declared_type。
type Conflict struct {
	Tenant   string
	Path     string
	Declared DeclaredType
}

// CoerceResult 是单条路径的强制结果。
type CoerceResult struct {
	WriteMain bool
	Value     any
	Shadow    any
	Conflict  bool
	Declared  DeclaredType
}

// Coercer 是 loader 侧类型强制的可插拔接口。
//
// A3（Q2-1 跨租户类型冲突实测）还没做。本接口是给 A3 留的替换点：实验若结论是
// 「必须放弃 RANDOM 分桶」或「强制规则要改」，换实现即可，不必改 flatten 调用方。
// 默认实现按 DD-002 当前主方案，不假装 A3 已拍板。
type Coercer interface {
	Declared(path string) (DeclaredType, bool)
	Coerce(path string, value any) CoerceResult
}

// SchemaCoercer 按 Schema Template 声明类型转换；失败不写主路径，旁路到 path__raw。
type SchemaCoercer struct {
	Paths map[string]DeclaredType
}

// DefaultLogAttrSchema 是 logs.log_attributes 的 typed path（DD-002 §3.2）。
func DefaultLogAttrSchema() map[string]DeclaredType {
	return map[string]DeclaredType{
		"http.status_code":  TypeInt,
		"http.method":       TypeString,
		"http.route":        TypeString,
		"url.path":          TypeString,
		"db.system":         TypeString,
		"db.statement":      TypeString,
		"rpc.service":       TypeString,
		"rpc.method":        TypeString,
		"error.type":        TypeString,
		"exception.type":    TypeString,
		"exception.message": TypeString,
	}
}

// DefaultResourceSchema 是 logs.resource / spans.resource_attributes 的 typed path。
func DefaultResourceSchema() map[string]DeclaredType {
	return map[string]DeclaredType{
		"k8s.namespace.name":  TypeString,
		"k8s.deployment.name": TypeString,
		"k8s.node.name":       TypeString,
		"host.name":           TypeString,
	}
}

// DefaultSpanAttrSchema 是 spans.span_attributes 的 typed path（DD-002 §3.4）。
func DefaultSpanAttrSchema() map[string]DeclaredType {
	return map[string]DeclaredType{
		"http.status_code":           TypeInt,
		"http.method":                TypeString,
		"http.route":                 TypeString,
		"url.full":                   TypeString,
		"db.system":                  TypeString,
		"db.statement":               TypeString,
		"rpc.system":                 TypeString,
		"rpc.service":                TypeString,
		"rpc.method":                 TypeString,
		"messaging.system":           TypeString,
		"exception.type":             TypeString,
		"gen_ai.system":              TypeString,
		"gen_ai.request.model":       TypeString,
		"gen_ai.usage.input_tokens":  TypeBigInt,
		"gen_ai.usage.output_tokens": TypeBigInt,
	}
}

func (c SchemaCoercer) Declared(path string) (DeclaredType, bool) {
	t, ok := c.Paths[path]
	return t, ok
}

func (c SchemaCoercer) Coerce(path string, value any) CoerceResult {
	declared, ok := c.Paths[path]
	if !ok {
		return CoerceResult{WriteMain: true, Value: value}
	}
	v, err := convert(declared, value)
	if err != nil {
		return CoerceResult{
			WriteMain: false,
			Shadow:    rawString(value),
			Conflict:  true,
			Declared:  declared,
		}
	}
	return CoerceResult{WriteMain: true, Value: v, Declared: declared}
}

func convert(t DeclaredType, value any) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("nil")
	}
	switch t {
	case TypeInt:
		n, err := asInt64(value)
		if err != nil {
			return nil, err
		}
		if n < int64(-1<<31) || n > int64(1<<31-1) {
			return nil, fmt.Errorf("int overflow")
		}
		return int32(n), nil
	case TypeBigInt:
		return asInt64(value)
	case TypeString:
		return asString(value)
	default:
		return nil, fmt.Errorf("未知声明类型 %s", t)
	}
}

func asInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		if v != float64(int64(v)) {
			return 0, fmt.Errorf("非整数 float")
		}
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case string:
		s := strings.TrimSpace(v)
		return strconv.ParseInt(s, 10, 64)
	case bool:
		return 0, fmt.Errorf("bool→int")
	default:
		return 0, fmt.Errorf("无法转为整数")
	}
}

func asString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int:
		return strconv.Itoa(v), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

func rawString(value any) string {
	s, err := asString(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return s
}

// applyCoercer 对 VARIANT 对象做类型强制与 key 截断。
func applyCoercer(in map[string]any, c Coercer, tenant string) (map[string]any, []Conflict, int) {
	if in == nil {
		return nil, nil, 0
	}
	out := make(map[string]any, len(in))
	var conflicts []Conflict
	truncated := 0
	for k, v := range in {
		key, cut := truncateKey(k)
		if cut {
			truncated++
		}
		if c == nil {
			out[key] = v
			continue
		}
		r := c.Coerce(key, v)
		if r.WriteMain {
			out[key] = r.Value
		}
		if r.Shadow != nil {
			out[key+"__raw"] = r.Shadow
		}
		if r.Conflict {
			conflicts = append(conflicts, Conflict{Tenant: tenant, Path: key, Declared: r.Declared})
		}
	}
	return out, conflicts, truncated
}
