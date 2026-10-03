// Package otlpwire 在 protobuf 线格式上直接提取摄入网关需要的少量信息，
// 不做完整的 OTLP 反序列化。
//
// # 为什么不完整解析
//
// 网关在 M1 阶段只需要四件事：记录条数（按 events/s 限流）、字节数（按 bytes/s 限流）、
// service.name（IF-2 规定 logs.raw 的 partition key 为 tenant_id + service）、
// 以及一个事件时间戳（§8.1 第一段"Collector 攒批"只能由网关用客户端时间戳代理观测）。
// 真正的展开与字段处理在下游 otlp-loader（DD-001 §3.4）。
//
// 在 10.5 万 EPS 峰值下，为了拿这几个数而把整个 OTLP 对象图反序列化一遍是纯粹的 CPU
// 浪费——而 §8.1 只给了摄入网关 0.5 秒的延迟预算。线级扫描只走一遍字节、不分配对象。
//
// # 一次遍历拿齐
//
// Scan 是网关实际使用的入口：它在同一次遍历里取齐上述信息。CountRecords 与 ServiceName
// 保留为单一用途的便捷函数，各自只做自己需要的那部分工作，不为对方付代价。
//
// # M2 的接缝
//
// PII 脱敏（X-002 §2.2）必须在摄入网关执行且早于 Kafka，那一步需要完整解码。
// 届时本包的角色会退化为"快速路径"：未开启脱敏的租户继续走线级扫描，开启的租户走
// 完整解码。X-002 §2.2.3 要求实测开启脱敏后的吞吐衰减，衰减的来源正是这个差异。
//
// # 结构同构
//
// 三类信号的顶层嵌套完全相同，因此一套计数逻辑覆盖三者：
//
//	Export*ServiceRequest.field1  →  Resource{Logs,Spans,Metrics}
//	  Resource*.field2            →  Scope{Logs,Spans,Metrics}
//	    Scope*.field2             →  {LogRecord, Span, Metric}   ← 计这一层的出现次数
//
// 同构只到记录这一层为止：记录内部的时间戳字段号三者不同（见 eventNanos）。
//
// 本包解析的是不可信输入，所有函数对任意字节串都必须返回错误而不是 panic。
package otlpwire

import (
	"encoding/binary"
	"errors"

	"github.com/mokaz/ops-system/pkg/telemetry"
)

// protobuf 线类型。
const (
	wireVarint = 0
	wireI64    = 1
	wireLen    = 2
	wireSGroup = 3
	wireEGroup = 4
	wireI32    = 5
)

var (
	// ErrMalformed 表示字节串不是合法的 protobuf 编码。对应 IF-1 的 E3。
	ErrMalformed = errors.New("otlpwire: protobuf 编码非法")
	// ErrOverflow 表示 varint 超过 64 位。
	ErrOverflow = errors.New("otlpwire: varint 溢出")
)

// attrServiceName 是 OTel 语义约定中的服务名属性键。
const attrServiceName = "service.name"

// 客户端不得自报的身份属性键。
//
// 这些键由网关按 Token 反查后强制注入（SD-000 §7.1），客户端在 resource attributes 里
// 写什么都不会被采用。扫描它们不是为了"过滤"——身份走消息头、payload 内的同名属性对
// 下游本就无效（见 pkg/ingestmsg 的包注释）——而是为了让"有租户在自报租户 ID"这件事
// 可观测：它通常意味着客户照着自建部署的文档接入，是一次该推治理事件的接入错误。
const (
	attrTenantID     = "tenant_id"
	attrProjectID    = "project_id"
	attrClusterID    = "cluster_id"
	attrK8sClusterID = "k8s.cluster.id"
)

// SelfReported 记录客户端自报了哪些身份属性。
type SelfReported struct {
	TenantID  bool
	ProjectID bool
	ClusterID bool
}

// Any 报告是否自报了任一身份属性。
func (s SelfReported) Any() bool { return s.TenantID || s.ProjectID || s.ClusterID }

// Summary 是一次线级扫描的结果。
type Summary struct {
	// RecordCount 是 LogRecord / Span / Metric 的条数。
	RecordCount int
	// ServiceName 是首个 resource 的 service.name，可能为空。
	ServiceName string
	// EventUnixNano 是首条记录的事件时间戳，0 表示未取到（信号为 metrics、
	// 记录未带时间戳、或该字段编码不符合预期）。
	//
	// 刻意只取首条而不求全批最小值：求最小值要走进每一条记录，在 10.5 万 EPS 下为一个
	// 分布型指标付这个代价不值得。首条记录的时间戳对"Collector 攒批了多久"已足够有代表性。
	EventUnixNano uint64
	// SelfReported 见 SelfReported 的说明。
	SelfReported SelfReported
}

// scanOpts 控制遍历深度。按需开启是为了让三个入口各自只付自己的成本。
type scanOpts struct {
	service    bool
	timestamps bool
}

// Scan 一次遍历取齐网关所需的全部线级信息。
//
// sig 只影响时间戳提取；记录计数与 service.name 对三类信号同构。
func Scan(buf []byte, sig telemetry.Signal) (Summary, error) {
	return scan(buf, sig, scanOpts{service: true, timestamps: true})
}

// CountRecords 返回该导出请求中的记录条数（LogRecord / Span / Metric）。
//
// 入参可以是 ExportLogsServiceRequest、ExportTraceServiceRequest 或
// ExportMetricsServiceRequest 的任一编码——三者同构（见包注释）。
func CountRecords(buf []byte) (int, error) {
	s, err := scan(buf, telemetry.SignalUnknown, scanOpts{})
	return s.RecordCount, err
}

// ServiceName 返回首个 resource 上的 service.name，用于 IF-2 的 partition key。
//
// 返回空串表示未找到。网关不因缺失 service.name 而拒绝数据——那是埋点质量问题，
// 应由治理事件推动，不应表现为摄入失败。
func ServiceName(buf []byte) (string, error) {
	s, err := scan(buf, telemetry.SignalUnknown, scanOpts{service: true})
	return s.ServiceName, err
}

func scan(buf []byte, sig telemetry.Signal, opt scanOpts) (Summary, error) {
	var out Summary
	// 深度 0：顶层的 resource_* 列表。
	err := eachField(buf, func(field int, wt int, val []byte) error {
		if field != 1 || wt != wireLen {
			return nil
		}
		return eachField(val, func(field int, wt int, val []byte) error {
			switch {
			case field == 1 && wt == wireLen && opt.service:
				// Resource*.field1 = Resource
				return scanResource(val, &out)
			case field == 2 && wt == wireLen:
				// 深度 1：resource_* 内的 scope_* 列表。
				return eachField(val, func(field int, wt int, val []byte) error {
					if field != 2 || wt != wireLen {
						return nil
					}
					// 深度 2：scope_* 内的记录列表。计数即止，只在需要时间戳且尚未取到时
					// 进入记录内部。
					out.RecordCount++
					if opt.timestamps && out.EventUnixNano == 0 {
						ns, err := eventNanos(val, sig)
						if err != nil {
							return err
						}
						out.EventUnixNano = ns
					}
					return nil
				})
			}
			return nil
		})
	})
	if err != nil {
		return Summary{}, err
	}
	return out, nil
}

// scanResource 在一个 Resource 上取 service.name 并检出自报的身份属性。
func scanResource(buf []byte, out *Summary) error {
	return eachField(buf, func(field int, wt int, val []byte) error {
		if field != 1 || wt != wireLen { // Resource.field1 = repeated KeyValue
			return nil
		}
		key, value, err := keyValue(val)
		if err != nil {
			return err
		}
		switch key {
		case attrServiceName:
			if out.ServiceName == "" {
				name, err := stringValue(value)
				if err != nil {
					return err
				}
				out.ServiceName = name
			}
		case attrTenantID:
			out.SelfReported.TenantID = true
		case attrProjectID:
			out.SelfReported.ProjectID = true
		case attrClusterID, attrK8sClusterID:
			out.SelfReported.ClusterID = true
		}
		return nil
	})
}

// eventNanos 取一条记录的事件时间戳。
//
// 字段号来自 OTLP proto 定义，三类信号各不相同：
//
//	LogRecord.time_unix_nano          = 1（缺失时退回 observed_time_unix_nano = 11）
//	Span.start_time_unix_nano         = 7
//	Metric                            数据点时间戳埋在 Gauge/Sum/... 的 data_points 里，
//	                                  层级随类型变化，为一个代理指标走到那一层不值得
//
// 字段号是协议的一部分而非实现细节，变更只会发生在 OTLP 的大版本上。
func eventNanos(record []byte, sig telemetry.Signal) (uint64, error) {
	var want, fallback int
	switch sig {
	case telemetry.SignalLogs:
		want, fallback = 1, 11
	case telemetry.SignalTraces:
		want, fallback = 7, 0
	default:
		return 0, nil
	}

	var got, alt uint64
	err := eachField(record, func(field int, wt int, val []byte) error {
		if wt != wireI64 || len(val) != 8 {
			return nil
		}
		switch field {
		case want:
			got = binary.LittleEndian.Uint64(val)
		case fallback:
			alt = binary.LittleEndian.Uint64(val)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if got != 0 {
		return got, nil
	}
	return alt, nil
}

// keyValue 拆出一条 KeyValue 的键与 AnyValue 载荷。
func keyValue(buf []byte) (key string, anyValue []byte, err error) {
	err = eachField(buf, func(field int, wt int, val []byte) error {
		if wt != wireLen {
			return nil
		}
		switch field {
		case 1: // KeyValue.key
			key = string(val)
		case 2: // KeyValue.value = AnyValue
			anyValue = val
		}
		return nil
	})
	return key, anyValue, err
}

// stringValue 取 AnyValue 的 string_value，非字符串类型返回空串。
func stringValue(anyValue []byte) (string, error) {
	if anyValue == nil {
		return "", nil
	}
	out := ""
	err := eachField(anyValue, func(field int, wt int, val []byte) error {
		if field == 1 && wt == wireLen { // AnyValue.string_value
			out = string(val)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

// eachField 遍历一条 protobuf 消息的顶层字段。
//
// 对 LEN / I64 / I32 型字段，val 是其原始载荷；对 VARINT 型，val 为 nil
// （调用方目前不需要 varint 标量值）。回调返回非 nil 错误即中止遍历。
func eachField(buf []byte, fn func(field int, wireType int, val []byte) error) error {
	for len(buf) > 0 {
		tag, n, err := readVarint(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]

		field := int(tag >> 3)
		wt := int(tag & 7)
		if field <= 0 {
			return ErrMalformed
		}

		switch wt {
		case wireVarint:
			_, n, err := readVarint(buf)
			if err != nil {
				return err
			}
			buf = buf[n:]
			if err := fn(field, wt, nil); err != nil {
				return err
			}

		case wireI64:
			if len(buf) < 8 {
				return ErrMalformed
			}
			val := buf[:8]
			buf = buf[8:]
			if err := fn(field, wt, val); err != nil {
				return err
			}

		case wireI32:
			if len(buf) < 4 {
				return ErrMalformed
			}
			val := buf[:4]
			buf = buf[4:]
			if err := fn(field, wt, val); err != nil {
				return err
			}

		case wireLen:
			size, n, err := readVarint(buf)
			if err != nil {
				return err
			}
			buf = buf[n:]
			// 同时防两件事：长度超过剩余字节（截断或伪造），以及 size 转 int 溢出。
			if size > uint64(len(buf)) {
				return ErrMalformed
			}
			val := buf[:size]
			buf = buf[size:]
			if err := fn(field, wt, val); err != nil {
				return err
			}

		case wireSGroup, wireEGroup:
			// proto2 的 group 已废弃，OTLP 不使用。遇到即判非法，避免为此实现栈式跳过。
			return ErrMalformed

		default:
			return ErrMalformed
		}
	}
	return nil
}

// readVarint 读取一个 base-128 varint，返回值与消耗的字节数。
func readVarint(buf []byte) (uint64, int, error) {
	var v uint64
	for i := 0; i < len(buf); i++ {
		b := buf[i]
		if i == 9 && b > 1 {
			// 第 10 个字节只能贡献 1 位，否则超过 64 位。
			return 0, 0, ErrOverflow
		}
		v |= uint64(b&0x7f) << (7 * uint(i))
		if b < 0x80 {
			return v, i + 1, nil
		}
		if i == 9 {
			return 0, 0, ErrOverflow
		}
	}
	// 字节用尽但延续位仍为 1：截断。
	return 0, 0, ErrMalformed
}
