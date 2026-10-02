// Package otlpwire 在 protobuf 线格式上直接提取摄入网关需要的少量信息，
// 不做完整的 OTLP 反序列化。
//
// # 为什么不完整解析
//
// 网关在 M1 阶段只需要三件事：记录条数（按 events/s 限流）、字节数（按 bytes/s 限流）、
// service.name（IF-2 规定 logs.raw 的 partition key 为 tenant_id + service）。
// 真正的展开与字段处理在下游 otlp-loader（DD-001 §3.4）。
//
// 在 10.5 万 EPS 峰值下，为了拿三个数而把整个 OTLP 对象图反序列化一遍是纯粹的 CPU
// 浪费——而 §8.1 只给了摄入网关 0.5 秒的延迟预算。线级扫描只走一遍字节、不分配对象。
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
// 本包解析的是不可信输入，所有函数对任意字节串都必须返回错误而不是 panic。
package otlpwire

import "errors"

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

// CountRecords 返回该导出请求中的记录条数（LogRecord / Span / Metric）。
//
// 入参可以是 ExportLogsServiceRequest、ExportTraceServiceRequest 或
// ExportMetricsServiceRequest 的任一编码——三者同构（见包注释）。
func CountRecords(buf []byte) (int, error) {
	total := 0
	// 深度 0：顶层的 resource_* 列表。
	err := eachField(buf, func(field int, wt int, val []byte) error {
		if field != 1 || wt != wireLen {
			return nil
		}
		// 深度 1：resource_* 内的 scope_* 列表。
		return eachField(val, func(field int, wt int, val []byte) error {
			if field != 2 || wt != wireLen {
				return nil
			}
			// 深度 2：scope_* 内的记录列表，计数即止，不进入记录内部。
			return eachField(val, func(field int, wt int, _ []byte) error {
				if field == 2 && wt == wireLen {
					total++
				}
				return nil
			})
		})
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// ServiceName 返回首个 resource 上的 service.name，用于 IF-2 的 partition key。
//
// 返回空串表示未找到。网关不因缺失 service.name 而拒绝数据——那是埋点质量问题，
// 应由治理事件推动，不应表现为摄入失败。
func ServiceName(buf []byte) (string, error) {
	found := ""
	err := eachField(buf, func(field int, wt int, val []byte) error {
		if field != 1 || wt != wireLen || found != "" {
			return nil
		}
		// resource_*.field1 = Resource
		return eachField(val, func(field int, wt int, val []byte) error {
			if field != 1 || wt != wireLen || found != "" {
				return nil
			}
			// Resource.field1 = repeated KeyValue
			return eachField(val, func(field int, wt int, val []byte) error {
				if field != 1 || wt != wireLen || found != "" {
					return nil
				}
				name, err := serviceNameFromKeyValue(val)
				if err != nil {
					return err
				}
				if name != "" {
					found = name
				}
				return nil
			})
		})
	})
	if err != nil {
		return "", err
	}
	return found, nil
}

// serviceNameFromKeyValue 从一条 KeyValue 中取出 service.name 的字符串值。
// 键不匹配时返回空串。
func serviceNameFromKeyValue(buf []byte) (string, error) {
	var key string
	var anyValue []byte
	err := eachField(buf, func(field int, wt int, val []byte) error {
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
	if err != nil {
		return "", err
	}
	if key != attrServiceName || anyValue == nil {
		return "", nil
	}

	out := ""
	err = eachField(anyValue, func(field int, wt int, val []byte) error {
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
// 对 LEN 型字段，val 是其载荷；对其他线类型，val 为 nil（调用方目前不需要标量值）。
// 回调返回非 nil 错误即中止遍历。
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
			buf = buf[8:]
			if err := fn(field, wt, nil); err != nil {
				return err
			}

		case wireI32:
			if len(buf) < 4 {
				return ErrMalformed
			}
			buf = buf[4:]
			if err := fn(field, wt, nil); err != nil {
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
