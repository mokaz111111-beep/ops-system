package otlphttp

import "github.com/mokaz/ops-system/pkg/ingesterr"

// 手写的 protobuf 编码器，只覆盖两个响应消息。
//
// 对应的 proto 定义（字段号是线格式契约的一部分，不是实现细节）：
//
//	message Export{Logs,Trace,Metrics}ServiceResponse {
//	  Export*PartialSuccess partial_success = 1;
//	}
//	message Export*PartialSuccess {
//	  int64  rejected_{log_records,spans,data_points} = 1;   // 三者字段号同为 1
//	  string error_message                           = 2;
//	}
//	message google.rpc.Status {
//	  int32  code    = 1;
//	  string message = 2;
//	}
//
// 三类信号的 PartialSuccess 只有字段名不同、字段号相同，因此一套编码覆盖三者——与
// otlpwire 依赖的同构性是同一件事。

const (
	wireVarint = 0
	wireLen    = 2
)

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendTag(b []byte, field, wireType int) []byte {
	return appendVarint(b, uint64(field)<<3|uint64(wireType))
}

func appendString(b []byte, field int, s string) []byte {
	b = appendTag(b, field, wireLen)
	b = appendVarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendBytes(b []byte, field int, v []byte) []byte {
	b = appendTag(b, field, wireLen)
	b = appendVarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendInt64(b []byte, field int, v int64) []byte {
	b = appendTag(b, field, wireVarint)
	return appendVarint(b, uint64(v))
}

// encodeExportResponse 编码 Export*ServiceResponse。
//
// 全批接受时返回 nil（零字节），这是一个合法的空 protobuf 消息，也是 OTLP 规范对成功
// 响应的推荐形态。只有确有记录被拒时才带 partial_success——带一个 rejected=0 的
// partial_success 会让部分 SDK 误判为发生了部分失败并打告警日志。
func encodeExportResponse(p ingesterr.PartialSuccess) []byte {
	if !p.IsPartial() {
		return nil
	}
	var inner []byte
	inner = appendInt64(inner, 1, p.RejectedRecords)
	if p.Reason != "" {
		inner = appendString(inner, 2, p.Reason)
	}
	return appendBytes(nil, 1, inner)
}

// encodeStatus 编码 google.rpc.Status。
func encodeStatus(grpcCode int, msg string) []byte {
	var b []byte
	b = appendInt64(b, 1, int64(grpcCode))
	if msg != "" {
		b = appendString(b, 2, msg)
	}
	return b
}
