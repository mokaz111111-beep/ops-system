package flatten

import (
	"encoding/binary"
	"errors"
)

// 本包在 protobuf 线格式上展开 OTLP logs/spans，不引入 go.opentelemetry.io/proto。
//
// 网关的 otlpwire 只计数与抽 service.name，展行必须读记录内部字段。官方 proto
// 依赖会把整个 OTLP 对象图拉进来，而我们只用到 logs/traces 两条导出请求的字段号。
// 字段号是协议的一部分（OTLP proto），变更只会发生在大版本上。

const (
	wireVarint = 0
	wireI64    = 1
	wireLen    = 2
	wireSGroup = 3
	wireEGroup = 4
	wireI32    = 5
)

var (
	errMalformed = errors.New("flatten: protobuf 编码非法")
	errOverflow  = errors.New("flatten: varint 溢出")
)

func eachField(buf []byte, fn func(field, wt int, raw []byte, u uint64) error) error {
	for len(buf) > 0 {
		tag, n, err := readVarint(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
		field := int(tag >> 3)
		wt := int(tag & 7)
		if field <= 0 {
			return errMalformed
		}
		switch wt {
		case wireVarint:
			v, n, err := readVarint(buf)
			if err != nil {
				return err
			}
			buf = buf[n:]
			if err := fn(field, wt, nil, v); err != nil {
				return err
			}
		case wireI64:
			if len(buf) < 8 {
				return errMalformed
			}
			raw := buf[:8]
			buf = buf[8:]
			if err := fn(field, wt, raw, binary.LittleEndian.Uint64(raw)); err != nil {
				return err
			}
		case wireI32:
			if len(buf) < 4 {
				return errMalformed
			}
			raw := buf[:4]
			buf = buf[4:]
			if err := fn(field, wt, raw, uint64(binary.LittleEndian.Uint32(raw))); err != nil {
				return err
			}
		case wireLen:
			size, n, err := readVarint(buf)
			if err != nil {
				return err
			}
			buf = buf[n:]
			if size > uint64(len(buf)) {
				return errMalformed
			}
			raw := buf[:size]
			buf = buf[size:]
			if err := fn(field, wt, raw, 0); err != nil {
				return err
			}
		default:
			return errMalformed
		}
	}
	return nil
}

func readVarint(buf []byte) (uint64, int, error) {
	var v uint64
	for i := 0; i < len(buf); i++ {
		b := buf[i]
		if i == 9 && b > 1 {
			return 0, 0, errOverflow
		}
		v |= uint64(b&0x7f) << (7 * uint(i))
		if b < 0x80 {
			return v, i + 1, nil
		}
		if i == 9 {
			return 0, 0, errOverflow
		}
	}
	return 0, 0, errMalformed
}
