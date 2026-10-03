package flatten

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"unicode/utf8"
)

// AnyValue 字段号（OTLP common.proto）。
const (
	anyString = 1
	anyBool   = 2
	anyInt    = 3
	anyDouble = 4
	anyArray  = 5
	anyKVList = 6
	anyBytes  = 7
)

func decodeAny(buf []byte) (any, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	var out any
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch field {
		case anyString:
			out = string(raw)
		case anyBool:
			out = u != 0
		case anyInt:
			out = int64(u)
		case anyDouble:
			if len(raw) == 8 {
				out = math.Float64frombits(binary.LittleEndian.Uint64(raw))
			}
		case anyArray:
			arr, err := decodeArray(raw)
			if err != nil {
				return err
			}
			out = arr
		case anyKVList:
			m, err := decodeKVList(raw)
			if err != nil {
				return err
			}
			out = m
		case anyBytes:
			out = hex.EncodeToString(raw)
		}
		return nil
	})
	return out, err
}

func decodeArray(buf []byte) ([]any, error) {
	var out []any
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		if field != 1 || wt != wireLen {
			return nil
		}
		v, err := decodeAny(raw)
		if err != nil {
			return err
		}
		out = append(out, v)
		return nil
	})
	return out, err
}

func decodeKVList(buf []byte) (map[string]any, error) {
	out := map[string]any{}
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		if field != 1 || wt != wireLen {
			return nil
		}
		k, v, err := decodeKeyValue(raw)
		if err != nil {
			return err
		}
		if k != "" {
			out[k] = v
		}
		return nil
	})
	return out, err
}

func decodeKeyValue(buf []byte) (string, any, error) {
	var key string
	var val any
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		switch {
		case field == 1 && wt == wireLen:
			key = string(raw)
		case field == 2 && wt == wireLen:
			v, err := decodeAny(raw)
			if err != nil {
				return err
			}
			val = v
		}
		return nil
	})
	return key, val, err
}

func decodeAttributes(buf []byte) (map[string]any, error) {
	out := map[string]any{}
	err := eachField(buf, func(field, wt int, raw []byte, u uint64) error {
		if field != 1 || wt != wireLen { // Resource.attributes / 记录 attributes 由调用方喂单条 KV
			return nil
		}
		k, v, err := decodeKeyValue(raw)
		if err != nil {
			return err
		}
		if k != "" {
			out[k] = v
		}
		return nil
	})
	return out, err
}

func decodeAttrList(kvs [][]byte) (map[string]any, error) {
	out := map[string]any{}
	for _, raw := range kvs {
		k, v, err := decodeKeyValue(raw)
		if err != nil {
			return nil, err
		}
		if k != "" {
			out[k] = v
		}
	}
	return out, nil
}

func hexID(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

func stringAttr(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func truncateKey(k string) (string, bool) {
	if len(k) <= maxJSONKeyBytes {
		return k, false
	}
	// 按字节截断，避免切在 rune 中间。
	n := maxJSONKeyBytes
	for n > 0 && !utf8.RuneStart(k[n]) {
		n--
	}
	if n <= 0 {
		n = maxJSONKeyBytes
	}
	return k[:n], true
}

const maxJSONKeyBytes = 255
