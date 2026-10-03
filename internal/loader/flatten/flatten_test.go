package flatten

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func varint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func tag(field, wt int) []byte { return varint(uint64(field)<<3 | uint64(wt)) }

func lenF(field int, payload []byte) []byte {
	var b bytes.Buffer
	b.Write(tag(field, wireLen))
	b.Write(varint(uint64(len(payload))))
	b.Write(payload)
	return b.Bytes()
}

func varF(field int, v uint64) []byte {
	var b bytes.Buffer
	b.Write(tag(field, wireVarint))
	b.Write(varint(v))
	return b.Bytes()
}

func i64F(field int, v uint64) []byte {
	var b bytes.Buffer
	b.Write(tag(field, wireI64))
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], v)
	b.Write(raw[:])
	return b.Bytes()
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func avString(s string) []byte { return lenF(1, []byte(s)) }
func avInt(v int64) []byte     { return varF(3, uint64(v)) }

func kv(key string, anyVal []byte) []byte {
	return cat(lenF(1, []byte(key)), lenF(2, anyVal))
}

func ident() Identity {
	return Identity{TenantID: "org-from-header", ClusterID: "c-from-header"}
}

func TestFlattenLogsUsesHeaderIdentityOnly(t *testing.T) {
	res := cat(
		lenF(1, kv("service.name", avString("checkout"))),
		lenF(1, kv("tenant_id", avString("forged-tenant"))),
		lenF(1, kv("cluster_id", avString("forged-cluster"))),
		lenF(1, kv("host.name", avString("n1"))),
	)
	rec := cat(
		i64F(1, uint64(time.Date(2026, 10, 4, 1, 2, 3, 123000000, time.UTC).UnixNano())),
		varF(2, 17),
		lenF(3, []byte("ERROR")),
		lenF(5, avString("boom")),
		lenF(6, kv("http.status_code", avString("200"))),
		lenF(6, kv("http.method", avString("GET"))),
		lenF(9, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
		lenF(10, []byte{9, 8, 7, 6, 5, 4, 3, 2}),
	)
	scope := lenF(2, rec)
	block := cat(lenF(1, res), lenF(2, scope))
	req := lenF(1, block)

	got, err := Flatten(telemetry.SignalLogs, req, ident(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 1 {
		t.Fatalf("行数 %d", len(got.Rows))
	}
	row := got.Rows[0]
	if row["tenant_id"] != "org-from-header" || row["cluster_id"] != "c-from-header" {
		t.Fatalf("身份列必须只认头: %+v", row)
	}
	resMap := row["resource"].(map[string]any)
	if resMap["tenant_id"] != "forged-tenant" {
		t.Fatal("payload 内自报身份应留在 VARIANT，但不得回填列")
	}
	attrs := row["log_attributes"].(map[string]any)
	if attrs["http.status_code"] != int32(200) {
		t.Fatalf("\"200\" 应按声明 INT 强制转换，得到 %#v", attrs["http.status_code"])
	}
	if got.EncodedBytes <= 0 || !bytes.Contains(got.Encoded, []byte("org-from-header")) {
		t.Fatalf("encoded_bytes 未计入 JSON: %s", got.Encoded)
	}
	if row["service"] != "checkout" || row["severity"] != "ERROR" {
		t.Fatalf("列映射错误: %+v", row)
	}
}

func TestCoerceFailureGoesToRawShadow(t *testing.T) {
	rec := cat(lenF(6, kv("http.status_code", avString("not-a-number"))))
	req := lenF(1, cat(lenF(2, lenF(2, rec))))
	got, err := Flatten(telemetry.SignalLogs, req, ident(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	attrs := got.Rows[0]["log_attributes"].(map[string]any)
	if _, ok := attrs["http.status_code"]; ok {
		t.Fatal("转换失败不得写入主路径")
	}
	if attrs["http.status_code__raw"] != "not-a-number" {
		t.Fatalf("影子路径: %+v", attrs)
	}
	if len(got.Conflicts) != 1 || got.Conflicts[0].Path != "http.status_code" {
		t.Fatalf("冲突未计数: %+v", got.Conflicts)
	}
}

func TestJSONKeyTruncatedAt255(t *testing.T) {
	long := strings.Repeat("k", 300)
	rec := cat(lenF(6, kv(long, avString("v"))))
	req := lenF(1, cat(lenF(2, lenF(2, rec))))
	got, err := Flatten(telemetry.SignalLogs, req, ident(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if got.TruncatedKeys != 1 {
		t.Fatalf("截断计数 %d", got.TruncatedKeys)
	}
	attrs := got.Rows[0]["log_attributes"].(map[string]any)
	if len(attrs) != 1 {
		t.Fatalf("键: %+v", attrs)
	}
	for k := range attrs {
		if len(k) > 255 {
			t.Fatalf("键长 %d", len(k))
		}
	}
}

func TestFlattenSpansColumns(t *testing.T) {
	start := uint64(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC).UnixNano())
	end := start + 1500_000 // 1500µs
	res := cat(
		lenF(1, kv("service.name", avString("api"))),
		lenF(1, kv("service.instance.id", avString("i-1"))),
	)
	event := cat(i64F(1, start), lenF(2, []byte("exc")), lenF(3, kv("k", avString("v"))))
	span := cat(
		lenF(1, bytes.Repeat([]byte{0xab}, 16)),
		lenF(2, bytes.Repeat([]byte{0xcd}, 8)),
		lenF(4, bytes.Repeat([]byte{0x11}, 8)),
		lenF(5, []byte("GET /")),
		varF(6, 2),
		i64F(7, start),
		i64F(8, end),
		lenF(9, kv("http.status_code", avInt(404))),
		lenF(11, event),
		lenF(15, cat(lenF(2, []byte("boom")), varF(3, 2))),
	)
	scope := cat(lenF(1, cat(lenF(1, []byte("go.opentelemetry")), lenF(2, []byte("1.0")))), lenF(2, span))
	req := lenF(1, cat(lenF(1, res), lenF(2, scope)))

	got, err := Flatten(telemetry.SignalTraces, req, ident(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	row := got.Rows[0]
	if row["tenant_id"] != "org-from-header" {
		t.Fatal(row["tenant_id"])
	}
	if row["service_name"] != "api" || row["span_kind"] != "Server" || row["status_code"] != "Error" {
		t.Fatalf("%+v", row)
	}
	if row["duration"] != int64(1500) {
		t.Fatalf("duration=%#v", row["duration"])
	}
	if row["scope_name"] != "go.opentelemetry" {
		t.Fatalf("scope=%v", row["scope_name"])
	}
}

func TestCoercerIsPluggable(t *testing.T) {
	// A3 未结论：换成空 Schema 后不应再强制 http.status_code。
	opt := DefaultOptions()
	opt.LogAttrs = SchemaCoercer{Paths: map[string]DeclaredType{}}
	rec := cat(lenF(6, kv("http.status_code", avString("200"))))
	req := lenF(1, cat(lenF(2, lenF(2, rec))))
	got, err := Flatten(telemetry.SignalLogs, req, ident(), opt)
	if err != nil {
		t.Fatal(err)
	}
	attrs := got.Rows[0]["log_attributes"].(map[string]any)
	if attrs["http.status_code"] != "200" {
		t.Fatalf("可插拔关闭后应保持原值: %#v", attrs["http.status_code"])
	}
}

func TestMalformedRejected(t *testing.T) {
	_, err := Flatten(telemetry.SignalLogs, []byte{0xff, 0xff, 0xff, 0xff, 0xff}, ident(), Options{})
	if err == nil {
		t.Fatal("非法 protobuf 必须报错")
	}
}

func TestIdentityTypesCannotBeInferred(t *testing.T) {
	// 编译期约束的运行时回声：Flatten 的身份参数是独立类型，测试里故意
	// 不用 payload 里的值构造 Identity。
	_ = identity.TenantID("org-from-header")
}
