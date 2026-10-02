package otlpwire

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
)

// --- 测试用的最小 protobuf 编码器 ---------------------------------------------
//
// 刻意手写而不引入 OTLP proto 依赖：被测代码声称"只按线格式理解 OTLP"，
// 那么测试也应当只按线格式构造输入，否则测的就不是同一件事。

func varint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func tag(field, wireType int) []byte {
	return varint(uint64(field)<<3 | uint64(wireType))
}

// lenField 编码一个 LEN 型字段。
func lenField(field int, payload []byte) []byte {
	var b bytes.Buffer
	b.Write(tag(field, wireLen))
	b.Write(varint(uint64(len(payload))))
	b.Write(payload)
	return b.Bytes()
}

// varintField 编码一个 VARINT 型字段。
func varintField(field int, v uint64) []byte {
	var b bytes.Buffer
	b.Write(tag(field, wireVarint))
	b.Write(varint(v))
	return b.Bytes()
}

func concat(parts ...[]byte) []byte {
	var b bytes.Buffer
	for _, p := range parts {
		b.Write(p)
	}
	return b.Bytes()
}

// keyValue 构造 OTel 的 KeyValue{key, value:AnyValue{string_value}}。
func keyValue(key, val string) []byte {
	anyValue := lenField(1, []byte(val))
	return concat(lenField(1, []byte(key)), lenField(2, anyValue))
}

// resource 构造 Resource{repeated KeyValue attributes = 1}。
func resource(kvs ...[]byte) []byte {
	var parts [][]byte
	for _, kv := range kvs {
		parts = append(parts, lenField(1, kv))
	}
	return concat(parts...)
}

// scope 构造 Scope*{repeated record = 2}，records 为记录数。
func scope(records int) []byte {
	var parts [][]byte
	for i := 0; i < records; i++ {
		// 记录内部填一个无关字段，确认计数不进入记录内部。
		parts = append(parts, lenField(2, varintField(7, uint64(i))))
	}
	return concat(parts...)
}

// resourceBlock 构造 Resource*{resource = 1, repeated scope_* = 2}。
func resourceBlock(res []byte, scopes ...[]byte) []byte {
	parts := [][]byte{}
	if res != nil {
		parts = append(parts, lenField(1, res))
	}
	for _, s := range scopes {
		parts = append(parts, lenField(2, s))
	}
	return concat(parts...)
}

// request 构造 Export*ServiceRequest{repeated resource_* = 1}。
func request(blocks ...[]byte) []byte {
	var parts [][]byte
	for _, b := range blocks {
		parts = append(parts, lenField(1, b))
	}
	return concat(parts...)
}

// --- CountRecords -------------------------------------------------------------

func TestCountRecords(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
		want int
	}{
		{"空请求", nil, 0},
		{"单 resource 单 scope 单记录",
			request(resourceBlock(nil, scope(1))), 1},
		{"单 resource 单 scope 多记录",
			request(resourceBlock(nil, scope(42))), 42},
		{"单 resource 多 scope",
			request(resourceBlock(nil, scope(3), scope(4))), 7},
		{"多 resource",
			request(
				resourceBlock(resource(keyValue("service.name", "a")), scope(2)),
				resourceBlock(resource(keyValue("service.name", "b")), scope(5), scope(1)),
			), 8},
		{"resource 有属性但无 scope",
			request(resourceBlock(resource(keyValue("service.name", "a")))), 0},
		{"scope 存在但无记录",
			request(resourceBlock(nil, scope(0))), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := CountRecords(c.buf)
			if err != nil {
				t.Fatalf("意外错误: %v", err)
			}
			if got != c.want {
				t.Errorf("期望 %d 条，得到 %d 条", c.want, got)
			}
		})
	}
}

// TestCountRecordsIgnoresRecordInternals 确认计数停在记录这一层。
// 如果实现误入记录内部，嵌套了 field 2 的记录就会被重复计数。
func TestCountRecordsIgnoresRecordInternals(t *testing.T) {
	// 一条记录，其内部也有一个 field 2 的 LEN 字段（例如 LogRecord 的某个子消息）。
	nasty := lenField(2, lenField(2, []byte("inner")))
	buf := request(resourceBlock(nil, nasty))

	got, err := CountRecords(buf)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if got != 1 {
		t.Errorf("期望 1 条（不得递归进记录内部），得到 %d 条", got)
	}
}

// --- ServiceName --------------------------------------------------------------

func TestServiceName(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
		want string
	}{
		{"无 resource", request(resourceBlock(nil, scope(1))), ""},
		{"有 service.name",
			request(resourceBlock(resource(keyValue("service.name", "checkout")), scope(1))),
			"checkout"},
		{"service.name 不在首位",
			request(resourceBlock(resource(
				keyValue("host.name", "node-1"),
				keyValue("service.name", "payment"),
				keyValue("k8s.pod.name", "p-1"),
			), scope(1))),
			"payment"},
		{"只有其他属性",
			request(resourceBlock(resource(keyValue("host.name", "node-1")), scope(1))),
			""},
		{"多 resource 取首个",
			request(
				resourceBlock(resource(keyValue("service.name", "first")), scope(1)),
				resourceBlock(resource(keyValue("service.name", "second")), scope(1)),
			),
			"first"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ServiceName(c.buf)
			if err != nil {
				t.Fatalf("意外错误: %v", err)
			}
			if got != c.want {
				t.Errorf("期望 %q，得到 %q", c.want, got)
			}
		})
	}
}

// --- 不可信输入 ---------------------------------------------------------------

// TestMalformedInputNeverPanics 是本包最重要的测试：它解析的是客户直接发来的字节，
// 是整个平台第一个接触不可信输入的地方。任何输入都必须返回错误而不是 panic——
// 网关 panic 等于该副本上所有租户的数据一起中断。
func TestMalformedInputNeverPanics(t *testing.T) {
	seeds := [][]byte{
		{0x08},                         // varint 字段但值被截断
		{0x0a},                         // LEN 字段但长度被截断
		{0x0a, 0x05, 'a', 'b'},         // 声明 5 字节只给了 2 字节
		{0x0a, 0xff, 0xff, 0xff, 0x7f}, // 超大长度
		{0x09},                         // I64 但无数据
		{0x0d},                         // I32 但无数据
		{0x1b},                         // SGROUP（已废弃）
		{0x00},                         // field 0，非法
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, // varint 溢出
		bytes.Repeat([]byte{0x80}, 16),                                     // 全是延续位
	}

	check := func(buf []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("CountRecords/ServiceName 在输入 %x 上 panic: %v", buf, r)
			}
		}()
		_, _ = CountRecords(buf)
		_, _ = ServiceName(buf)
	}

	for _, s := range seeds {
		check(s)
	}

	// 随机字节：确定性种子，便于失败复现。
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		buf := make([]byte, rng.Intn(64))
		rng.Read(buf)
		check(buf)
	}

	// 对合法编码做截断：这是最接近真实故障（连接中断、batch 被切断）的一类畸形输入。
	valid := request(resourceBlock(resource(keyValue("service.name", "svc")), scope(3)))
	for i := 0; i < len(valid); i++ {
		check(valid[:i])
	}
}

func TestTruncatedValidInputReportsError(t *testing.T) {
	valid := request(resourceBlock(nil, scope(3)))
	// 砍掉最后一个字节后，顶层长度声明必然超出剩余字节。
	_, err := CountRecords(valid[:len(valid)-1])
	if err == nil {
		t.Fatal("截断输入应报错，而不是静默返回部分计数")
	}
	if !errors.Is(err, ErrMalformed) && !errors.Is(err, ErrOverflow) {
		t.Errorf("期望 ErrMalformed/ErrOverflow，得到 %v", err)
	}
}

func TestReadVarint(t *testing.T) {
	cases := []struct {
		in   []byte
		want uint64
		n    int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x01}, 1, 1},
		{[]byte{0x7f}, 127, 1},
		{[]byte{0x80, 0x01}, 128, 2},
		{[]byte{0xff, 0x01}, 255, 2},
		{[]byte{0xac, 0x02}, 300, 2},
	}
	for _, c := range cases {
		got, n, err := readVarint(c.in)
		if err != nil {
			t.Errorf("%x: 意外错误 %v", c.in, err)
			continue
		}
		if got != c.want || n != c.n {
			t.Errorf("%x: 期望 (%d, %d)，得到 (%d, %d)", c.in, c.want, c.n, got, n)
		}
	}
}

func BenchmarkCountRecords(b *testing.B) {
	// 近似一个真实批次：10 个 resource，每个 2 个 scope，每 scope 50 条记录 = 1000 条。
	var blocks [][]byte
	for i := 0; i < 10; i++ {
		blocks = append(blocks, resourceBlock(
			resource(keyValue("service.name", "checkout"), keyValue("host.name", "node-1")),
			scope(50), scope(50),
		))
	}
	buf := request(blocks...)

	b.ReportAllocs()
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := CountRecords(buf); err != nil {
			b.Fatal(err)
		}
	}
}
