// Package ingestmsg 定义摄入网关写入 Kafka 的消息元数据，是 IF-2 的一部分。
//
// # 为什么身份走消息头，而不是改写 payload
//
// DD-001 §3.2 原本把租户富化放在平台侧 Collector 的 tenant_enricher，由它把 tenant_id
// 注入 resource attributes。M1 的摄入网关直接投 Kafka（见 DD-001 §3.2 的 M1 说明），
// 于是注入点落到网关。网关有两种选法：
//
//	A. 在 protobuf 线格式上改写每个 Resource 的 attributes；
//	B. 把身份放进 Kafka 消息头，payload 保持客户原样。
//
// 选 B。理由有三条，按重要性排序：
//
//  1. 覆盖语义只有 B 能干净实现。SD-000 §7.1 要求客户端自报的 tenant_id 一律不被信任。
//     方案 A 的"注入"实际是"先删除客户已有的同名属性再插入"，删除需要重写变长的嵌套
//     结构；而消息头天然在 payload 之外，客户无法伪造。
//  2. 保住快速路径。网关只有 0.5s 预算（§8.1），改写 payload 等于对每个 resource 做一次
//     解码—编码往返，与 otlpwire 存在的全部理由相悖。M2 的 PII 脱敏会引入完整解码，
//     那是脱敏自己的成本，不该由身份注入提前付掉。
//  3. 编码不变。IF-2 规定消息编码为 otlp_proto，payload 原样转发使这一条在字节层面成立，
//     下游换用官方反序列化实现时不需要考虑网关的私有改写。
//
// 代价是 otlp-loader 必须只认消息头里的身份，忽略 payload 内的同名 resource 属性。
// 这条约束写在这里，是因为它是 B 方案成立的前提，而不是 loader 的内部选择。
package ingestmsg

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// 消息头键。全部小写并带 ops- 前缀，避免与 OTel Collector kafka exporter 可能写入的头
// 冲突——M2 若把平台侧 Collector 补回链路，两种生产者会同时存在一段时间。
const (
	HeaderVersion     = "ops-msg-version"
	HeaderTenantID    = "ops-tenant-id"
	HeaderProjectID   = "ops-project-id"
	HeaderClusterID   = "ops-cluster-id"
	HeaderSignal      = "ops-signal"
	HeaderRecordCount = "ops-record-count"
	HeaderReceivedAt  = "ops-gateway-received-at"
	HeaderServiceName = "ops-service-name"
)

// Version 是消息头格式版本。
//
// 新增可选头不递增版本；改变已有头的含义或必填集合才递增，使 loader 能在混跑期间按版本
// 分流而不是靠猜。
const Version = "1"

// Header 是一条 Kafka 消息头。刻意不复用 Kafka 客户端库的类型：IF-2 是契约，不应让契约
// 定义随客户端库的版本漂移。
type Header struct {
	Key   string
	Value []byte
}

// Meta 是一条摄入消息的元数据。
type Meta struct {
	TenantID  identity.TenantID
	ProjectID identity.ProjectID
	ClusterID identity.ClusterID
	Signal    telemetry.Signal

	// RecordCount 是 payload 内的记录条数（otlpwire 线级计数结果）。下游用它做
	// "网关收到 N 条 → 落库 M 行"的对账，缺了它这个缺口只能靠抽样估计。
	RecordCount int64

	// ReceivedAt 是网关收到请求的时刻。
	//
	// 它是 §8.1 后三段延迟的唯一时间原点：loader 用它减出 Kafka 段与自身攒批段。
	// 刻意用网关时钟而非客户端时钟——平台内部各进程的时钟由同一 NTP 源收敛，跨进程相减
	// 有意义；客户端时钟不在平台控制内（见 latency.StageCollectorBatch 的 Note）。
	ReceivedAt time.Time

	// ServiceName 是首个 resource 的 service.name，可为空。
	// 它同时是 logs.raw 的分区键组成部分，放进头里便于排查分区热点。
	ServiceName string
}

var (
	// ErrMissingHeader 表示必填头缺失。
	ErrMissingHeader = errors.New("ingestmsg: 必填消息头缺失")
	// ErrBadHeader 表示头存在但取值非法。
	ErrBadHeader = errors.New("ingestmsg: 消息头取值非法")
)

// Valid 报告必填字段是否齐备。
//
// ProjectID 不在必填集合内：DD-006 OQ-4 尚未裁定 project 维度的隔离执行强度，此刻把它设为
// 必填会让控制面还没下发 project 的租户直接无法摄入。
func (m Meta) Valid() bool {
	return m.TenantID != "" && m.ClusterID != "" && m.Signal.Valid() && !m.ReceivedAt.IsZero()
}

// Headers 把元数据编码为 Kafka 消息头。
func (m Meta) Headers() []Header {
	hs := []Header{
		{HeaderVersion, []byte(Version)},
		{HeaderTenantID, []byte(m.TenantID)},
		{HeaderClusterID, []byte(m.ClusterID)},
		{HeaderSignal, []byte(m.Signal.String())},
		{HeaderRecordCount, []byte(strconv.FormatInt(m.RecordCount, 10))},
		{HeaderReceivedAt, []byte(strconv.FormatInt(m.ReceivedAt.UTC().UnixNano(), 10))},
	}
	// 可选头为空时不写，省掉每条消息的常量开销；缺失与空串在语义上同为"未知"。
	if m.ProjectID != "" {
		hs = append(hs, Header{HeaderProjectID, []byte(m.ProjectID)})
	}
	if m.ServiceName != "" {
		hs = append(hs, Header{HeaderServiceName, []byte(m.ServiceName)})
	}
	return hs
}

// ParseMeta 从消息头还原元数据，供 otlp-loader 使用。
//
// 必填头缺失一律报错而不是补默认值：一条身份不全的消息无法被归属到租户，猜一个 tenant_id
// 写进去会直接造成跨租户污染，而静默丢弃又违反 SLO-6。正确处置是让 loader 显式进死信路径
// 并告警，因此这里必须报错。
func ParseMeta(hs []Header) (Meta, error) {
	// 同名头取最后一个：Kafka 允许重复头，取最后相当于"后写覆盖先写"，与消息经过中间
	// 组件被追加时的直觉一致。
	get := func(key string) (string, bool) {
		val, ok := "", false
		for _, h := range hs {
			if h.Key == key {
				val, ok = string(h.Value), true
			}
		}
		return val, ok
	}

	var m Meta
	ver, ok := get(HeaderVersion)
	if !ok {
		return m, fmt.Errorf("%w: %s", ErrMissingHeader, HeaderVersion)
	}
	if ver != Version {
		return m, fmt.Errorf("%w: %s=%q（本实现支持 %q）", ErrBadHeader, HeaderVersion, ver, Version)
	}

	tenant, ok := get(HeaderTenantID)
	if !ok || tenant == "" {
		return m, fmt.Errorf("%w: %s", ErrMissingHeader, HeaderTenantID)
	}
	cluster, ok := get(HeaderClusterID)
	if !ok || cluster == "" {
		return m, fmt.Errorf("%w: %s", ErrMissingHeader, HeaderClusterID)
	}
	sigRaw, ok := get(HeaderSignal)
	if !ok {
		return m, fmt.Errorf("%w: %s", ErrMissingHeader, HeaderSignal)
	}
	sig, ok := telemetry.ParseSignal(sigRaw)
	if !ok {
		return m, fmt.Errorf("%w: %s=%q", ErrBadHeader, HeaderSignal, sigRaw)
	}

	countRaw, ok := get(HeaderRecordCount)
	if !ok {
		return m, fmt.Errorf("%w: %s", ErrMissingHeader, HeaderRecordCount)
	}
	count, err := strconv.ParseInt(countRaw, 10, 64)
	if err != nil || count < 0 {
		return m, fmt.Errorf("%w: %s=%q", ErrBadHeader, HeaderRecordCount, countRaw)
	}

	tsRaw, ok := get(HeaderReceivedAt)
	if !ok {
		return m, fmt.Errorf("%w: %s", ErrMissingHeader, HeaderReceivedAt)
	}
	nanos, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil || nanos <= 0 {
		return m, fmt.Errorf("%w: %s=%q", ErrBadHeader, HeaderReceivedAt, tsRaw)
	}

	project, _ := get(HeaderProjectID)
	service, _ := get(HeaderServiceName)

	return Meta{
		TenantID:    identity.TenantID(tenant),
		ProjectID:   identity.ProjectID(project),
		ClusterID:   identity.ClusterID(cluster),
		Signal:      sig,
		RecordCount: count,
		ReceivedAt:  time.Unix(0, nanos).UTC(),
		ServiceName: service,
	}, nil
}
