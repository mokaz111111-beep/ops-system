// Package identity 定义平台的租户身份模型（DD-006 §3.1）。
//
// 四层划分的依据是四种不同的"边界"各自需要独立控制，而不是层级好看：
//
//	Organization  计费与合同边界   —— 账单、合同配额、租户档位
//	Project       数据隔离边界     —— 查询可见范围、留存策略、脱敏规则
//	Cluster       故障与限流边界   —— 限流桶、采样策略、健康状态
//	Token         凭证边界         —— 吊销、速率上限
//
// 全链路字段 tenant_id 对应 Organization，不是 Project（SD-000 §7.1）。这一点在此
// 用独立类型锁死：TenantID 与 ProjectID 不可互相赋值，因此"把 project 当 tenant 用"
// 这类错误在编译期就会被挡住，而不是等到某天发现隔离只存在于纸面上。
package identity

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
)

// TenantID 是 Organization 粒度的租户标识，即全链路的 tenant_id。
type TenantID string

// ProjectID 是环境（prod / staging）粒度的标识，数据隔离边界。
type ProjectID string

// ClusterID 是客户的一个 K8s 集群或自建机房，故障与限流边界。
type ClusterID string

// TokenHash 是集群级摄入 Token 的 SHA-256 十六进制摘要。
//
// 平台只存摘要不存明文（X-002 §2.6.2：Token 哈希存储，仅创建时明文展示一次）。
type TokenHash string

// Status 是租户或集群的启用状态。
type Status uint8

const (
	// StatusUnknown 零值。显式区分于 StatusActive，避免未初始化的 Binding 被当作有效。
	StatusUnknown Status = iota
	// StatusActive 正常服务。
	StatusActive
	// StatusSuspended 停用（欠费、熔断）。对应 IF-1 的 E2。
	StatusSuspended
	// StatusOffboarding 退租中。拒绝新数据，等待删除流程（X-002 §2.5）。
	StatusOffboarding
)

func (s Status) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusSuspended:
		return "suspended"
	case StatusOffboarding:
		return "offboarding"
	default:
		return "unknown"
	}
}

// Servable 报告该状态是否允许接收数据。
func (s Status) Servable() bool { return s == StatusActive }

// Binding 是一个 Token 反查出的完整身份，由摄入网关强制注入下游。
//
// 客户端自报的任何同名字段都被覆盖（SD-000 §7.1）——这是零数据串扰的第一道前提。
type Binding struct {
	TenantID  TenantID
	ProjectID ProjectID
	ClusterID ClusterID

	// TokenHash 保留摘要用于审计与吊销比对，不含明文。
	TokenHash TokenHash

	// Revoked 为 true 表示该 Token 已吊销。与 Status 分开是因为吊销针对单个凭证，
	// 停用针对整个租户：吊销一个集群的 Token 不应影响同租户其他集群。
	Revoked bool

	// TenantStatus 为租户级状态，ClusterStatus 为集群级状态。
	// 两者都必须 Servable 才接收数据——任一层停用都足以拒绝。
	TenantStatus  Status
	ClusterStatus Status
}

// Servable 报告该身份当前是否可接收数据。
func (b Binding) Servable() bool {
	return !b.Revoked && b.TenantStatus.Servable() && b.ClusterStatus.Servable()
}

// Valid 报告 Binding 的必填字段是否齐备。
//
// 它与 Servable 是两个问题：Valid 问"这条身份记录本身完整吗"（配置正确性），
// Servable 问"现在能收数据吗"（运行时状态）。
func (b Binding) Valid() bool {
	return b.TenantID != "" && b.ClusterID != "" && b.TokenHash != ""
}

// QuotaKey 是限流桶的键。
//
// 按 Cluster 而非 Tenant 分桶：集群是故障与限流边界，某集群异常刷量只应熔断该集群，
// 不影响同租户的其他集群（DD-006 §3.1）。TenantID 一并带上是为了桶键在跨租户同名
// cluster_id 的情况下不冲突。
type QuotaKey struct {
	TenantID  TenantID
	ClusterID ClusterID
}

func (k QuotaKey) String() string { return string(k.TenantID) + "/" + string(k.ClusterID) }

// ErrEmptyToken 表示请求未携带 Token。
var ErrEmptyToken = errors.New("identity: token 为空")

// HashToken 计算 Token 的摘要。
//
// 比较必须用 EqualHash 做常量时间比较，不要直接用 ==，以免摘要比较本身成为计时侧信道。
func HashToken(token string) (TokenHash, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", ErrEmptyToken
	}
	sum := sha256.Sum256([]byte(token))
	return TokenHash(hex.EncodeToString(sum[:])), nil
}

// EqualHash 以常量时间比较两个摘要。
func EqualHash(a, b TokenHash) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
