// Package config 加载摄入网关的本地配置与本地身份/配额快照。
//
// # 只读本地，没有例外
//
// PLAN §3 对 M1 的要求是"配置硬编码即可，但不得写出请求路径同步查控制面的代码"。因此本包
// 只有一个数据来源：本地文件。它不含任何 HTTP 客户端、不认识控制面地址，于是"在请求路径上
// 顺手查一下控制面"这件事在本包里无处落脚——这正是 fail-static 结构要在 M1 就建起来的理由
// （DD-006 §3.6.1：控制面可用性不得乘进数据面）。
//
// M2 的接缝：控制面下发通道（IF-9）落地后，它的职责是把下发内容写成本地快照文件，
// 网关这一侧的加载路径不变。换句话说，下发通道是快照的生产者，而不是网关的依赖。
//
// # 为什么配置文件里只有 Token 摘要
//
// X-002 §2.6.2 规定平台只存 Token 摘要。快照文件是平台的一部分，因此它也只接受摘要字段
// token_sha256。明文 Token 不出现在任何磁盘文件里，包括本地开发用的那一份——
// 开发期用 `ingest-gateway -hash-token=<token>` 生成摘要。
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mokaz/ops-system/internal/gateway/authn"
	"github.com/mokaz/ops-system/internal/gateway/kafkasink"
	"github.com/mokaz/ops-system/internal/gateway/ratelimit"
	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// Config 是网关的启动配置。
type Config struct {
	// DataListen 是 OTLP 数据面监听地址。
	DataListen string `json:"data_listen"`
	// AdminListen 是 /metrics 与健康检查的监听地址。
	//
	// 与数据面分开监听是刻意的：数据端口可能终止 TLS、可能被打满，而 X-001 §2.6 要求元集群
	// 在故障时仍能抓到指标——"故障时抓不到指标"等于把最需要观测的时刻变成盲区。
	AdminListen string `json:"admin_listen"`

	TLS TLSConfig `json:"tls"`

	// Signals 是启用的信号。未列出的信号其端点不注册（返回 404）。
	// M1 默认只有 logs 与 traces：metrics 链路属 Track C，metrics-ingest 尚未存在。
	Signals []string `json:"signals"`

	MaxBodyBytes     int64 `json:"max_body_bytes"`
	MaxInflightBytes int64 `json:"max_inflight_bytes"`

	Kafka    KafkaConfig    `json:"kafka"`
	Snapshot SnapshotConfig `json:"snapshot"`
	Limits   LimitsConfig   `json:"limits"`

	// ShutdownGrace 是优雅停机的等待时长。
	//
	// 停机期间必须把在途请求做完：粗暴关闭会让客户端收到连接错误而不是明确的状态码，
	// 而连接错误在部分 SDK 里不会触发持久队列，直接等于丢数。
	ShutdownGrace Duration `json:"shutdown_grace"`
}

// TLSConfig 控制数据面的 TLS 终止。
//
// Enabled 为 false 时网关监听明文 HTTP。这不是降级配置而是正常形态之一：IF-1 要求客户到
// 平台这一跳是 TLS，但 TLS 可以在前置 LB 终止（DD-001 §3.6 的客户侧出口走 443 到平台）。
// 把终止点做成可配置，省掉"为了合规在网关里强开 TLS、结果 LB 到网关又解一次"的双重开销。
type TLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	// ClientCAFile 非空时启用 mTLS。集群级 Token 已是主凭证，mTLS 是企业客户的额外要求，
	// 因此可选而非必选。
	ClientCAFile string `json:"client_ca_file"`
}

// KafkaConfig 是下游配置。
type KafkaConfig struct {
	// Mode 为 "kafka" 或 "memory"。
	//
	// memory 模式用内存 fake 替代 Kafka，供本地开发与 PLAN B7 压测"摘掉 Kafka 单独标定
	// 网关 0.5s 预算"的基线使用。它不写任何持久存储，生产环境配成 memory 等于静默丢数，
	// 因此 Validate 要求显式写出该值，不给默认。
	Mode string `json:"mode"`

	Brokers            []string `json:"brokers"`
	ClientID           string   `json:"client_id"`
	TopicLogs          string   `json:"topic_logs"`
	TopicTraces        string   `json:"topic_traces"`
	MaxBufferedRecords int      `json:"max_buffered_records"`
	ProduceTimeout     Duration `json:"produce_timeout"`
	TLSEnabled         bool     `json:"tls_enabled"`

	// KeyBuckets 为指定租户在分区键上追加 bucket 后缀打散热点（DD-001 §3.3）。
	// 键为 tenant_id，值为后缀基数。
	KeyBuckets map[string]int `json:"key_buckets"`
}

// SnapshotConfig 控制本地快照的加载。
type SnapshotConfig struct {
	Path           string   `json:"path"`
	ReloadInterval Duration `json:"reload_interval"`
	// ClassSTTL 是安全类配置的陈旧阈值（authn.DefaultClassSTTL）。
	ClassSTTL Duration `json:"class_s_ttl"`

	// FreshOnLoad 为 true 时把"加载成功的时刻"当作快照的同步时刻。
	//
	// 默认 false，此时快照年龄取文件的 fetched_at 字段（缺失则取文件 mtime）。这个默认值是
	// 刻意的：快照年龄的意义是"配置有多久没被更新过"，而 M1 没有控制面下发，文件就是权威源，
	// 一份几天没动过的文件确实应该触发 X-001 的"快照陈旧"告警。只有在明知配置是静态的
	// 压测场景下才该把它设为 true。
	FreshOnLoad bool `json:"fresh_on_load"`
}

// LimitsConfig 是限流器的进程级参数。per-cluster 的配额值在快照文件里。
type LimitsConfig struct {
	IdleTTL       Duration `json:"idle_ttl"`
	SweepInterval Duration `json:"sweep_interval"`
}

// Duration 是支持 "500ms" / "2s" 这类字面量的时长。
//
// 自定义类型而不用 int 秒数：配额与超时这类值被写成裸数字时，"3" 是 3 秒还是 3 毫秒只能
// 靠读代码确认，而这类配置的误读代价是直接改变限流与超时行为。
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("时长必须是字符串字面量（如 \"500ms\"）: %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Get 返回时长，零值时回落到 def。
func (d Duration) Get(def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return time.Duration(d)
}

// Default 返回一份可直接用于本地开发的配置（Kafka 为 memory 模式）。
func Default() Config {
	return Config{
		DataListen:       ":4318",
		AdminListen:      ":9464",
		Signals:          []string{"logs", "traces"},
		MaxBodyBytes:     4 << 20,
		MaxInflightBytes: 64 << 20,
		Kafka: KafkaConfig{
			Mode:        "memory",
			TopicLogs:   kafkasink.TopicLogsRaw,
			TopicTraces: kafkasink.TopicTracesRaw,
		},
		Snapshot: SnapshotConfig{ReloadInterval: Duration(15 * time.Second)},
		Limits: LimitsConfig{
			IdleTTL:       Duration(ratelimit.DefaultIdleTTL),
			SweepInterval: Duration(time.Minute),
		},
		ShutdownGrace: Duration(15 * time.Second),
	}
}

// Load 读取并校验配置文件。
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: 读取 %s 失败: %w", path, err)
	}
	cfg := Default()
	dec := json.NewDecoder(bytes.NewReader(raw))
	// 未知字段报错而不是忽略：一个拼错的限流字段被静默忽略，表现为"限流配了但没生效"，
	// 这类故障只会在超限时才暴露。
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: 解析 %s 失败: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate 校验配置。
func (c Config) Validate() error {
	if c.DataListen == "" {
		return errors.New("config: data_listen 不能为空")
	}
	if _, err := c.ParsedSignals(); err != nil {
		return err
	}
	switch c.Kafka.Mode {
	case "kafka":
		if len(c.Kafka.Brokers) == 0 {
			return errors.New("config: kafka.mode=kafka 时必须配置 brokers")
		}
	case "memory":
	default:
		return fmt.Errorf("config: kafka.mode 必须是 kafka 或 memory，收到 %q", c.Kafka.Mode)
	}
	if c.TLS.Enabled && (c.TLS.CertFile == "" || c.TLS.KeyFile == "") {
		return errors.New("config: tls.enabled 时必须配置 cert_file 与 key_file")
	}
	if c.Snapshot.Path == "" {
		return errors.New("config: snapshot.path 不能为空——网关没有其他身份来源")
	}
	return nil
}

// ParsedSignals 解析启用的信号。
func (c Config) ParsedSignals() ([]telemetry.Signal, error) {
	out := make([]telemetry.Signal, 0, len(c.Signals))
	for _, s := range c.Signals {
		sig, ok := telemetry.ParseSignal(s)
		if !ok || !sig.Valid() {
			return nil, fmt.Errorf("config: 未知信号 %q", s)
		}
		out = append(out, sig)
	}
	if len(out) == 0 {
		return nil, errors.New("config: signals 不能为空")
	}
	return out, nil
}

// Topics 返回 topic 映射。
func (c Config) Topics() kafkasink.TopicConfig {
	return kafkasink.TopicConfig{Logs: c.Kafka.TopicLogs, Traces: c.Kafka.TopicTraces}
}

// KeyBuckets 把配置里的租户热点打散表转成 Router 需要的形态。
func (c Config) KeyBuckets() map[identity.TenantID]int {
	if len(c.Kafka.KeyBuckets) == 0 {
		return nil
	}
	out := make(map[identity.TenantID]int, len(c.Kafka.KeyBuckets))
	for k, v := range c.Kafka.KeyBuckets {
		out[identity.TenantID(k)] = v
	}
	return out
}

// --- 快照文件 ----------------------------------------------------------------

// snapshotFile 是快照文件的线上形态。
type snapshotFile struct {
	Version       uint64        `json:"version"`
	FetchedAt     *time.Time    `json:"fetched_at"`
	DefaultLimits *limitsFile   `json:"default_limits"`
	Bindings      []bindingFile `json:"bindings"`
}

type bindingFile struct {
	TokenSHA256   string      `json:"token_sha256"`
	TenantID      string      `json:"tenant_id"`
	ProjectID     string      `json:"project_id"`
	ClusterID     string      `json:"cluster_id"`
	Revoked       bool        `json:"revoked"`
	TenantStatus  string      `json:"tenant_status"`
	ClusterStatus string      `json:"cluster_status"`
	Limits        *limitsFile `json:"limits"`
}

type limitsFile struct {
	BytesPerSec  float64 `json:"bytes_per_sec"`
	BytesBurst   float64 `json:"bytes_burst"`
	EventsPerSec float64 `json:"events_per_sec"`
	EventsBurst  float64 `json:"events_burst"`
}

func (l *limitsFile) toLimits() ratelimit.Limits {
	if l == nil {
		return ratelimit.Limits{}
	}
	return ratelimit.Limits{
		BytesPerSec:  l.BytesPerSec,
		BytesBurst:   l.BytesBurst,
		EventsPerSec: l.EventsPerSec,
		EventsBurst:  l.EventsBurst,
	}
}

// Snapshots 是一次快照加载的结果：鉴权与配额来自同一份文件。
//
// 合在一个文件里而不是两份：两者都按 cluster 为粒度，分成两个文件就会出现"身份有、配额没"
// 的中间态，而那种中间态下网关要么放行无限流量、要么拒绝合法租户，两个结果都不对。
type Snapshots struct {
	Auth  *authn.Snapshot
	Quota *ratelimit.Snapshot
}

// LoadSnapshots 读取本地快照文件。
func LoadSnapshots(cfg SnapshotConfig) (Snapshots, error) {
	raw, err := os.ReadFile(cfg.Path)
	if err != nil {
		return Snapshots{}, fmt.Errorf("config: 读取快照 %s 失败: %w", cfg.Path, err)
	}

	var sf snapshotFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sf); err != nil {
		return Snapshots{}, fmt.Errorf("config: 解析快照 %s 失败: %w", cfg.Path, err)
	}

	fetchedAt, err := snapshotTime(cfg, sf.FetchedAt)
	if err != nil {
		return Snapshots{}, err
	}

	bindings := make(map[identity.TokenHash]identity.Binding, len(sf.Bindings))
	perKey := make(map[identity.QuotaKey]ratelimit.Limits, len(sf.Bindings))
	for i, b := range sf.Bindings {
		if b.TokenSHA256 == "" {
			return Snapshots{}, fmt.Errorf("config: 快照第 %d 条缺少 token_sha256", i)
		}
		if len(b.TokenSHA256) != 64 {
			// 明文 Token 被误写进摘要字段是最可能的配置错误，而它的后果是该租户永远无法
			// 鉴权成功——报错比静默接受一个永不命中的摘要好。
			return Snapshots{}, fmt.Errorf(
				"config: 快照第 %d 条的 token_sha256 长度应为 64 位十六进制（是否误填了明文 Token？）", i)
		}
		tenantStatus, err := parseStatus(b.TenantStatus)
		if err != nil {
			return Snapshots{}, fmt.Errorf("config: 快照第 %d 条 tenant_status: %w", i, err)
		}
		clusterStatus, err := parseStatus(b.ClusterStatus)
		if err != nil {
			return Snapshots{}, fmt.Errorf("config: 快照第 %d 条 cluster_status: %w", i, err)
		}

		hash := identity.TokenHash(b.TokenSHA256)
		binding := identity.Binding{
			TenantID:      identity.TenantID(b.TenantID),
			ProjectID:     identity.ProjectID(b.ProjectID),
			ClusterID:     identity.ClusterID(b.ClusterID),
			TokenHash:     hash,
			Revoked:       b.Revoked,
			TenantStatus:  tenantStatus,
			ClusterStatus: clusterStatus,
		}
		if !binding.Valid() {
			return Snapshots{}, fmt.Errorf("config: 快照第 %d 条缺少 tenant_id 或 cluster_id", i)
		}
		if _, dup := bindings[hash]; dup {
			// 同一个摘要出现两次意味着两套身份抢同一个 Token，谁生效取决于 map 的遍历顺序。
			return Snapshots{}, fmt.Errorf("config: 快照第 %d 条的 token_sha256 重复", i)
		}
		bindings[hash] = binding

		if b.Limits != nil {
			perKey[identity.QuotaKey{
				TenantID:  binding.TenantID,
				ClusterID: binding.ClusterID,
			}] = b.Limits.toLimits()
		}
	}

	return Snapshots{
		Auth: &authn.Snapshot{
			Version:   sf.Version,
			FetchedAt: fetchedAt,
			Bindings:  bindings,
		},
		Quota: &ratelimit.Snapshot{
			Version:   sf.Version,
			FetchedAt: fetchedAt,
			Default:   sf.DefaultLimits.toLimits(),
			PerKey:    perKey,
		},
	}, nil
}

func snapshotTime(cfg SnapshotConfig, declared *time.Time) (time.Time, error) {
	if cfg.FreshOnLoad {
		return time.Now(), nil
	}
	if declared != nil && !declared.IsZero() {
		return *declared, nil
	}
	st, err := os.Stat(cfg.Path)
	if err != nil {
		return time.Time{}, fmt.Errorf("config: 快照缺少 fetched_at 且无法取 mtime: %w", err)
	}
	return st.ModTime(), nil
}

func parseStatus(s string) (identity.Status, error) {
	switch s {
	case "", "active":
		// 缺省视为 active：快照里出现一条身份就意味着控制面认为它存在。
		// 把缺省读成 unknown 会让一份简写的快照全体不可服务。
		return identity.StatusActive, nil
	case "suspended":
		return identity.StatusSuspended, nil
	case "offboarding":
		return identity.StatusOffboarding, nil
	default:
		return identity.StatusUnknown, fmt.Errorf("未知状态 %q", s)
	}
}
