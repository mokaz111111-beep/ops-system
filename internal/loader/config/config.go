// Package config 加载 otlp-loader 的本地配置。
//
// 只读本地文件与启动时的环境变量，没有控制面客户端。PLAN §3：M1 配置硬编码即可，
// 但不得写出消费路径同步查控制面的代码——那个结构现在不建，后面改造成本极高。
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mokaz/ops-system/internal/kafka/topics"
	"github.com/mokaz/ops-system/pkg/loadbatch"
)

// Config 是 loader 的启动配置。
type Config struct {
	AdminListen string `json:"admin_listen"`

	Kafka KafkaConfig `json:"kafka"`
	Doris DorisConfig `json:"doris"`
	Batch BatchConfig `json:"batch"`

	ShutdownGrace Duration `json:"shutdown_grace"`
}

// KafkaConfig 是消费端。
type KafkaConfig struct {
	// Mode 为 "kafka" 或 "memory"。memory 只给测试与无 broker 的冒烟；
	// 生产配成 memory 等于不消费任何数据。Validate 要求显式写出。
	Mode     string   `json:"mode"`
	Brokers  []string `json:"brokers"`
	GroupID  string   `json:"group_id"`
	ClientID string   `json:"client_id"`
	Topics   []string `json:"topics"`
	TLSEnabled bool   `json:"tls_enabled"`
}

// DorisConfig 是 Stream Load 目标。
type DorisConfig struct {
	// Mode 为 "http" 或 "fake"。
	Mode       string `json:"mode"`
	FE         string `json:"fe"`
	Database   string `json:"database"`
	User       string `json:"user"`
	TableLogs  string `json:"table_logs"`
	TableSpans string `json:"table_spans"`
	// Password 不出现在 JSON 里，只从 OPS_DORIS_PASSWORD 注入。
	Password string `json:"-"`
}

// BatchConfig 是已签字的切批参数。改 stride / max_encoded_bytes 属 IF-3。
type BatchConfig struct {
	Stride          int64    `json:"stride"`
	MaxEncodedBytes int64    `json:"max_encoded_bytes"`
	IdleSubmit      Duration `json:"idle_submit"`
}

// Duration 支持 "2s" 字面量。
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("时长必须是字符串字面量（如 \"2s\"）: %w", err)
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

func (d Duration) Get(def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return time.Duration(d)
}

// Default 返回本机冒烟配置（Kafka / Doris 都是 memory/fake）。
func Default() Config {
	return Config{
		AdminListen: ":9465",
		Kafka: KafkaConfig{
			Mode:     "memory",
			GroupID:  "otlp-loader",
			ClientID: "otlp-loader",
			Topics:   []string{topics.LogsRaw, topics.TracesRaw},
		},
		Doris: DorisConfig{
			Mode:       "fake",
			Database:   "ops",
			TableLogs:  "logs",
			TableSpans: "spans",
		},
		Batch: BatchConfig{
			Stride:          loadbatch.DefaultStride,
			MaxEncodedBytes: loadbatch.DefaultMaxEncodedBytes,
			IdleSubmit:      Duration(2 * time.Second),
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
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: 解析 %s 失败: %w", path, err)
	}
	ApplyEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

const (
	envKafkaMode    = "OPS_KAFKA_MODE"
	envKafkaBrokers = "OPS_KAFKA_BROKERS"
	envKafkaGroup   = "OPS_KAFKA_GROUP"
	envDorisMode    = "OPS_DORIS_MODE"
	envDorisFE      = "OPS_DORIS_FE"
	envDorisUser    = "OPS_DORIS_USER"
	envDorisPass    = "OPS_DORIS_PASSWORD"
	envAdminListen  = "OPS_ADMIN_LISTEN"
)

// ApplyEnv 在启动时覆盖文件值。消费路径不再读环境、更不查控制面。
func ApplyEnv(cfg *Config) {
	if v := strings.TrimSpace(os.Getenv(envKafkaMode)); v != "" {
		cfg.Kafka.Mode = v
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaBrokers)); v != "" {
		cfg.Kafka.Brokers = splitCSV(v)
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaGroup)); v != "" {
		cfg.Kafka.GroupID = v
	}
	if v := strings.TrimSpace(os.Getenv(envDorisMode)); v != "" {
		cfg.Doris.Mode = v
	}
	if v := strings.TrimSpace(os.Getenv(envDorisFE)); v != "" {
		cfg.Doris.FE = v
	}
	if v := strings.TrimSpace(os.Getenv(envDorisUser)); v != "" {
		cfg.Doris.User = v
	}
	if v := os.Getenv(envDorisPass); v != "" {
		cfg.Doris.Password = v
	}
	if v := strings.TrimSpace(os.Getenv(envAdminListen)); v != "" {
		cfg.AdminListen = v
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Validate 校验配置。
func (c Config) Validate() error {
	if c.AdminListen == "" {
		return errors.New("config: admin_listen 不能为空")
	}
	switch c.Kafka.Mode {
	case "kafka":
		if len(c.Kafka.Brokers) == 0 {
			return errors.New("config: kafka.mode=kafka 时必须配置 brokers")
		}
		if c.Kafka.GroupID == "" {
			return errors.New("config: kafka.mode=kafka 时必须配置 group_id")
		}
	case "memory":
	default:
		return fmt.Errorf("config: kafka.mode 必须是 kafka 或 memory，收到 %q", c.Kafka.Mode)
	}
	if len(c.Kafka.Topics) == 0 {
		return errors.New("config: kafka.topics 不能为空")
	}
	switch c.Doris.Mode {
	case "http":
		if c.Doris.FE == "" || c.Doris.Database == "" {
			return errors.New("config: doris.mode=http 时必须配置 fe 与 database")
		}
	case "fake":
	default:
		return fmt.Errorf("config: doris.mode 必须是 http 或 fake，收到 %q", c.Doris.Mode)
	}
	if c.Batch.Stride < 0 || c.Batch.MaxEncodedBytes < 0 {
		return errors.New("config: batch 参数不能为负")
	}
	return nil
}

// Params 返回切批参数；零值回落到已签字默认。
func (c Config) Params() loadbatch.Params {
	return loadbatch.Params{
		Stride:          c.Batch.Stride,
		MaxEncodedBytes: c.Batch.MaxEncodedBytes,
	}
}
