package config

import (
	"os"
	"strings"
	"time"
)

// 环境变量覆盖本地文件。这不是控制面：变量在进程启动时读一次，请求路径不再看。
// 用来在 compose / systemd 里注入 broker 列表，避免为每个环境复制一份 JSON。

const (
	envKafkaMode      = "OPS_KAFKA_MODE"
	envKafkaBrokers   = "OPS_KAFKA_BROKERS"
	envKafkaClientID  = "OPS_KAFKA_CLIENT_ID"
	envKafkaTopicLogs = "OPS_KAFKA_TOPIC_LOGS"
	envKafkaTopicTr   = "OPS_KAFKA_TOPIC_TRACES"
	envKafkaTLS       = "OPS_KAFKA_TLS"
	envKafkaTimeout   = "OPS_KAFKA_PRODUCE_TIMEOUT"
	envSnapshotPath   = "OPS_SNAPSHOT_PATH"
	envDataListen     = "OPS_DATA_LISTEN"
)

// ApplyEnv 用环境变量覆盖 cfg 中已设置的对应字段。未设置的变量不改动。
func ApplyEnv(cfg *Config) error {
	if v := strings.TrimSpace(os.Getenv(envKafkaMode)); v != "" {
		cfg.Kafka.Mode = v
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaBrokers)); v != "" {
		cfg.Kafka.Brokers = splitCSV(v)
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaClientID)); v != "" {
		cfg.Kafka.ClientID = v
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaTopicLogs)); v != "" {
		cfg.Kafka.TopicLogs = v
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaTopicTr)); v != "" {
		cfg.Kafka.TopicTraces = v
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaTLS)); v != "" {
		on, err := parseBoolEnv(v)
		if err != nil {
			return err
		}
		cfg.Kafka.TLSEnabled = on
	}
	if v := strings.TrimSpace(os.Getenv(envKafkaTimeout)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		cfg.Kafka.ProduceTimeout = Duration(d)
	}
	if v := strings.TrimSpace(os.Getenv(envSnapshotPath)); v != "" {
		cfg.Snapshot.Path = v
	}
	if v := strings.TrimSpace(os.Getenv(envDataListen)); v != "" {
		cfg.DataListen = v
	}
	return nil
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

func parseBoolEnv(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, errBadBool(s)
	}
}

type envError string

func (e envError) Error() string { return string(e) }

func errBadBool(s string) error {
	return envError("config: OPS_KAFKA_TLS 必须是 true/false，收到 " + s)
}
