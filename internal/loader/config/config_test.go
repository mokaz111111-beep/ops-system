package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mokaz/ops-system/pkg/loadbatch"
)

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		envKafkaMode, envKafkaBrokers, envKafkaGroup,
		envDorisMode, envDorisFE, envDorisUser, envDorisPass, envAdminListen,
	} {
		t.Setenv(k, "")
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	clearEnv(t)
	path := filepath.Join(t.TempDir(), "cfg.json")
	if err := os.WriteFile(path, []byte(`{"admin_listen":":1","control_plane_url":"http://x"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("未知字段必须报错——尤其是控制面地址，不能被静默忽略")
	}
}

func TestDefaultStrideAndCapAreSignedValues(t *testing.T) {
	c := Default()
	if c.Batch.Stride != loadbatch.DefaultStride || c.Batch.MaxEncodedBytes != loadbatch.DefaultMaxEncodedBytes {
		t.Fatalf("默认切批参数漂了: %+v", c.Batch)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateModes(t *testing.T) {
	c := Default()
	c.Kafka.Mode = "kafka"
	if err := c.Validate(); err == nil {
		t.Fatal("kafka 模式无 brokers 必须失败")
	}
	c.Kafka.Brokers = []string{"127.0.0.1:9092"}
	c.Doris.Mode = "http"
	if err := c.Validate(); err == nil {
		t.Fatal("http 模式无 FE 必须失败")
	}
}

func TestPasswordOnlyFromEnv(t *testing.T) {
	clearEnv(t)
	path := filepath.Join(t.TempDir(), "cfg.json")
	body := `{"admin_listen":":9465","kafka":{"mode":"memory","topics":["logs.raw"]},"doris":{"mode":"fake"}}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envDorisPass, "s3cret")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Doris.Password != "s3cret" {
		t.Fatal("密码必须从环境注入，不得写进 JSON")
	}
	raw, _ := os.ReadFile(path)
	if strings.Contains(string(raw), "s3cret") {
		t.Fatal("示例文件里不得出现密码")
	}
}
