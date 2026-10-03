package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
)

func TestLoadRejectsUnknownField(t *testing.T) {
	clearKafkaEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	if err := os.WriteFile(path, []byte(`{"data_listen":":1","snapshot":{"path":"x"},"typo_limit":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown") && !strings.Contains(err.Error(), "typo") {
		t.Fatalf("未知字段必须报错，得到 %v", err)
	}
}

func TestValidateKafkaAndTLS(t *testing.T) {
	c := Default()
	c.Snapshot.Path = "snap.json"
	if err := c.Validate(); err != nil {
		t.Fatalf("默认 memory 配置应合法: %v", err)
	}
	c.Kafka.Mode = "kafka"
	if err := c.Validate(); err == nil {
		t.Fatal("kafka 模式无 brokers 必须失败")
	}
	c.Kafka.Mode = "redis"
	if err := c.Validate(); err == nil {
		t.Fatal("未知 mode 必须失败")
	}
	c.Kafka.Mode = "memory"
	c.TLS.Enabled = true
	if err := c.Validate(); err == nil {
		t.Fatal("TLS 开启无证书必须失败")
	}
}

func TestLoadSnapshots(t *testing.T) {
	dir := t.TempDir()
	hash, err := identity.HashToken("tok-dev-local")
	if err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(dir, "snap.json")
	body := `{
		"version": 3,
		"fetched_at": "2026-10-03T00:00:00Z",
		"default_limits": {"bytes_per_sec": 1000, "events_per_sec": 10},
		"bindings": [{
			"token_sha256": "` + string(hash) + `",
			"tenant_id": "org-1",
			"project_id": "prj-prod",
			"cluster_id": "c1",
			"limits": {"bytes_per_sec": 2000, "events_per_sec": 20}
		}]
	}`
	if err := os.WriteFile(snap, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := LoadSnapshots(SnapshotConfig{Path: snap})
	if err != nil {
		t.Fatal(err)
	}
	if out.Auth.Version != 3 || len(out.Auth.Bindings) != 1 {
		t.Fatalf("鉴权快照不对: %+v", out.Auth)
	}
	b := out.Auth.Bindings[hash]
	if b.TenantID != "org-1" || b.ProjectID != "prj-prod" || b.ClusterID != "c1" {
		t.Errorf("身份: %+v", b)
	}
	lim := out.Quota.PerKey[identity.QuotaKey{TenantID: "org-1", ClusterID: "c1"}]
	if lim.BytesPerSec != 2000 {
		t.Errorf("专属配额: %+v", lim)
	}
	if out.Quota.Default.BytesPerSec != 1000 {
		t.Errorf("默认配额: %+v", out.Quota.Default)
	}
}

func TestLoadSnapshotsRejectsPlainToken(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "snap.json")
	if err := os.WriteFile(snap, []byte(`{"bindings":[{"token_sha256":"not-a-hash","tenant_id":"a","cluster_id":"b"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnapshots(SnapshotConfig{Path: snap}); err == nil {
		t.Fatal("明文 Token 误填必须被拒")
	}
}

func TestLoadSnapshotsRejectsDuplicateHash(t *testing.T) {
	dir := t.TempDir()
	hash, err := identity.HashToken("tok")
	if err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(dir, "snap.json")
	body := `{"bindings":[
		{"token_sha256":"` + string(hash) + `","tenant_id":"a","cluster_id":"c1"},
		{"token_sha256":"` + string(hash) + `","tenant_id":"b","cluster_id":"c2"}
	]}`
	if err := os.WriteFile(snap, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnapshots(SnapshotConfig{Path: snap}); err == nil {
		t.Fatal("重复摘要必须被拒")
	}
}

func TestDurationJSON(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"1500ms"`)); err != nil {
		t.Fatal(err)
	}
	if time.Duration(d) != 1500*time.Millisecond {
		t.Errorf("得到 %v", time.Duration(d))
	}
	if err := d.UnmarshalJSON([]byte(`3`)); err == nil {
		t.Fatal("裸数字必须拒绝")
	}
}

func TestParsedSignals(t *testing.T) {
	c := Default()
	c.Snapshot.Path = "x"
	sigs, err := c.ParsedSignals()
	if err != nil || len(sigs) != 2 {
		t.Fatalf("默认信号: %v %v", sigs, err)
	}
	c.Signals = []string{"nope"}
	if _, err := c.ParsedSignals(); err == nil {
		t.Fatal("未知信号必须失败")
	}
}

func TestApplyEnvOverridesKafka(t *testing.T) {
	t.Setenv("OPS_KAFKA_MODE", "kafka")
	t.Setenv("OPS_KAFKA_BROKERS", "127.0.0.1:9092, 10.0.0.2:9092")
	t.Setenv("OPS_KAFKA_CLIENT_ID", "gw-it")
	t.Setenv("OPS_KAFKA_PRODUCE_TIMEOUT", "1500ms")
	t.Setenv("OPS_SNAPSHOT_PATH", "snap.json")

	c := Default()
	if err := ApplyEnv(&c); err != nil {
		t.Fatal(err)
	}
	if c.Kafka.Mode != "kafka" {
		t.Fatalf("mode=%s", c.Kafka.Mode)
	}
	if len(c.Kafka.Brokers) != 2 || c.Kafka.Brokers[0] != "127.0.0.1:9092" {
		t.Fatalf("brokers=%v", c.Kafka.Brokers)
	}
	if c.Kafka.ClientID != "gw-it" {
		t.Fatalf("client_id=%s", c.Kafka.ClientID)
	}
	if time.Duration(c.Kafka.ProduceTimeout) != 1500*time.Millisecond {
		t.Fatalf("timeout=%s", time.Duration(c.Kafka.ProduceTimeout))
	}
	if c.Snapshot.Path != "snap.json" {
		t.Fatalf("snapshot=%s", c.Snapshot.Path)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestExampleGatewayConfigsLoad(t *testing.T) {
	clearKafkaEnv(t)
	root := findRepoRoot(t)
	for _, rel := range []string{
		"deploy/ingest-gateway/gateway.memory.json",
		"deploy/ingest-gateway/gateway.kafka.json",
	} {
		cfg, err := Load(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if cfg.Snapshot.Path == "" {
			t.Fatalf("%s 缺少 snapshot.path", rel)
		}
	}
}

func clearKafkaEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"OPS_KAFKA_MODE", "OPS_KAFKA_BROKERS", "OPS_KAFKA_CLIENT_ID",
		"OPS_KAFKA_TOPIC_LOGS", "OPS_KAFKA_TOPIC_TRACES", "OPS_KAFKA_TLS",
		"OPS_KAFKA_PRODUCE_TIMEOUT", "OPS_SNAPSHOT_PATH", "OPS_DATA_LISTEN",
	} {
		t.Setenv(k, "")
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("找不到仓库根（go.mod）")
	return ""
}
