package kafkasink

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/ingestmsg"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

// TestProduceLiveKafka 对着已经起来的集群打一条消息。
//
// 默认 skip：CI 和没有 Docker 的开发机不能依赖本机 Kafka。
// 本地验证：先 deploy/kafka/verify.ps1（或 compose + ensure-kafka-topics），再
//
//	$env:OPS_KAFKA_IT=1; go test ./internal/gateway/kafkasink -run TestProduceLiveKafka
func TestProduceLiveKafka(t *testing.T) {
	if os.Getenv("OPS_KAFKA_IT") != "1" {
		t.Skip("optional live Kafka; set OPS_KAFKA_IT=1 after compose + ensure-kafka-topics")
	}

	brokers := os.Getenv("OPS_KAFKA_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:9092"
	}

	k, err := NewKafka(KafkaConfig{
		Brokers:        splitCSV(brokers),
		ClientID:       "kafkasink-it",
		ProduceTimeout: 8 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	r := &Router{Topics: DefaultTopics()}
	rec, err := r.Route(ingestmsg.Meta{
		TenantID:    "org-dev",
		ProjectID:   "prj-local",
		ClusterID:   "c-local-01",
		Signal:      telemetry.SignalLogs,
		RecordCount: 1,
		ReceivedAt:  time.Now().UTC(),
		ServiceName: "b2-it",
	}, []byte("b2-live-kafka"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k.Produce(ctx, rec); err != nil {
		t.Fatalf("写入 %s 失败（topic 是否已用 -profile dev 建好？）: %v", rec.Topic, err)
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
