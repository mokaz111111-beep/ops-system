// Command ensure-kafka-topics 按 IF-2 创建或核对 logs.raw / traces.raw。
//
// 生产：12 / 6 分区、3 副本、min.insync.replicas=2、保留 12 小时。
// 开发：分区与保留期不变，副本降到 1——单节点 Kafka 放不下 3 副本，
// 本命令拒绝把开发集群伪装成已满足 PLAN B2 的生产判据。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mokaz/ops-system/internal/kafka/topics"
)

func main() {
	var (
		brokers   = flag.String("brokers", envOr("OPS_KAFKA_BROKERS", "127.0.0.1:9092"), "broker 列表，逗号分隔")
		profile   = flag.String("profile", envOr("OPS_KAFKA_PROFILE", "dev"), "副本策略：dev（RF=1）或 prod（RF=3, min.ISR=2）")
		checkOnly = flag.Bool("check-only", false, "只核对，不创建、不改配置")
		timeout   = flag.Duration("timeout", 20*time.Second, "连集群与变更的超时")
	)
	flag.Parse()

	p, err := topics.ParseProfile(*profile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	seeds := splitCSV(*brokers)
	res, err := topics.Ensure(ctx, seeds, p, topics.EnsureOptions{
		CheckOnly: *checkOnly,
		ClientID:  "ensure-kafka-topics",
	})
	printResult(res)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *checkOnly {
		fmt.Println("topic 与 IF-2 规格一致")
		return
	}
	if len(res.Applied) == 0 {
		fmt.Println("无需变更")
		return
	}
	fmt.Printf("已应用 %d 个动作\n", len(res.Applied))
}

func printResult(res topics.Result) {
	fmt.Printf("profile=%s rf=%d min.isr=%d degraded=%v retention=%s\n",
		res.Profile, res.Policy.ReplicationFactor, res.Policy.MinInSyncReplicas,
		res.Policy.Degraded, topics.Retention)
	if res.Policy.Degraded {
		fmt.Println("注意：这是单节点开发降级，不是 PLAN B2 的生产副本（3 + min.isr=2）")
	}
	for _, a := range res.Plan.Actions {
		switch a.Kind {
		case topics.ActionCreate:
			fmt.Printf("  create %s partitions=%d rf=%d\n", a.Spec.Name, a.Spec.Partitions, a.Spec.ReplicationFactor)
		case topics.ActionIncreasePartitions:
			fmt.Printf("  increase %s partitions %d -> %d\n", a.Spec.Name, a.FromPartitions, a.Spec.Partitions)
		case topics.ActionAlterConfig:
			fmt.Printf("  alter %s configs=%v\n", a.Spec.Name, a.ConfigKeys)
		}
	}
	for _, p := range res.Plan.Problems {
		fmt.Printf("  problem: %s\n", p)
	}
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
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
