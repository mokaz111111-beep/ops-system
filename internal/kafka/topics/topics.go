// Package topics 是 IF-2 的可执行规格：topic 名、分区数、保留期、副本与 ISR。
//
// 生产值与 PLAN B2 / DD-001 §3.3 一致。开发环境单节点 Kafka 无法放下 3 副本，
// 因此 ProfileDev 把副本因子和 min.insync.replicas 降到 1——这是环境降级，
// 不是契约放宽。分区数与 12 小时保留期两端相同，不得在开发环境偷偷改小。
package topics

import (
	"fmt"
	"strconv"
	"time"
)

const (
	LogsRaw   = "logs.raw"
	TracesRaw = "traces.raw"

	LogsPartitions   = 12
	TracesPartitions = 6

	// Retention 是落库链路故障必须恢复完毕的硬期限（SD-000 G7 / DD-001 §3.3）。
	Retention = 12 * time.Hour

	// ProductionReplication 是 IF-2 的生产副本因子。
	ProductionReplication = 3
	// ProductionMinISR 是 IF-2 的生产 min.insync.replicas。
	// 与生产者 acks=all 配套：ISR < 2 时写入失败，而不是只落一台就回 200。
	ProductionMinISR = 2

	// DevReplication / DevMinISR 是单节点开发集群的降级值。
	// 单 broker 的 ISR 集合只有 1，写 3 或 2 会让 topic 永远建不出来或永远不可写。
	DevReplication = 1
	DevMinISR      = 1
)

// Profile 选择副本策略。分区数与保留期不随 Profile 变化。
type Profile string

const (
	ProfileProd Profile = "prod"
	ProfileDev  Profile = "dev"
)

// ParseProfile 解析 -profile / 环境变量。
func ParseProfile(s string) (Profile, error) {
	switch Profile(s) {
	case ProfileProd, ProfileDev:
		return Profile(s), nil
	default:
		return "", fmt.Errorf("topics: profile 必须是 prod 或 dev，收到 %q", s)
	}
}

// ReplicaPolicy 是副本因子与 ISR 下限。
type ReplicaPolicy struct {
	ReplicationFactor int
	MinInSyncReplicas int
	// Degraded 为 true 表示相对 IF-2 做了环境降级。
	Degraded bool
}

// Policy 返回该 Profile 的副本策略。
func Policy(p Profile) (ReplicaPolicy, error) {
	switch p {
	case ProfileProd:
		return ReplicaPolicy{
			ReplicationFactor: ProductionReplication,
			MinInSyncReplicas: ProductionMinISR,
		}, nil
	case ProfileDev:
		return ReplicaPolicy{
			ReplicationFactor: DevReplication,
			MinInSyncReplicas: DevMinISR,
			Degraded:          true,
		}, nil
	default:
		return ReplicaPolicy{}, fmt.Errorf("topics: 未知 profile %q", p)
	}
}

// Spec 是一个 topic 的期望状态。
type Spec struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string
}

// RetentionMillis 返回 retention.ms 的取值。
func RetentionMillis() int64 { return Retention.Milliseconds() }

// Desired 返回 IF-2 两个 topic 在指定 Profile 下的期望状态。
func Desired(p Profile) ([]Spec, error) {
	pol, err := Policy(p)
	if err != nil {
		return nil, err
	}
	cfg := map[string]string{
		"retention.ms":                   strconv.FormatInt(RetentionMillis(), 10),
		"min.insync.replicas":            strconv.Itoa(pol.MinInSyncReplicas),
		"compression.type":               "lz4",
		"unclean.leader.election.enable": "false",
	}
	return []Spec{
		{
			Name:              LogsRaw,
			Partitions:        LogsPartitions,
			ReplicationFactor: pol.ReplicationFactor,
			Configs:           cfg,
		},
		{
			Name:              TracesRaw,
			Partitions:        TracesPartitions,
			ReplicationFactor: pol.ReplicationFactor,
			Configs:           cfg,
		},
	}, nil
}
