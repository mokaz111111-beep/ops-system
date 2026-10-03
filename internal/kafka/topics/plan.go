package topics

import (
	"fmt"
	"sort"
)

// Existing 是集群里已经存在的 topic 快照。
type Existing struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Configs           map[string]string
}

// Action 是一次 ensure 计划里的动作。
type ActionKind string

const (
	ActionCreate             ActionKind = "create"
	ActionIncreasePartitions ActionKind = "increase_partitions"
	ActionAlterConfig        ActionKind = "alter_config"
)

// Action 描述要对一个 topic 做的事。
type Action struct {
	Kind ActionKind
	Spec Spec
	// FromPartitions 仅在 increase_partitions 时有意义。
	FromPartitions int
	// ConfigKeys 仅在 alter_config 时有意义。
	ConfigKeys []string
}

// Plan 是纯函数计算结果：不连 Kafka，便于单测。
type Plan struct {
	Actions []Action
	// Problems 是不能自动修复的偏差（例如副本因子对不上）。
	// 出现任一条则 Ensure 必须失败，避免把错误副本数当成成功。
	Problems []string
}

// Empty 报告集群已与规格一致。
func (p Plan) Empty() bool { return len(p.Actions) == 0 && len(p.Problems) == 0 }

// Diff 比较期望与现状。
//
// 分区只增不减（DD-001 §3.3）：现网分区更多是合法预扩容，不回滚。
// 副本因子不一致不能靠本工具改——那是分区重分配，误跑会把开发集群当成生产。
func Diff(desired []Spec, have []Existing) Plan {
	byName := make(map[string]Existing, len(have))
	for _, e := range have {
		byName[e.Name] = e
	}

	var plan Plan
	for _, spec := range desired {
		e, ok := byName[spec.Name]
		if !ok {
			plan.Actions = append(plan.Actions, Action{Kind: ActionCreate, Spec: spec})
			continue
		}

		if e.Partitions < spec.Partitions {
			plan.Actions = append(plan.Actions, Action{
				Kind:           ActionIncreasePartitions,
				Spec:           spec,
				FromPartitions: e.Partitions,
			})
		}
		// 现网分区更多：合法预扩容（只增不减），不回滚、不报错。

		if e.ReplicationFactor != spec.ReplicationFactor {
			plan.Problems = append(plan.Problems, fmt.Sprintf(
				"%s 副本因子是 %d，规格为 %d；拒绝自动重分配，请换 profile 或手工 reassign",
				spec.Name, e.ReplicationFactor, spec.ReplicationFactor))
		}

		var keys []string
		for k, want := range spec.Configs {
			got, exists := e.Configs[k]
			if !exists || got != want {
				keys = append(keys, k)
			}
		}
		if len(keys) > 0 {
			sort.Strings(keys)
			plan.Actions = append(plan.Actions, Action{
				Kind:       ActionAlterConfig,
				Spec:       spec,
				ConfigKeys: keys,
			})
		}
	}
	return plan
}
