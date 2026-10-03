package topics

import (
	"reflect"
	"testing"
	"time"
)

func TestIF2ProductionValues(t *testing.T) {
	if LogsRaw != "logs.raw" || TracesRaw != "traces.raw" {
		t.Fatalf("topic 名偏离 IF-2: %s %s", LogsRaw, TracesRaw)
	}
	if LogsPartitions != 12 || TracesPartitions != 6 {
		t.Fatalf("分区数偏离 IF-2: logs=%d traces=%d", LogsPartitions, TracesPartitions)
	}
	if Retention != 12*time.Hour {
		t.Fatalf("保留期偏离 IF-2: %s", Retention)
	}
	if ProductionReplication != 3 || ProductionMinISR != 2 {
		t.Fatalf("生产副本偏离 IF-2: rf=%d minISR=%d", ProductionReplication, ProductionMinISR)
	}
}

func TestDevIsDegradedNotPretendProd(t *testing.T) {
	dev, err := Policy(ProfileDev)
	if err != nil {
		t.Fatal(err)
	}
	if !dev.Degraded {
		t.Fatal("单节点开发环境必须显式标记为降级，不能假装满足 3 副本")
	}
	if dev.ReplicationFactor != 1 || dev.MinInSyncReplicas != 1 {
		t.Fatalf("开发降级值不对: %+v", dev)
	}

	prod, err := Policy(ProfileProd)
	if err != nil {
		t.Fatal(err)
	}
	if prod.Degraded {
		t.Fatal("生产 profile 不得标记降级")
	}
	if prod.ReplicationFactor != 3 || prod.MinInSyncReplicas != 2 {
		t.Fatalf("生产值: %+v", prod)
	}
}

func TestDesiredKeepsPartitionsAndRetention(t *testing.T) {
	for _, p := range []Profile{ProfileDev, ProfileProd} {
		specs, err := Desired(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(specs) != 2 {
			t.Fatalf("应有 logs/traces 两个 topic，得到 %d", len(specs))
		}
		if specs[0].Name != LogsRaw || specs[0].Partitions != 12 {
			t.Errorf("%s logs: %+v", p, specs[0])
		}
		if specs[1].Name != TracesRaw || specs[1].Partitions != 6 {
			t.Errorf("%s traces: %+v", p, specs[1])
		}
		if specs[0].Configs["retention.ms"] != "43200000" {
			t.Errorf("%s retention.ms=%s", p, specs[0].Configs["retention.ms"])
		}
	}
}

func TestParseProfile(t *testing.T) {
	if _, err := ParseProfile("staging"); err == nil {
		t.Fatal("未知 profile 必须失败")
	}
	p, err := ParseProfile("dev")
	if err != nil || p != ProfileDev {
		t.Fatalf("got %q %v", p, err)
	}
}

func TestDiffCreateIncreaseAlterAndRFMismatch(t *testing.T) {
	desired, err := Desired(ProfileDev)
	if err != nil {
		t.Fatal(err)
	}

	empty := Diff(desired, nil)
	if len(empty.Actions) != 2 || empty.Actions[0].Kind != ActionCreate {
		t.Fatalf("空集群应创建两个 topic: %+v", empty)
	}

	have := []Existing{
		{
			Name: LogsRaw, Partitions: 3, ReplicationFactor: 1,
			Configs: map[string]string{"retention.ms": "3600000", "min.insync.replicas": "1", "compression.type": "lz4", "unclean.leader.election.enable": "false"},
		},
		{
			Name: TracesRaw, Partitions: 6, ReplicationFactor: 1,
			Configs: desired[1].Configs,
		},
	}
	plan := Diff(desired, have)
	if plan.Empty() {
		t.Fatal("logs 分区不足且保留期不对，计划不应为空")
	}
	var kinds []ActionKind
	for _, a := range plan.Actions {
		if a.Spec.Name == LogsRaw {
			kinds = append(kinds, a.Kind)
		}
	}
	want := []ActionKind{ActionIncreasePartitions, ActionAlterConfig}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("logs 动作: %v", kinds)
	}

	have[0].Partitions = 24
	have[0].Configs = desired[0].Configs
	have[0].ReplicationFactor = 3
	plan = Diff(desired, have)
	if len(plan.Problems) == 0 {
		t.Fatal("副本因子 3 vs 开发规格 1 必须视为不可自动修复")
	}
	for _, a := range plan.Actions {
		if a.Kind == ActionIncreasePartitions {
			t.Fatal("24 > 12 不得回滚分区")
		}
	}
}
