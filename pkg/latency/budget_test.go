package latency

import (
	"testing"
	"time"
)

func TestBudgetsSumToSLO(t *testing.T) {
	if got := TotalBudget() + Unallocated; got != SLOIngestEndToEnd {
		t.Errorf("五段预算 %v + 余量 %v = %v，不等于 SLO-1 的 %v",
			TotalBudget(), Unallocated, got, SLOIngestEndToEnd)
	}
}

func TestFiveStagesInOrder(t *testing.T) {
	want := []Stage{StageCollectorBatch, StageGateway, StageKafka, StageLoaderBatch, StageStreamLoad}
	got := Stages()
	if len(got) != 5 {
		t.Fatalf("§8.1 必须是五段，得到 %d", len(got))
	}
	for i, s := range got {
		if s.Stage != want[i] {
			t.Errorf("第 %d 段期望 %s，得到 %s", i, want[i], s.Stage)
		}
		if s.Budget <= 0 {
			t.Errorf("%s 预算必须为正", s.Stage)
		}
	}
}

func TestGatewayEmitsFirstThreeOnly(t *testing.T) {
	gw := StagesFor(ComponentGateway)
	if len(gw) != 3 {
		t.Fatalf("网关只负责前三段，得到 %d 段", len(gw))
	}
	if gw[0].Stage != StageCollectorBatch || gw[1].Stage != StageGateway || gw[2].Stage != StageKafka {
		t.Errorf("网关三段顺序不对: %+v", gw)
	}
	loader := StagesFor(ComponentLoader)
	if len(loader) != 2 {
		t.Fatalf("loader 负责后两段，得到 %d 段", len(loader))
	}
}

func TestBucketsIncludeBudget(t *testing.T) {
	for _, spec := range Stages() {
		budget := spec.Budget.Seconds()
		found := false
		for _, b := range Buckets(spec.Stage) {
			if b == budget {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s 的桶边界必须包含预算值本身 %.4fs，否则超预算占比无法直接相除",
				spec.Stage, budget)
		}
	}
}

func TestUnionBucketsSortedUnique(t *testing.T) {
	u := UnionBuckets()
	if len(u) < 11 {
		t.Fatalf("并集桶过少: %v", u)
	}
	for i := 1; i < len(u); i++ {
		if u[i] <= u[i-1] {
			t.Fatalf("并集桶必须严格递增: %v", u)
		}
	}
}

func TestUnallocatedIsFortyPercent(t *testing.T) {
	if Unallocated != 4*time.Second {
		t.Errorf("未分配余量是架构 owner 持有的 4.0s，不得被模块悄悄改成 %v", Unallocated)
	}
}
