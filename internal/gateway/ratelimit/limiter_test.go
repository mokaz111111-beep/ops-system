package ratelimit

import (
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
)

func key(tenant, cluster string) identity.QuotaKey {
	return identity.QuotaKey{TenantID: identity.TenantID(tenant), ClusterID: identity.ClusterID(cluster)}
}

func limits(bps, eps float64) Limits {
	return Limits{BytesPerSec: bps, BytesBurst: bps, EventsPerSec: eps, EventsBurst: eps}.Normalize()
}

func TestUnlimitedWhenNoSnapshot(t *testing.T) {
	l := New(nil)
	d := l.Allow(key("t", "c"), 1<<20, 1000)
	if !d.OK {
		t.Fatal("无快照必须放行：控制面尚未下发不等于配额为零")
	}
}

func TestUnlimitedWhenRateZero(t *testing.T) {
	l := New(&Snapshot{Default: Limits{}})
	if !l.Allow(key("t", "c"), 99, 99).OK {
		t.Fatal("速率为 0 视为不限流")
	}
}

func TestNoPartialDeduction(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	l := New(&Snapshot{Default: limits(100, 10)}, WithClock(func() time.Time { return now }))

	if d := l.Allow(key("t", "c"), 40, 4); !d.OK {
		t.Fatalf("第一批应通过: %+v", d)
	}
	// 字节还够（60），事件不够（6 < 8）。若实现先扣字节再判事件，下一次 50 字节也会被拒。
	if d := l.Allow(key("t", "c"), 50, 8); d.OK || d.Dimension != DimEvents {
		t.Fatalf("应因 events 被拒且不扣字节: %+v", d)
	}
	if d := l.Allow(key("t", "c"), 50, 4); !d.OK {
		t.Fatalf("被拒请求不得扣掉另一维令牌，50 字节 4 事件仍应通过: %+v", d)
	}
}

func TestNeverFitsIsNotQuotaExceeded(t *testing.T) {
	l := New(&Snapshot{Default: limits(100, 10)})
	d := l.Allow(key("t", "c"), 200, 1)
	if d.OK || !d.NeverFits || d.Dimension != DimBytes {
		t.Fatalf("单批大于突发应 NeverFits: %+v", d)
	}
	d = l.Allow(key("t", "c"), 1, 20)
	if d.OK || !d.NeverFits || d.Dimension != DimEvents {
		t.Fatalf("事件数大于突发应 NeverFits: %+v", d)
	}
}

func TestRetryAfterRoundedUp(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	l := New(&Snapshot{Default: limits(100, 100)}, WithClock(func() time.Time { return now }))
	if !l.Allow(key("t", "c"), 100, 1).OK {
		t.Fatal("灌满字节桶")
	}
	d := l.Allow(key("t", "c"), 10, 1)
	if d.OK {
		t.Fatal("应被拒")
	}
	if d.RetryAfter < time.Second {
		t.Errorf("Retry-After 不得小于 1s，得到 %v", d.RetryAfter)
	}
}

func TestPerKeyOverridesDefault(t *testing.T) {
	k := key("org", "c1")
	l := New(&Snapshot{
		Default: limits(10, 10),
		PerKey:  map[identity.QuotaKey]Limits{k: limits(1000, 1000)},
	})
	if d := l.Allow(k, 500, 50); !d.OK {
		t.Fatalf("该 key 的专属配额应放行: %+v", d)
	}
	if d := l.Allow(key("org", "c2"), 500, 50); d.OK {
		t.Fatal("另一集群应走默认配额被拒")
	}
}

func TestSweepDoesNotGrantFreeBurst(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	clock := now
	// 突发远大于速率：空闲一个 TTL 后令牌仍未补满，此时回收等于发免费突发。
	slow := Limits{BytesPerSec: 1, BytesBurst: 10000, EventsPerSec: 1, EventsBurst: 10000}
	l := New(&Snapshot{Default: slow},
		WithClock(func() time.Time { return clock }),
		WithIdleTTL(time.Minute),
	)
	if !l.Allow(key("t", "c"), 10000, 10000).OK {
		t.Fatal("灌满")
	}
	clock = now.Add(2 * time.Minute) // 只补了 120，远未满
	if n := l.Sweep(); n != 0 {
		t.Fatalf("未满的桶不得回收（否则等于发一次免费突发），回收了 %d", n)
	}
	if d := l.Allow(key("t", "c"), 10000, 10000); d.OK {
		t.Fatal("回收被拦住后，灌满的桶不得被当成新桶")
	}
}

func TestSweepEvictsFullIdleBucket(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	clock := now
	l := New(&Snapshot{Default: limits(100, 10)},
		WithClock(func() time.Time { return clock }),
		WithIdleTTL(time.Minute),
	)
	if !l.Allow(key("t", "c"), 10, 1).OK {
		t.Fatal("扣一点")
	}
	clock = now.Add(2 * time.Minute) // 已补满且空闲超过 TTL
	if n := l.Sweep(); n != 1 {
		t.Fatalf("满且空闲的桶应回收，得到 %d", n)
	}
	if l.Stats().Buckets != 0 {
		t.Errorf("回收后桶数应为 0，得到 %d", l.Stats().Buckets)
	}
}

func TestClusterIsolation(t *testing.T) {
	l := New(&Snapshot{Default: limits(100, 10)})
	if !l.Allow(key("org", "c1"), 100, 10).OK {
		t.Fatal("c1 灌满")
	}
	if d := l.Allow(key("org", "c2"), 100, 10); !d.OK {
		t.Fatal("同租户另一集群不得被牵连")
	}
}
