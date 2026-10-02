package authn

import (
	"errors"
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/ingesterr"
)

const (
	goodToken    = "tok-live-abcdefghijklmnop"
	revokedToken = "tok-revoked-123456789012"
	suspToken    = "tok-suspended-1234567890"
)

func mustHash(t *testing.T, tok string) identity.TokenHash {
	t.Helper()
	h, err := identity.HashToken(tok)
	if err != nil {
		t.Fatalf("HashToken(%q): %v", tok, err)
	}
	return h
}

func testSnapshot(t *testing.T, fetchedAt time.Time) *Snapshot {
	t.Helper()
	good := mustHash(t, goodToken)
	revoked := mustHash(t, revokedToken)
	susp := mustHash(t, suspToken)

	return &Snapshot{
		Version:   7,
		FetchedAt: fetchedAt,
		Bindings: map[identity.TokenHash]identity.Binding{
			good: {
				TenantID: "org-1", ProjectID: "prj-prod", ClusterID: "c-sh-01",
				TokenHash:    good,
				TenantStatus: identity.StatusActive, ClusterStatus: identity.StatusActive,
			},
			revoked: {
				TenantID: "org-1", ClusterID: "c-sh-02", TokenHash: revoked,
				Revoked:      true,
				TenantStatus: identity.StatusActive, ClusterStatus: identity.StatusActive,
			},
			susp: {
				TenantID: "org-2", ClusterID: "c-bj-01", TokenHash: susp,
				TenantStatus: identity.StatusSuspended, ClusterStatus: identity.StatusActive,
			},
		},
	}
}

func fixedClock(at time.Time) Option {
	return WithClock(func() time.Time { return at })
}

func TestResolveHappyPath(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	r := NewResolver(testSnapshot(t, now.Add(-time.Minute)), fixedClock(now))

	res, err := r.Resolve(goodToken)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if res.Binding.TenantID != "org-1" || res.Binding.ClusterID != "c-sh-01" {
		t.Errorf("身份解析错误: %+v", res.Binding)
	}
	if res.Degraded {
		t.Error("新鲜快照不应标记为 degraded")
	}
	if res.SnapshotAge != time.Minute {
		t.Errorf("快照年龄应为 1m，得到 %v", res.SnapshotAge)
	}
}

func TestResolveErrorCodes(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	r := NewResolver(testSnapshot(t, now), fixedClock(now))

	cases := []struct {
		name  string
		token string
		want  ingesterr.Code
	}{
		{"空 Token", "", ingesterr.CodeTokenInvalid},
		{"未知 Token", "tok-nonexistent-00000000", ingesterr.CodeTokenInvalid},
		{"已吊销", revokedToken, ingesterr.CodeTokenInvalid},
		{"租户停用", suspToken, ingesterr.CodeTenantDisabled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := r.Resolve(c.token)
			if err == nil {
				t.Fatal("期望错误，得到 nil")
			}
			var ie *ingesterr.Error
			if !errors.As(err, &ie) {
				t.Fatalf("错误类型应为 *ingesterr.Error，得到 %T", err)
			}
			if ie.Code() != c.want {
				t.Errorf("期望码 %s，得到 %s", c.want, ie.Code())
			}
		})
	}
}

// TestNoSnapshotIsRetryable 固定一个容易做错的判定：首次同步未完成时必须返回可重试
// 错误。报成 401 会让客户端判定为接入配置错误并丢弃数据，而实际上重试就能成功。
func TestNoSnapshotIsRetryable(t *testing.T) {
	r := NewResolver(nil)

	_, err := r.Resolve(goodToken)
	var ie *ingesterr.Error
	if !errors.As(err, &ie) {
		t.Fatalf("错误类型应为 *ingesterr.Error，得到 %T", err)
	}
	if !ie.Retryable() {
		t.Errorf("%s 应可重试——否则客户端会丢弃本可成功的数据", ie.Code())
	}
	if ie.Code() == ingesterr.CodeTokenInvalid {
		t.Error("首次同步未完成不应报成 Token 无效")
	}
}

// TestStaleSnapshotStillServes 是 fail-static 的核心断言：
// 快照陈旧到任何程度都不得拒绝已知且有效的 Token。
//
// 如果这个测试变红，说明有人给鉴权加了"快照过期即失效"的逻辑，
// 而那等于把 fail-static 悄悄变成 fail-closed——控制面宕机会直接放大为全站拒收。
func TestStaleSnapshotStillServes(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	for _, age := range []time.Duration{
		time.Hour, 5 * time.Hour, 48 * time.Hour, 30 * 24 * time.Hour,
	} {
		r := NewResolver(testSnapshot(t, now.Add(-age)), fixedClock(now),
			WithClassSTTL(4*time.Hour))

		res, err := r.Resolve(goodToken)
		if err != nil {
			t.Fatalf("快照陈旧 %v 时仍必须放行有效 Token，却得到错误: %v", age, err)
		}
		if want := age > 4*time.Hour; res.Degraded != want {
			t.Errorf("快照陈旧 %v：Degraded 期望 %v，得到 %v", age, want, res.Degraded)
		}
	}
}

// TestDegradedIsObservable 确认降级判定会被计数——这是本实现对 DD-006 TTL 条款的
// 修正所依赖的机制：既然无法避免陈旧期间吊销失效，至少要让它可见。
func TestDegradedIsObservable(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	r := NewResolver(testSnapshot(t, now.Add(-10*time.Hour)), fixedClock(now),
		WithClassSTTL(4*time.Hour))

	for i := 0; i < 3; i++ {
		if _, err := r.Resolve(goodToken); err != nil {
			t.Fatalf("意外错误: %v", err)
		}
	}

	s := r.Stats()
	if s.DegradedDecisions != 3 {
		t.Errorf("降级判定计数期望 3，得到 %d", s.DegradedDecisions)
	}
	if !s.Stale {
		t.Error("Stats.Stale 应为 true")
	}
	if s.SnapshotAgeSeconds != int64((10 * time.Hour).Seconds()) {
		t.Errorf("快照年龄秒数不符: %d", s.SnapshotAgeSeconds)
	}
	if s.SnapshotVersion != 7 {
		t.Errorf("版本号期望 7，得到 %d", s.SnapshotVersion)
	}
}

func TestStatsCountRejections(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	r := NewResolver(testSnapshot(t, now), fixedClock(now))

	_, _ = r.Resolve("tok-unknown-000000000000")
	_, _ = r.Resolve(revokedToken)
	_, _ = r.Resolve(suspToken)
	_, _ = r.Resolve(suspToken)

	s := r.Stats()
	if s.RejectedUnknown != 1 {
		t.Errorf("未知 Token 计数期望 1，得到 %d", s.RejectedUnknown)
	}
	if s.RejectedRevoked != 1 {
		t.Errorf("吊销计数期望 1，得到 %d", s.RejectedRevoked)
	}
	if s.RejectedDisabled != 2 {
		t.Errorf("停用计数期望 2，得到 %d", s.RejectedDisabled)
	}
}

// TestReplaceDoesNotRejectOlderVersion 确认快照替换不做版本递增校验：
// 控制面回滚配置时，数据面必须接受回滚后的版本，否则一次回滚会变成数据面拒绝配置。
func TestReplaceDoesNotRejectOlderVersion(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	newer := testSnapshot(t, now)
	newer.Version = 100
	r := NewResolver(newer, fixedClock(now))

	rolledBack := testSnapshot(t, now)
	rolledBack.Version = 42
	r.Replace(rolledBack)

	if got := r.Stats().SnapshotVersion; got != 42 {
		t.Errorf("回滚后版本应为 42，得到 %d", got)
	}
}

func TestReplaceNilIsIgnored(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	r := NewResolver(testSnapshot(t, now), fixedClock(now))
	r.Replace(nil)

	if _, err := r.Resolve(goodToken); err != nil {
		t.Fatalf("nil 替换应被忽略，原快照继续生效，却得到: %v", err)
	}
}

func TestFutureSnapshotAgeClampedToZero(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	// 时钟漂移导致快照时间戳在未来。年龄应被压到 0 而不是变成负数。
	r := NewResolver(testSnapshot(t, now.Add(time.Hour)), fixedClock(now))

	res, err := r.Resolve(goodToken)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if res.SnapshotAge != 0 {
		t.Errorf("未来时间戳的快照年龄应为 0，得到 %v", res.SnapshotAge)
	}
	if res.Degraded {
		t.Error("未来时间戳不应被判为陈旧")
	}
}

func TestResolveIsConcurrencySafe(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	r := NewResolver(testSnapshot(t, now), fixedClock(now))

	done := make(chan struct{})
	// 一边高频鉴权，一边替换快照：-race 下应无数据竞争。
	go func() {
		for i := 0; i < 2000; i++ {
			s := testSnapshot(t, now)
			s.Version = uint64(i)
			r.Replace(s)
		}
		close(done)
	}()
	for i := 0; i < 2000; i++ {
		if _, err := r.Resolve(goodToken); err != nil {
			t.Fatalf("意外错误: %v", err)
		}
	}
	<-done
}
