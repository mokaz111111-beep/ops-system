package ingestmsg

import (
	"errors"
	"testing"
	"time"

	"github.com/mokaz/ops-system/pkg/identity"
	"github.com/mokaz/ops-system/pkg/telemetry"
)

func sampleMeta() Meta {
	return Meta{
		TenantID:    "org-1",
		ProjectID:   "prj-prod",
		ClusterID:   "c-sh-01",
		Signal:      telemetry.SignalLogs,
		RecordCount: 7,
		ReceivedAt:  time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC),
		ServiceName: "checkout",
	}
}

func TestHeadersRoundTrip(t *testing.T) {
	m := sampleMeta()
	got, err := ParseMeta(m.Headers())
	if err != nil {
		t.Fatalf("ParseMeta: %v", err)
	}
	if got.TenantID != m.TenantID || got.ProjectID != m.ProjectID || got.ClusterID != m.ClusterID {
		t.Errorf("身份往返丢失: %+v", got)
	}
	if got.Signal != m.Signal || got.RecordCount != m.RecordCount || got.ServiceName != m.ServiceName {
		t.Errorf("元数据往返丢失: %+v", got)
	}
	if !got.ReceivedAt.Equal(m.ReceivedAt) {
		t.Errorf("ReceivedAt 往返: 期望 %v，得到 %v", m.ReceivedAt, got.ReceivedAt)
	}
}

func TestProjectAndServiceOptional(t *testing.T) {
	m := sampleMeta()
	m.ProjectID = ""
	m.ServiceName = ""
	if !m.Valid() {
		t.Fatal("ProjectID 不是必填：DD-006 OQ-4 未裁定前不得因缺 project 拒收")
	}
	got, err := ParseMeta(m.Headers())
	if err != nil {
		t.Fatalf("ParseMeta: %v", err)
	}
	if got.ProjectID != "" || got.ServiceName != "" {
		t.Errorf("空可选头不应被写成零值以外的东西: %+v", got)
	}
}

func TestParseMetaRejectsIncompleteIdentity(t *testing.T) {
	m := sampleMeta()
	hs := m.Headers()
	stripped := hs[:0]
	for _, h := range hs {
		if h.Key != HeaderTenantID {
			stripped = append(stripped, h)
		}
	}
	_, err := ParseMeta(stripped)
	if !errors.Is(err, ErrMissingHeader) {
		t.Fatalf("缺 tenant 必须报 ErrMissingHeader，得到 %v", err)
	}
}

func TestParseMetaRejectsUnknownVersion(t *testing.T) {
	hs := sampleMeta().Headers()
	for i, h := range hs {
		if h.Key == HeaderVersion {
			hs[i].Value = []byte("2")
		}
	}
	_, err := ParseMeta(hs)
	if !errors.Is(err, ErrBadHeader) {
		t.Fatalf("未知版本必须报错，得到 %v", err)
	}
}

func TestLastHeaderWins(t *testing.T) {
	hs := sampleMeta().Headers()
	hs = append(hs, Header{HeaderTenantID, []byte("org-override")})
	got, err := ParseMeta(hs)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "org-override" {
		t.Errorf("同名头应取最后一个，得到 %s", got.TenantID)
	}
}

func TestValidRequiresClock(t *testing.T) {
	m := sampleMeta()
	m.ReceivedAt = time.Time{}
	if m.Valid() {
		t.Error("缺 ReceivedAt 的消息无法作为 §8.1 后三段的时间原点")
	}
	m = sampleMeta()
	m.TenantID = ""
	if m.Valid() {
		t.Error("缺 TenantID 不得 Valid")
	}
	_ = identity.TenantID("org-1")
}
