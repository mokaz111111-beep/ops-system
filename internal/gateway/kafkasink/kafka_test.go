package kafkasink

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mokaz/ops-system/pkg/ingesterr"
)

func TestClassifyBackpressureAndTooLarge(t *testing.T) {
	var ie *ingesterr.Error

	err := Classify(kgo.ErrMaxBuffered)
	if !errors.As(err, &ie) || ie.Code() != ingesterr.CodeBackpressure {
		t.Fatalf("缓冲区满必须是 E7，得到 %v", err)
	}
	if !ie.Retryable() {
		t.Fatal("E7 必须可重试")
	}

	err = Classify(kerr.MessageTooLarge)
	if !errors.As(err, &ie) || ie.Code() != ingesterr.CodeBodyTooLarge {
		t.Fatalf("消息过大必须是 E4，得到 %v", err)
	}
	if ie.Retryable() {
		t.Fatal("E4 不可重试，否则毒消息风暴")
	}

	err = Classify(kerr.RecordListTooLarge)
	if !errors.As(err, &ie) || ie.Code() != ingesterr.CodeBodyTooLarge {
		t.Fatalf("RecordListTooLarge 必须是 E4，得到 %v", err)
	}
}

func TestNewKafkaRequiresBroker(t *testing.T) {
	if _, err := NewKafka(KafkaConfig{}); err == nil {
		t.Fatal("未配置 broker 必须失败")
	}
}
