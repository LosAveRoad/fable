package kafkapush

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const directKey = "direct:S11111111-1111-4111-8111-111111111111"
const groupKey = "group:G22222222-2222-4222-8222-222222222222"

var payload = []byte(`{"schema_version":1,"message":{"position":"9007199254740993"}}`)

func publisher(t *testing.T) *Publisher {
	t.Helper()
	p, err := New([]string{"127.0.0.1:1"}, "fable.push.v1.test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}
func TestNewRejectsInvalidBrokerAndLegacyTopic(t *testing.T) {
	for _, brokers := range [][]string{nil, {}, {""}, {"localhost"}, {"http://localhost:9092"}, {"localhost:0"}, {"localhost:65536"}, {" localhost:9092"}} {
		if p, err := New(brokers, "fable.push.v1"); err == nil {
			p.Close()
			t.Fatalf("accepted brokers %v", brokers)
		}
	}
	for _, topic := range []string{"", "fable.chat.message", "other", "fable.push.v10", "fable.push.v1.", "fable.push.v1.bad/topic", strings.Repeat("x", 250)} {
		if p, err := New([]string{"localhost:9092"}, topic); err == nil {
			p.Close()
			t.Fatalf("accepted topic %q", topic)
		}
	}
	for _, topic := range []string{"fable.push.v1", "fable.push.v1.test-123"} {
		p, err := New([]string{"127.0.0.1:9092", "[::1]:9092"}, topic)
		if err != nil {
			t.Fatalf("valid config: %v", err)
		}
		p.Close()
	}
}
func TestInvalidKeyOrPayloadIsDefinitelyNotPublished(t *testing.T) {
	p := publisher(t)
	for _, key := range []string{"", "direct:11111111-1111-4111-8111-111111111111", "direct:G11111111-1111-4111-8111-111111111111", "group:S11111111-1111-4111-8111-111111111111", "direct:S00000000-0000-0000-0000-000000000000", "direct:SAAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
		if err := p.Publish(context.Background(), key, payload); !errors.Is(err, ErrNotPublished) {
			t.Fatalf("bad key %q: %v", key, err)
		}
	}
	for _, raw := range [][]byte{nil, {}, []byte(`{"bad":`), []byte(`[]`), []byte(`null`), []byte(`"text"`), []byte(`{"body":"` + strings.Repeat("x", 256*1024) + `"}`)} {
		if err := p.Publish(context.Background(), directKey, raw); !errors.Is(err, ErrNotPublished) {
			t.Fatalf("invalid payload: %v", err)
		}
	}
}
func TestCanceledBeforeWriteIsDefiniteAndPreservesContext(t *testing.T) {
	p := publisher(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.Publish(ctx, directKey, payload)
	if !errors.Is(err, ErrNotPublished) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled outcome: %v", err)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err = p.Publish(ctx, groupKey, payload)
	if !errors.Is(err, ErrNotPublished) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired outcome: %v", err)
	}
}
func TestClosedPublisherRejectsWithoutUncertainWrite(t *testing.T) {
	p := publisher(t)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), directKey, payload); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("closed publish: %v", err)
	}
	if err := p.Ping(context.Background()); err == nil {
		t.Fatal("closed publisher reported ready")
	}
}
func TestWriteFailureRemainsUncertainAndBounded(t *testing.T) {
	p := publisher(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Publish(ctx, directKey, payload)
	if err == nil || errors.Is(err, ErrNotPublished) {
		t.Fatalf("write failure incorrectly classified: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("publish ignored context budget")
	}
	if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "9007199254740993") {
		t.Fatalf("sensitive diagnostics: %v", err)
	}
}
