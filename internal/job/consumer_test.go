package job

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type sourceStub struct {
	messages   []kafka.Message
	events     *[]string
	commits    int
	commitFail bool
}

func (s *sourceStub) Fetch(ctx context.Context) (kafka.Message, error) {
	*s.events = append(*s.events, "fetch")
	if len(s.messages) == 0 {
		return kafka.Message{}, io.EOF
	}
	m := s.messages[0]
	s.messages = s.messages[1:]
	return m, nil
}
func (s *sourceStub) Commit(ctx context.Context, m kafka.Message) error {
	*s.events = append(*s.events, "commit")
	s.commits++
	if s.commitFail {
		return errors.New("secret error")
	}
	return nil
}

type processFunc func(context.Context, []byte) (Result, error)

func (f processFunc) Process(ctx context.Context, b []byte) (Result, error) { return f(ctx, b) }

type sinkStub struct {
	records []DeadLetter
	events  *[]string
	fail    bool
}

func (s *sinkStub) Write(ctx context.Context, r DeadLetter) error {
	*s.events = append(*s.events, "dlq")
	s.records = append(s.records, r)
	if s.fail {
		return errors.New("secret body")
	}
	return nil
}
func TestPartitionBoundaryDLQAndCommitFailuresNeverFetchLater(t *testing.T) {
	for _, stage := range []string{"dlq", "commit", "success"} {
		t.Run(stage, func(t *testing.T) {
			var events []string
			task := taskFixture()
			raw := taskBytes(t, task)
			src := &sourceStub{events: &events, messages: []kafka.Message{{Topic: "fable.push.v1", Partition: 0, Offset: 10, Value: raw}, {Offset: 11, Value: raw}}, commitFail: stage == "commit"}
			sink := &sinkStub{events: &events, fail: stage == "dlq"}
			p := processFunc(func(context.Context, []byte) (Result, error) {
				events = append(events, "process")
				return Result{Task: task, Recipients: []RecipientResult{{UserID: user1, Outcome: Enqueued}, {UserID: user2, Outcome: Unresolved}}}, ErrUnresolved
			})
			c := NewConsumer(p, sink, ConsumerOptions{RetryDelay: time.Nanosecond, Attempts: 2})
			err := c.RunPartition(context.Background(), src)
			if stage != "success" {
				if err == nil {
					t.Fatal("missing blocked error")
				}
				for _, e := range events[1:] {
					if e == "fetch" {
						t.Fatalf("advanced failed partition: %v", events)
					}
				}
			} else if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if len(sink.records) == 0 {
				t.Fatal("lost DLQ")
			}
			r := sink.records[0]
			if r.SourceOffset != "10" || r.Task == nil || len(r.Task.Recipients) != 2 || !reflect.DeepEqual(r.PendingRecipients, []string{user2}) {
				t.Fatalf("lost immutable task/pending: %+v", r)
			}
			if stage == "dlq" && src.commits != 0 {
				t.Fatal("committed before DLQ ack")
			}
			n := 0
			for _, e := range events {
				if e == "process" {
					n++
				}
			}
			if stage != "success" && n != 1 {
				t.Fatalf("redelivered known progress %v", events)
			}
		})
	}
}
func TestInvalidTaskPreservesExactRawAndCannotReplay(t *testing.T) {
	var events []string
	raw := []byte{0xff, '{', 'x', '}'}
	src := &sourceStub{events: &events, messages: []kafka.Message{{Topic: "fable.push.v1", Partition: 2, Offset: 9007199254740993, Value: raw}}}
	sink := &sinkStub{events: &events}
	p := processFunc(func(context.Context, []byte) (Result, error) { t.Fatal("invalid reached RPC"); return Result{}, nil })
	err := NewConsumer(p, sink, ConsumerOptions{}).RunPartition(context.Background(), src)
	if !errors.Is(err, io.EOF) || len(sink.records) != 1 {
		t.Fatal(err)
	}
	r := sink.records[0]
	b, e := base64.StdEncoding.DecodeString(r.RawPayloadBase64)
	if e != nil || !reflect.DeepEqual(b, raw) || r.Task != nil || r.SourceOffset != "9007199254740993" {
		t.Fatalf("raw changed %+v", r)
	}
	if _, err := ReplayTask(r, "repair-1"); err == nil {
		t.Fatal("invalid auto replayable")
	}
	if !reflect.DeepEqual(events, []string{"fetch", "dlq", "commit", "fetch"}) {
		t.Fatal(events)
	}
}
func TestCancellationNeverCommitsPartialTask(t *testing.T) {
	var events []string
	ctx, cancel := context.WithCancel(context.Background())
	task := taskFixture()
	src := &sourceStub{events: &events, messages: []kafka.Message{{Value: taskBytes(t, task)}}}
	sink := &sinkStub{events: &events}
	p := processFunc(func(context.Context, []byte) (Result, error) { cancel(); return Result{Task: task}, context.Canceled })
	err := NewConsumer(p, sink, ConsumerOptions{}).RunPartition(ctx, src)
	if !errors.Is(err, context.Canceled) || src.commits != 0 || len(sink.records) != 0 {
		t.Fatalf("cancel committed %v %v", err, events)
	}
}
func TestImmutableIdentityConflictAndBoundedMemory(t *testing.T) {
	var events []string
	task := taskFixture()
	second := task
	second.Message.Content = "changed"
	src := &sourceStub{events: &events, messages: []kafka.Message{{Value: taskBytes(t, task)}, {Value: taskBytes(t, second)}}}
	sink := &sinkStub{events: &events}
	calls := 0
	p := processFunc(func(context.Context, []byte) (Result, error) { calls++; return Result{Task: task}, nil })
	c := NewConsumer(p, sink, ConsumerOptions{IdentityCapacity: 1})
	_ = c.RunPartition(context.Background(), src)
	if calls != 1 || len(sink.records) != 1 || sink.records[0].Reason != "immutable_task_conflict" || sink.records[0].Task != nil || c.Stats().Conflicts != 1 {
		t.Fatalf("conflict lost: %d %+v", calls, sink.records)
	}
	if c.Stats().RecentIdentities != 1 {
		t.Fatal("identity memory unbounded")
	}
}

func TestIdentityWindowEvictsWithoutGrowingAndIgnoresHints(t *testing.T) {
	c := NewConsumer(nil, nil, ConsumerOptions{IdentityCapacity: 2})
	a := taskFixture()
	if c.conflict(a) {
		t.Fatal("new identity conflicts")
	}
	same := a
	same.RequestID = "another-trace"
	same.PendingRecipients = []string{user2}
	same.ReplayRunID = "repair"
	same.Targets = nil
	if c.conflict(same) {
		t.Fatal("mutable routing/replay context conflicts")
	}
	for i := 2; i <= 20; i++ {
		task := taskFixture()
		task.TaskID = fmt.Sprintf("task-%d", i)
		if c.conflict(task) {
			t.Fatal("new task conflicts")
		}
	}
	if c.Stats().RecentIdentities != 2 || len(c.order) != 2 {
		t.Fatal("unbounded identity window")
	}
	a.Message.Content = "changed after eviction"
	if c.conflict(a) {
		t.Fatal("expected explicitly bounded identity window")
	}
}

func TestConsumerCountsDeliveryRetriesAndUnresolvedResults(t *testing.T) {
	var events []string
	task := taskFixture()
	source := &sourceStub{events: &events, messages: []kafka.Message{{Value: taskBytes(t, task)}}}
	sink := &sinkStub{events: &events}
	p := processFunc(func(context.Context, []byte) (Result, error) {
		return Result{Task: task, Attempts: 3, Recipients: []RecipientResult{{UserID: user1, Outcome: Offline}, {UserID: user2, Outcome: Unresolved}}}, ErrUnresolved
	})
	consumer := NewConsumer(p, sink, ConsumerOptions{})
	_ = consumer.RunPartition(context.Background(), source)
	stats := consumer.Stats()
	if stats.Retries != 2 || stats.Unresolved != 1 || stats.Offline != 1 {
		t.Fatalf("missing retry/outcome metrics %+v", stats)
	}
}

func TestBoundaryCorrelationEventsContainOnlySafeIdentityAndCounts(t *testing.T) {
	task := taskFixture()
	task.RequestID = "request-1"
	var calls []ConsumerEvent
	var events []string
	source := &sourceStub{events: &events, messages: []kafka.Message{{Topic: "fable.push.v1", Partition: 2, Offset: 9007199254740993, Value: taskBytes(t, task)}}}
	sink := &sinkStub{events: &events}
	p := processFunc(func(context.Context, []byte) (Result, error) {
		return Result{Task: task, Attempts: 2, Recipients: []RecipientResult{{UserID: user1, Outcome: Enqueued}, {UserID: user2, Outcome: Unresolved}}}, ErrUnresolved
	})
	consumer := NewConsumer(p, sink, ConsumerOptions{Observe: func(e ConsumerEvent) { calls = append(calls, e) }})
	_ = consumer.RunPartition(context.Background(), source)
	if len(calls) != 3 || calls[0].Stage != "process" || calls[1].Stage != "dlq" || calls[2].Stage != "commit" {
		t.Fatalf("missing correlation events %+v", calls)
	}
	for _, e := range calls {
		if e.TaskID != task.TaskID || e.MessageID != task.Message.MessageID || e.RequestID != "request-1" || e.SourceOffset != "9007199254740993" || e.SourcePartition != 2 {
			t.Fatalf("lost correlation %+v", e)
		}
	}
	raw, _ := json.Marshal(calls)
	if strings.Contains(string(raw), "hello") || strings.Contains(string(raw), user1) || strings.Contains(string(raw), user2) {
		t.Fatal("payload or recipient in boundary event")
	}
}
