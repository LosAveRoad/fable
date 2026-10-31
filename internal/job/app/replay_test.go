package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"mychat/internal/dto/im"
	"mychat/internal/job"
)

func replayFixture(t *testing.T) job.DeadLetter {
	t.Helper()
	u1 := "U10000000-0000-4000-8000-000000000001"
	u2 := "U10000000-0000-4000-8000-000000000002"
	m := "M30000000-0000-4000-8000-000000000001"
	s := "S40000000-0000-4000-8000-000000000001"
	task := im.PushTask{SchemaVersion: 1, Kind: "message_push", TaskID: "push:v1:" + m + ":0", Message: im.Message{MessageID: m, ConversationID: "direct:" + s, SessionID: s, SendID: u1, ReceiveID: u2, ReceiveType: im.ReceiveTypeUser, Type: im.MessageTypeText, Content: "secret body", Origin: im.MessageOriginUser, CreatedAt: time.Now().UTC(), Position: "9007199254740993"}, Recipients: []string{u1, u2}, Targets: []im.ConnectionIdentity{}, ChunkCount: 1}
	raw, _ := json.Marshal(task)
	return job.DeadLetter{DLQRecord: im.DLQRecord{SourceTopic: "fable.push.v1.test", SourcePartition: 1, SourceOffset: "9007199254740993", Reason: "unresolved", Task: &task, PendingRecipients: []string{u2}, FirstAttemptAt: time.Now().UTC(), LastAttemptAt: time.Now().UTC()}, OriginalPayloadBase64: base64.StdEncoding.EncodeToString(raw)}
}
func TestReplaySelectionBoundedAndRejectsForeignInvalidRecords(t *testing.T) {
	o := ReplayOptions{RunID: "repair-1", Partition: 0, FromOffset: 5, ToOffset: 7}
	if validateReplay(o) != nil {
		t.Fatal("valid selection rejected")
	}
	for _, f := range []func(*ReplayOptions){func(o *ReplayOptions) { o.RunID = "" }, func(o *ReplayOptions) { o.Partition = -1 }, func(o *ReplayOptions) { o.FromOffset = -1 }, func(o *ReplayOptions) { o.ToOffset = 4 }, func(o *ReplayOptions) { o.ToOffset = 10005 }, func(o *ReplayOptions) { o.RunID = strings.Repeat("x", 129) }} {
		bad := o
		f(&bad)
		if validateReplay(bad) == nil {
			t.Fatal("unsafe range accepted")
		}
	}
	r := replayFixture(t)
	raw, _ := json.Marshal(r)
	foreign := r
	foreign.SourceTopic = "fable.push.v1.foreign"
	fraw, _ := json.Marshal(foreign)
	selected, summary, err := prepareReplay(Config{Topic: "fable.push.v1.test", DLQTopic: "fable.push.dlq.v1.test"}, o, []kafka.Message{{Partition: 0, Offset: 5, Value: raw}, {Partition: 0, Offset: 6, Value: []byte("bad")}, {Partition: 0, Offset: 7, Value: fraw}})
	if err != nil || summary.Scanned != 3 || summary.Replayable != 1 || summary.Rejected != 2 || len(selected) != 1 {
		t.Fatalf("wrong preview %+v %v", summary, err)
	}
	task, err := job.DecodeTask(selected[0].Value)
	if err != nil || task.ReplayRunID != "repair-1" || len(task.PendingRecipients) != 1 || len(task.Recipients) != 2 {
		t.Fatalf("lost scope %+v %v", task, err)
	}
	if len(selected[0].Headers) < 4 {
		t.Fatal("lost replay provenance")
	}
}

func TestPrepareReplayRejectsMissingOrReorderedOffsetsBeforePublishing(t *testing.T) {
	options := ReplayOptions{RunID: "repair-1", Partition: 0, FromOffset: 5, ToOffset: 7}
	for _, offsets := range [][]int64{{6, 7}, {5, 7}, {5, 6}, {8}, {5, 5, 7}, {7, 6, 5}, {}} {
		messages := make([]kafka.Message, 0, len(offsets))
		for _, offset := range offsets {
			messages = append(messages, kafka.Message{Partition: 0, Offset: offset, Value: []byte(`{}`)})
		}
		selected, _, err := prepareReplay(Config{Topic: "fable.push.v1.test"}, options, messages)
		if err == nil || len(selected) != 0 {
			t.Fatalf("incomplete range %v accepted", offsets)
		}
	}
}

func TestReplayRangeCollectorRejectsRetentionClampGapsAndTruncation(t *testing.T) {
	options := ReplayOptions{RunID: "repair-1", Partition: 0, FromOffset: 5, ToOffset: 7}
	for _, offsets := range [][]int64{{6, 7}, {5, 7}, {5, 6, 8}, {5, 6}, {8}, {5, 5, 7}} {
		at := 0
		fetch := func(context.Context) (kafka.Message, error) {
			if at == len(offsets) {
				return kafka.Message{}, io.EOF
			}
			offset := offsets[at]
			at++
			return kafka.Message{Partition: 0, Offset: offset}, nil
		}
		got, err := collectReplayRange(context.Background(), options, fetch)
		if err == nil || len(got) != 0 {
			t.Fatalf("incomplete stream %v accepted", offsets)
		}
	}
	at := int64(5)
	got, err := collectReplayRange(context.Background(), options, func(context.Context) (kafka.Message, error) {
		message := kafka.Message{Partition: 0, Offset: at}
		at++
		return message, nil
	})
	if err != nil || len(got) != 3 || got[0].Offset != 5 || got[2].Offset != 7 {
		t.Fatalf("complete range rejected %v %v", got, err)
	}
}
