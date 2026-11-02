package job

import (
	"encoding/base64"
	"encoding/json"
	"mychat/internal/dto/im"
	"reflect"
	"testing"
	"time"
)

func TestReplayPreservesIdentityAndOnlyPendingClearsHints(t *testing.T) {
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{{UserID: user1, InstanceID: "comet-a", ConnectionID: "20000000-0000-4000-8000-000000000001", RouteToken: "MDEyMzQ1Njc4OWFiY2RlZg"}}
	r := DeadLetter{DLQRecord: im.DLQRecord{SourceTopic: "fable.push.v1", SourcePartition: 0, SourceOffset: "9007199254740993", Reason: "unresolved", Task: &task, PendingRecipients: []string{user2}, FirstAttemptAt: time.Now().UTC(), LastAttemptAt: time.Now().UTC()}, OriginalPayloadBase64: base64.StdEncoding.EncodeToString(taskBytes(t, task))}
	got, err := ReplayTask(r, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.TaskID != task.TaskID || !reflect.DeepEqual(got.Message, task.Message) || !reflect.DeepEqual(got.Recipients, task.Recipients) || !reflect.DeepEqual(got.PendingRecipients, []string{user2}) || len(got.Targets) != 0 || got.ReplayRunID != "run-1" {
		t.Fatalf("changed replay scope %+v", got)
	}
	raw, _ := json.Marshal(got)
	if _, err := DecodeTask(raw); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*DeadLetter){
		func(r *DeadLetter) { r.PendingRecipients = []string{user3} },
		func(r *DeadLetter) { r.PendingRecipients = nil },
		func(r *DeadLetter) { r.RawPayloadBase64 = "eA==" },
		func(r *DeadLetter) { r.SourceOffset = "1e3" },
		func(r *DeadLetter) { r.OriginalPayloadBase64 = base64.StdEncoding.EncodeToString([]byte("{}")) },
	} {
		bad := r
		mutate(&bad)
		if _, err := ReplayTask(bad, "run-1"); err == nil {
			t.Fatal("unsafe replay accepted")
		}
	}
	if _, err := ReplayTask(r, ""); err == nil {
		t.Fatal("missing run id accepted")
	}
}
func TestReplayCannotWidenPreviouslyPendingSubset(t *testing.T) {
	task := taskFixture()
	task.PendingRecipients = []string{user2}
	task.ReplayRunID = "old"
	r := DeadLetter{DLQRecord: im.DLQRecord{SourceTopic: "fable.push.v1", SourcePartition: 0, SourceOffset: "0", Reason: "unresolved", Task: &task, PendingRecipients: []string{user1}, FirstAttemptAt: time.Now().UTC(), LastAttemptAt: time.Now().UTC()}, OriginalPayloadBase64: base64.StdEncoding.EncodeToString(taskBytes(t, task))}
	if _, err := ReplayTask(r, "new"); err == nil {
		t.Fatal("replay widened known pending")
	}
}

func TestDeadLetterDecoderRejectsAmbiguousOrInvalidEncoding(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`{"reason":"a","reason":"b"}`),
		[]byte(`{"Reason":"a"}`),
		[]byte(`{"reason":"\ud800"}`),
		[]byte{'{', '"', 'r', 'e', 'a', 's', 'o', 'n', '"', ':', '"', 0xff, '"', '}'},
		[]byte(`{"future":1}`),
	} {
		if _, err := DecodeDeadLetter(raw); err == nil {
			t.Fatalf("unsafe DLQ accepted %q", raw)
		}
	}
}
