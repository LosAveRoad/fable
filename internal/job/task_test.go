package job

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"mychat/internal/dto/im"
)

const user1 = "U10000000-0000-4000-8000-000000000001"
const user2 = "U10000000-0000-4000-8000-000000000002"
const user3 = "U10000000-0000-4000-8000-000000000003"

func taskFixture() im.PushTask {
	return im.PushTask{SchemaVersion: 1, Kind: "message_push", TaskID: "push:v1:M30000000-0000-4000-8000-000000000001:0",
		Message: im.Message{MessageID: "M30000000-0000-4000-8000-000000000001", ConversationID: "direct:S40000000-0000-4000-8000-000000000001",
			SessionID: "S40000000-0000-4000-8000-000000000001", SendID: user1, ReceiveID: user2, ReceiveType: im.ReceiveTypeUser,
			Type: im.MessageTypeText, Content: "hello", Origin: im.MessageOriginUser, CreatedAt: time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC), Position: "9007199254740993"},
		Recipients: []string{user1, user2}, Targets: []im.ConnectionIdentity{}, ChunkCount: 1}
}

func taskBytes(t *testing.T, task im.PushTask) []byte {
	t.Helper()
	b, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecodeTaskPreservesMessageAndReplay(t *testing.T) {
	task := taskFixture()
	task.PendingRecipients = []string{user2}
	task.ReplayRunID = "replay-1"
	got, err := DecodeTask(taskBytes(t, task))
	if err != nil {
		t.Fatal(err)
	}
	if got.Message.Position != "9007199254740993" || len(got.Recipients) != 2 || len(got.PendingRecipients) != 1 || got.PendingRecipients[0] != user2 {
		t.Fatalf("lost task identity or subset: %+v", got)
	}
	group := taskFixture()
	group.Message.SessionID = "G60000000-0000-4000-8000-000000000001"
	group.Message.ConversationID = "group:" + group.Message.SessionID
	group.Message.ReceiveID = group.Message.SessionID
	group.Message.ReceiveType = im.ReceiveTypeGroup
	if _, err := DecodeTask(taskBytes(t, group)); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeTaskRejectsInvalidPayload(t *testing.T) {
	base := string(taskBytes(t, taskFixture()))
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"unknown schema", strings.Replace(base, `"schema_version":1`, `"schema_version":2`, 1)},
		{"missing index", strings.Replace(base, `"chunk_index":0,`, "", 1)},
		{"duplicate key", strings.Replace(base, `"chunk_index":0`, `"chunk_index":0,"chunk_index":1`, 1)},
		{"nested duplicate", strings.Replace(base, `"content":"hello"`, `"content":"hello","content":"other"`, 1)},
		{"unknown field", strings.TrimSuffix(base, "}") + `,"future":1}`},
		{"invalid utf8", strings.Replace(base, "hello", string([]byte{255}), 1)},
		{"extra value", base + ` {}`},
		{"null targets", strings.Replace(base, `"targets":[]`, `"targets":null`, 1)},
		{"null pending", strings.TrimSuffix(base, "}") + `,"pending_recipients":null,"replay_run_id":"r"}`},
		{"empty pending", strings.TrimSuffix(base, "}") + `,"pending_recipients":[],"replay_run_id":"r"}`},
		{"unknown pending", strings.TrimSuffix(base, "}") + `,"pending_recipients":["` + user3 + `"],"replay_run_id":"r"}`},
		{"missing replay id", strings.TrimSuffix(base, "}") + `,"pending_recipients":["` + user1 + `"]}`},
		{"orphan replay id", strings.TrimSuffix(base, "}") + `,"replay_run_id":"r"}`},
		{"duplicate recipient", strings.Replace(base, `"recipients":["`+user1+`","`+user2+`"]`, `"recipients":["`+user1+`","`+user1+`"]`, 1)},
		{"unsorted recipient", strings.Replace(base, `"recipients":["`+user1+`","`+user2+`"]`, `"recipients":["`+user2+`","`+user1+`"]`, 1)},
		{"position overflow", strings.Replace(base, "9007199254740993", "9223372036854775808", 1)},
		{"position leading zero", strings.Replace(base, "9007199254740993", "01", 1)},
		{"task mismatch", strings.Replace(base, `:0"`, `:1"`, 1)},
		{"invalid enum", strings.Replace(base, `"origin":"user"`, `"origin":"system"`, 1)},
		{"wrong conversation", strings.Replace(base, "direct:S", "group:S", 1)},
		{"zero time", strings.Replace(base, "2026-10-28T00:00:00Z", "0001-01-01T00:00:00Z", 1)},
		{"non UTC", strings.Replace(base, "2026-10-28T00:00:00Z", "2026-10-28T01:00:00+01:00", 1)},
		{"too large", strings.Repeat(" ", 256*1024) + base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeTask([]byte(tc.raw))
			if !errors.Is(err, ErrInvalidTask) {
				t.Fatalf("accepted invalid payload: %v", err)
			}
			if strings.Contains(err.Error(), user1) || strings.Contains(err.Error(), "hello") {
				t.Fatal("sensitive error", err)
			}
		})
	}
}

func TestDecodeTaskRejectsInvalidRouteHints(t *testing.T) {
	for _, mutate := range []func(*im.ConnectionIdentity){
		func(h *im.ConnectionIdentity) { h.UserID = user3 },
		func(h *im.ConnectionIdentity) { h.RouteToken = "MDEyMzQ1Njc4OWFiY2RlZh" },
		func(h *im.ConnectionIdentity) { h.InstanceID = "http://evil" },
		func(h *im.ConnectionIdentity) { h.ConnectionID = "bad" },
	} {
		task := taskFixture()
		h := im.ConnectionIdentity{UserID: user1, InstanceID: "comet-a", ConnectionID: "20000000-0000-4000-8000-000000000001", RouteToken: "MDEyMzQ1Njc4OWFiY2RlZg"}
		mutate(&h)
		task.Targets = []im.ConnectionIdentity{h}
		if _, err := DecodeTask(taskBytes(t, task)); !errors.Is(err, ErrInvalidTask) {
			t.Fatalf("hint accepted: %v", err)
		}
	}
}
