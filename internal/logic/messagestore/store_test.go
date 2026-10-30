package messagestore

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	pb "mychat/api/gen/fable/v1"
)

const (
	userA     = "U10000000-0000-4000-8000-000000000001"
	userB     = "U10000000-0000-4000-8000-000000000002"
	userC     = "U10000000-0000-4000-8000-000000000003"
	sessionAB = "S20000000-0000-4000-8000-000000000001"
	sessionAC = "S20000000-0000-4000-8000-000000000002"
	groupID   = "G30000000-0000-4000-8000-000000000001"
	clientID  = "40000000-0000-4000-8000-000000000001"
)

func validInput() Input {
	return Input{SenderID: userA, ClientMessageID: clientID, Destination: &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_USER, ReceiveId: userB}, Content: "hello"}
}

// Removing input checks must not let malformed requests reach the database.
func TestSaveValidatesBeforeDatabase(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Input)
	}{
		{"sender prefix", func(in *Input) { in.SenderID = sessionAB }},
		{"sender malformed", func(in *Input) { in.SenderID = "Ubad" }},
		{"client malformed", func(in *Input) { in.ClientMessageID = "not-uuid" }},
		{"client noncanonical", func(in *Input) { in.ClientMessageID = "{40000000-0000-4000-8000-000000000001}" }},
		{"missing destination", func(in *Input) { in.Destination = nil }},
		{"unspecified type", func(in *Input) { in.Destination.ReceiveType = 0 }},
		{"unknown type", func(in *Input) { in.Destination.ReceiveType = 42 }},
		{"wrong target", func(in *Input) { in.Destination.ReceiveId = groupID }},
		{"self", func(in *Input) { in.Destination.ReceiveId = userA }},
		{"empty", func(in *Input) { in.Content = "" }},
		{"invalid utf8", func(in *Input) { in.Content = string([]byte{0xff}) }},
		{"too long", func(in *Input) { in.Content = strings.Repeat("a", 16385) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := validInput()
			tt.change(&in)
			_, err := New(nil).Save(context.Background(), in)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error=%v, want ErrInvalid", err)
			}
		})
	}
	for _, content := range []string{" ", strings.Repeat("a", 16384)} {
		in := validInput()
		in.Content = content
		_, err := New(nil).Save(context.Background(), in)
		if !errors.Is(err, ErrDatabase) {
			t.Fatalf("valid content error=%v, want database", err)
		}
	}
}

func TestSaveAIValidatesNormalizedContent(t *testing.T) {
	for _, content := range []string{" \n\t", strings.Repeat("界", 4001), string([]byte{0xff})} {
		_, err := New(nil).SaveAI(context.Background(), userA, sessionAB, clientID, content)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("error=%v", err)
		}
	}
	_, err := New(nil).SaveAI(context.Background(), userA, sessionAB, clientID, " \n"+strings.Repeat("界", 4000)+" \n")
	if !errors.Is(err, ErrDatabase) {
		t.Fatalf("valid normalized content error=%v", err)
	}
}

func cursorText(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

// Accepting alternate encodings, unbound cursors or overflow could skip messages.
func TestSyncRejectsInvalidCursorAndLimitBeforeDatabase(t *testing.T) {
	conversation := "direct:" + sessionAB
	cursors := []string{
		"invalid!", cursorText(`{"v":2,"conversation_id":"` + conversation + `","after_position":"1"}`),
		cursorText(`{"v":1,"conversation_id":"group:` + groupID + `","after_position":"1"}`),
	}
	for _, position := range []string{"0", "-1", "01", "+1", "9223372036854775808", "1.0", ""} {
		cursors = append(cursors, cursorText(`{"v":1,"conversation_id":"`+conversation+`","after_position":"`+position+`"}`))
	}
	cursors = append(cursors, cursorText(`{"v":1,"v":1,"conversation_id":"`+conversation+`","after_position":"1"}`))
	for _, cursor := range cursors {
		_, err := New(nil).Sync(context.Background(), userA, conversation, cursor, 1)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("cursor %q error=%v", cursor, err)
		}
	}
	for _, limit := range []int{-1, 501} {
		_, err := New(nil).Sync(context.Background(), userA, conversation, "", limit)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("limit %d error=%v", limit, err)
		}
	}
	for _, conversation := range []string{"direct:" + groupID, "group:" + sessionAB, "garbage", "direct:Sbad"} {
		_, err := New(nil).Sync(context.Background(), userA, conversation, "", 1)
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("conversation %s error=%v", conversation, err)
		}
	}
	for _, limit := range []int{0, 1, 500} {
		_, err := New(nil).Sync(context.Background(), userA, conversation, "", limit)
		if !errors.Is(err, ErrDatabase) {
			t.Fatalf("valid query error=%v", err)
		}
	}
}
