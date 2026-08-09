package aiservice

import (
	"strings"
	"testing"

	"mychat/internal/model"
)

func TestNormalizeLimit(t *testing.T) {
	if got, err := normalizeLimit(0, 30, 50); err != nil || got != 30 {
		t.Fatalf("default limit = %d, %v", got, err)
	}
	for _, invalid := range []int{-1, 51} {
		if _, err := normalizeLimit(invalid, 30, 50); err != ErrInvalidToolInput {
			t.Fatalf("limit %d error = %v", invalid, err)
		}
	}
}

func TestEscapeLike(t *testing.T) {
	if got, want := escapeLike(`50%_\done`), `50\%\_\\done`; got != want {
		t.Fatalf("escapeLike() = %q, want %q", got, want)
	}
}

func TestReverseMessages(t *testing.T) {
	messages := []model.Message{{UUID: "M3"}, {UUID: "M2"}, {UUID: "M1"}}
	reverseMessages(messages)
	if messages[0].UUID != "M1" || messages[2].UUID != "M3" {
		t.Fatalf("messages = %#v", messages)
	}
}

func TestSendMessageRejectsInvalidInputBeforeDatabaseAccess(t *testing.T) {
	for _, test := range []struct {
		name        string
		userUUID    string
		sessionUUID string
		content     string
	}{
		{name: "missing user", sessionUUID: "S001", content: "hello"},
		{name: "missing session", userUUID: "U001", content: "hello"},
		{name: "blank content", userUUID: "U001", sessionUUID: "S001", content: "  "},
		{name: "content too long", userUUID: "U001", sessionUUID: "S001", content: strings.Repeat("界", MaxAIMessageContentLength+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := SendMessage(test.userUUID, test.sessionUUID, test.content); err != ErrInvalidToolInput {
				t.Fatalf("error = %v, want %v", err, ErrInvalidToolInput)
			}
		})
	}
}
