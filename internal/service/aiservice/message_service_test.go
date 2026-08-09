package aiservice

import (
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
