package gormservice

import (
	"errors"
	"testing"
)

func TestGroupServiceRejectsInvalidInputsWithoutDatabase(t *testing.T) {
	if _, err := CreateGroup("", "group"); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("CreateGroup error = %v, want %v", err, ErrInvalidGroup)
	}
	if _, err := CreateGroup("U001", ""); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("CreateGroup empty name error = %v, want %v", err, ErrInvalidGroup)
	}
	if err := JoinGroup("", "G001"); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("JoinGroup error = %v, want %v", err, ErrInvalidGroup)
	}
	if _, err := SendGroupMessage("U001", "G001", ""); !errors.Is(err, ErrInvalidMessageContent) {
		t.Fatalf("SendGroupMessage empty content error = %v, want %v", err, ErrInvalidMessageContent)
	}
}
