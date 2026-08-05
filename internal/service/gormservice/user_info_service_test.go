package gormservice

import (
	"errors"
	"testing"

	"mychat/internal/dto/request"
)

func TestIsPhone(t *testing.T) {
	tests := []struct {
		name  string
		phone string
		want  bool
	}{
		{name: "valid mobile number", phone: "13800138000", want: true},
		{name: "too short", phone: "1380013800", want: false},
		{name: "wrong prefix", phone: "12800138000", want: false},
		{name: "contains letters", phone: "13800138abc", want: false},
		{name: "empty", phone: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPhone(tt.phone); got != tt.want {
				t.Fatalf("isPhone(%q) = %v, want %v", tt.phone, got, tt.want)
			}
		})
	}
}

func TestRegisterRejectsInvalidTelephone(t *testing.T) {
	req := &request.RegisterRequest{
		Telephone: "not-a-phone",
		Password:  "password123",
		Nickname:  "alice",
	}

	user, err := Register(req)

	if user != nil {
		t.Fatalf("expected no user, got %+v", user)
	}

	if !errors.Is(err, ErrInvalidRegister) {
		t.Fatalf("expected ErrInvalidRegister, got %v", err)
	}
}
