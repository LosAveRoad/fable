package mcpserver

import (
	"context"
	"testing"
	"time"

	appauth "mychat/internal/auth"

	"github.com/golang-jwt/jwt/v5"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

func TestTokenVerifierReturnsAuthenticatedUser(t *testing.T) {
	secret := []byte("test-secret")
	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, appauth.Claims{
		UserUUID: "U001",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}

	info, err := newTokenVerifier(secret)(context.Background(), token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.UserID != "U001" || !info.Expiration.Equal(expiresAt) {
		t.Fatalf("token info = %+v", info)
	}
}

func TestTokenVerifierRejectsInvalidToken(t *testing.T) {
	_, err := newTokenVerifier([]byte("test-secret"))(context.Background(), "invalid", nil)
	if err != mcpauth.ErrInvalidToken {
		t.Fatalf("error = %v, want %v", err, mcpauth.ErrInvalidToken)
	}
}
