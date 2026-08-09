package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func signTestToken(t *testing.T, secret []byte, claims Claims) string {
	t.Helper()
	raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestParseJWT(t *testing.T) {
	secret := []byte("test-secret")
	raw := signTestToken(t, secret, Claims{
		UserUUID: "U001",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	})

	claims, err := ParseJWT(raw, secret)
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserUUID != "U001" {
		t.Fatalf("user uuid = %q, want U001", claims.UserUUID)
	}
}

func TestParseJWTRejectsMissingExpiration(t *testing.T) {
	secret := []byte("test-secret")
	raw := signTestToken(t, secret, Claims{UserUUID: "U001"})
	if _, err := ParseJWT(raw, secret); err == nil {
		t.Fatal("ParseJWT accepted token without expiration")
	}
}
