package mcpserver

import (
	"context"
	"net/http"

	appauth "mychat/internal/auth"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

func requireBearerToken(jwtSecret []byte) func(http.Handler) http.Handler {
	return mcpauth.RequireBearerToken(newTokenVerifier(jwtSecret), nil)
}

func newTokenVerifier(jwtSecret []byte) mcpauth.TokenVerifier {
	return func(_ context.Context, rawToken string, _ *http.Request) (*mcpauth.TokenInfo, error) {
		claims, err := appauth.ParseJWT(rawToken, jwtSecret)
		if err != nil || claims.ExpiresAt == nil {
			return nil, mcpauth.ErrInvalidToken
		}
		return &mcpauth.TokenInfo{
			UserID:     claims.UserUUID,
			Expiration: claims.ExpiresAt.Time,
		}, nil
	}
}
