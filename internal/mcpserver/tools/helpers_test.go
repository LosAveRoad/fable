package tools

import (
	"testing"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestAuthenticatedUserUUID(t *testing.T) {
	req := &mcp.CallToolRequest{Extra: &mcp.RequestExtra{TokenInfo: &auth.TokenInfo{UserID: "U001"}}}
	got, err := authenticatedUserUUID(req)
	if err != nil || got != "U001" {
		t.Fatalf("authenticatedUserUUID() = %q, %v", got, err)
	}
	if _, err := authenticatedUserUUID(&mcp.CallToolRequest{}); err == nil {
		t.Fatal("missing token info was accepted")
	}
}
