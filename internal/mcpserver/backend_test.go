package mcpserver

import (
	"context"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"mychat/internal/mcpserver/contract"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type tokenTransport struct{ token string }

func (r tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := req.Clone(req.Context())
	c.Header = req.Header.Clone()
	c.Header.Set("Authorization", "Bearer "+r.token)
	return http.DefaultTransport.RoundTrip(c)
}
func TestInjectedSendBackendIsPerServer(t *testing.T) {
	key := []byte("test-jwt-secret-01234567890123456789")
	token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": "U-authenticated", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
	if e != nil {
		t.Fatal(e)
	}
	for _, expected := range []string{"server-one", "server-two"} {
		called := false
		server := httptest.NewServer(NewHTTPHandler(NewWithSend(func(_ context.Context, user string, in contract.SendMessageInput) (contract.SendMessageOutput, error) {
			called = true
			if user != "U-authenticated" || in.SessionUUID != "S-test" || in.ClientMessageID != "client-id" {
				t.Errorf("identity/input not propagated: %s %+v", user, in)
			}
			return contract.SendMessageOutput{Message: contract.Message{MessageUUID: expected}}, nil
		}), key))
		client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
		cs, e := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL, HTTPClient: &http.Client{Transport: tokenTransport{token}}, DisableStandaloneSSE: true}, nil)
		if e != nil {
			server.Close()
			t.Fatal(e)
		}
		out, e := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"session_uuid": "S-test", "content": "hello", "client_message_id": "client-id"}})
		if e != nil || out.IsError || !called {
			t.Errorf("backend not reached: %+v %v", out, e)
		}
		_ = cs.Close()
		server.Close()
	}
}
