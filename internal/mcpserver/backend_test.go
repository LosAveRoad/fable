package mcpserver

import (
	"context"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"mychat/internal/mcpserver/contract"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Requests must remain authorized and usable when the ingress changes Logic
// replicas between initialize, notifications and tools/call.
func TestMCPRequestsCanAlternateLogicReplicas(t *testing.T) {
	key := []byte("stateless-jwt-secret-01234567890123456789")
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": "U-authenticated", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	var called [2]atomic.Int32
	handlers := make([]http.Handler, 2)
	for i := range handlers {
		handlers[i] = NewStatelessHTTPHandler(NewWithSend(func(_ context.Context, user string, _ contract.SendMessageInput) (contract.SendMessageOutput, error) {
			if user != "U-authenticated" {
				t.Errorf("replica trusted wrong identity: %s", user)
			}
			called[i].Add(1)
			return contract.SendMessageOutput{Message: contract.Message{MessageUUID: "M-test"}}, nil
		}), key)
	}
	var next atomic.Uint32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handlers[(next.Add(1)-1)%2].ServeHTTP(w, r) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "replica-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL, HTTPClient: &http.Client{Transport: tokenTransport{token}}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for range 4 {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"session_uuid": "S-test", "content": "hello"}})
		if err != nil || result.IsError {
			t.Fatalf("replica switch failed: %+v %v", result, err)
		}
	}
	if called[0].Load() == 0 || called[1].Load() == 0 {
		t.Fatal("did not exercise both replica backends")
	}
}

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
