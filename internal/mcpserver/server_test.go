package mcpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestServerListsExpectedTools(t *testing.T) {
	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := New().Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.1.0"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = clientSession.Close()
		_ = serverSession.Wait()
	}()

	result, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
		if tool.InputSchema == nil || tool.OutputSchema == nil {
			t.Fatalf("tool %s is missing inferred schemas", tool.Name)
		}
		if tool.Annotations == nil {
			t.Fatalf("tool %s is missing annotations", tool.Name)
		}
		if tool.Name == "send_message" {
			if tool.Annotations.ReadOnlyHint || tool.Annotations.IdempotentHint || tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint {
				t.Fatalf("send_message annotations = %+v", tool.Annotations)
			}
		} else if !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool %s is not marked read-only", tool.Name)
		}
	}
	sort.Strings(names)
	want := []string{"get_recent_messages", "list_sessions", "search_messages", "send_message"}
	if len(names) != len(want) {
		t.Fatalf("tool names = %#v, want %#v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("tool names = %#v, want %#v", names, want)
		}
	}
}

func TestHTTPHandlerRequiresBearerToken(t *testing.T) {
	handler := NewHTTPHandler(New(), []byte("test-secret"))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}
