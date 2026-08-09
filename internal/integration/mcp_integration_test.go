//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/mcpserver/contract"
	"mychat/internal/model"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type bearerRoundTripper struct {
	token string
	base  http.RoundTripper
}

func (r bearerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+r.token)
	return r.base.RoundTrip(clone)
}

func connectMCPClient(t *testing.T, token string) *mcp.ClientSession {
	t.Helper()
	transport := &mcp.StreamableClientTransport{
		Endpoint: testServer.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearerRoundTripper{
			token: token,
			base:  http.DefaultTransport,
		}},
		DisableStandaloneSSE: true,
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "v0.1.0"}, nil)
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func decodeStructuredOutput[T any](t *testing.T, result *mcp.CallToolResult) T {
	t.Helper()
	var output T
	payload, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func TestMCPReadOnlyToolsRespectAISessionAccess(t *testing.T) {
	userA := createTestUser(t)
	userB := createTestUser(t)
	openSession(t, userA, userB)

	var setting response.AISettingResponse
	resp := requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userA.Token, &setting)
	if resp.StatusCode != http.StatusOK || len(setting.Sessions) != 1 {
		t.Fatalf("get setting status=%d setting=%+v", resp.StatusCode, setting)
	}
	sessionUUID := setting.Sessions[0].SessionUUID

	now := time.Now().UTC()
	messages := []model.Message{
		{UUID: "M-mcp-001", SessionId: sessionUUID, Type: 1, Content: "project alpha deadline Friday", SendId: userB.UUID, ReceiveId: userA.UUID, CreatedAt: now.Add(-time.Minute)},
		{UUID: "M-mcp-002", SessionId: sessionUUID, Type: 1, Content: "acknowledged", SendId: userA.UUID, ReceiveId: userB.UUID, CreatedAt: now},
	}
	if err := dao.GormDB.Create(&messages).Error; err != nil {
		t.Fatal(err)
	}

	client := connectMCPClient(t, userA.Token)
	ctx := context.Background()

	before, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "list_sessions", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	beforeOutput := decodeStructuredOutput[contract.ListSessionsOutput](t, before)
	if len(beforeOutput.Sessions) != 0 {
		t.Fatalf("sessions before authorization = %+v", beforeOutput.Sessions)
	}

	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{sessionUUID},
	}, userA.Token, &setting)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authorize status = %d", resp.StatusCode)
	}

	listed, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "list_sessions", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	listOutput := decodeStructuredOutput[contract.ListSessionsOutput](t, listed)
	if len(listOutput.Sessions) != 1 || listOutput.Sessions[0].SessionUUID != sessionUUID {
		t.Fatalf("list output = %+v", listOutput)
	}

	recent, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "get_recent_messages", Arguments: map[string]any{
		"session_uuid": sessionUUID,
		"limit":        10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	recentOutput := decodeStructuredOutput[contract.GetRecentMessagesOutput](t, recent)
	if len(recentOutput.Messages) != 2 || recentOutput.Messages[0].MessageUUID != "M-mcp-001" {
		t.Fatalf("recent output = %+v", recentOutput)
	}

	searched, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "search_messages", Arguments: map[string]any{
		"query": "alpha",
	}})
	if err != nil {
		t.Fatal(err)
	}
	searchOutput := decodeStructuredOutput[contract.SearchMessagesOutput](t, searched)
	if len(searchOutput.Messages) != 1 || searchOutput.Messages[0].MessageUUID != "M-mcp-001" {
		t.Fatalf("search output = %+v", searchOutput)
	}

	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{},
	}, userA.Token, &setting)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("revoke status = %d", resp.StatusCode)
	}

	denied, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "get_recent_messages", Arguments: map[string]any{
		"session_uuid": sessionUUID,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !denied.IsError {
		t.Fatalf("get_recent_messages succeeded after revocation: %+v", denied)
	}
}
