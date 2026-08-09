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

	"github.com/gorilla/websocket"
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

func TestMCPSendMessageRequiresSessionAccessAndSyncsBothUsers(t *testing.T) {
	waitForOnlineConnections(t, 0)
	userA := createTestUser(t)
	userB := createTestUser(t)
	openSession(t, userA, userB)

	var setting response.AISettingResponse
	resp := requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userA.Token, &setting)
	if resp.StatusCode != http.StatusOK || len(setting.Sessions) != 1 {
		t.Fatalf("get setting status=%d setting=%+v", resp.StatusCode, setting)
	}
	sessionUUID := setting.Sessions[0].SessionUUID

	client := connectMCPClient(t, userA.Token)
	ctx := context.Background()
	denied, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{
		"session_uuid": sessionUUID,
		"content":      "AI-assisted hello",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !denied.IsError {
		t.Fatalf("send_message succeeded before authorization: %+v", denied)
	}

	var count int64
	if err := dao.GormDB.Model(&model.Message{}).
		Where("session_id = ? AND content = ?", sessionUUID, "AI-assisted hello").
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("messages before authorization = %d, want 0", count)
	}

	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{sessionUUID},
	}, userA.Token, &setting)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authorize status = %d", resp.StatusCode)
	}

	connA, _, err := websocket.DefaultDialer.Dial(websocketURL(t, userA), nil)
	if err != nil {
		t.Fatalf("connect user A: %v", err)
	}
	defer connA.Close()
	connB, _, err := websocket.DefaultDialer.Dial(websocketURL(t, userB), nil)
	if err != nil {
		t.Fatalf("connect user B: %v", err)
	}
	defer connB.Close()
	waitForOnlineConnections(t, 2)

	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{
		"session_uuid": sessionUUID,
		"content":      "AI-assisted hello",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("send_message returned tool error: %+v", result)
	}
	output := decodeStructuredOutput[contract.SendMessageOutput](t, result)
	if output.Message.SessionUUID != sessionUUID || output.Message.Content != "AI-assisted hello" || output.Message.Origin != model.MessageOriginAI {
		t.Fatalf("send output = %+v", output)
	}

	for name, connection := range map[string]*websocket.Conn{"sender": connA, "receiver": connB} {
		received := waitForMessage(t, connection)
		if received.SendID != userA.UUID || received.ReceiveID != userB.UUID || received.Content != "AI-assisted hello" || received.Origin != model.MessageOriginAI {
			t.Fatalf("%s received = %+v", name, received)
		}
	}

	var persisted model.Message
	if err := dao.GormDB.Where("uuid = ?", output.Message.MessageUUID).First(&persisted).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.SessionId != sessionUUID || persisted.SendId != userA.UUID || persisted.ReceiveId != userB.UUID || persisted.Origin != model.MessageOriginAI {
		t.Fatalf("persisted message = %+v", persisted)
	}

	_ = connA.Close()
	_ = connB.Close()
	waitForOnlineConnections(t, 0)

	offline, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{
		"session_uuid": sessionUUID,
		"content":      "persist while offline",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if offline.IsError {
		t.Fatalf("offline send returned tool error: %+v", offline)
	}
	offlineOutput := decodeStructuredOutput[contract.SendMessageOutput](t, offline)
	persisted = model.Message{}
	if err := dao.GormDB.Where("uuid = ? AND origin = ?", offlineOutput.Message.MessageUUID, model.MessageOriginAI).
		First(&persisted).Error; err != nil {
		t.Fatal(err)
	}
}
