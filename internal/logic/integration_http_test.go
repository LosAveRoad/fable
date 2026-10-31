//go:build integration

package logic_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"mychat/internal/dao"
	"mychat/internal/dto/im"
	"mychat/internal/dto/response"
	"mychat/internal/logic"
	"mychat/internal/mcpserver/contract"
	"mychat/internal/model"
	"mychat/internal/service/chatservice"
)

// Legacy history/read tools still use DAO globals. These integration tests are
// intentionally serial and restore globals after the real HTTP server stops.
func integrationHTTP(t *testing.T, f *integrationFixture) *httptest.Server {
	t.Helper()
	oldDB, oldRedis := dao.GormDB, dao.RedisClient
	dao.GormDB = f.db
	dao.RedisClient = nil
	t.Cleanup(func() { dao.GormDB = oldDB; dao.RedisClient = oldRedis })
	server := httptest.NewServer(logic.NewHandler(f.service, f.db, func(context.Context) logic.DependencyStatus {
		return logic.DependencyStatus{Database: true, Routes: true, Push: true}
	}))
	t.Cleanup(server.Close)
	return server
}
func integrationHTTPCall(t *testing.T, server *httptest.Server, method, path, token string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	reply, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Body.Close()
	raw, err := io.ReadAll(reply.Body)
	if err != nil {
		t.Fatal(err)
	}
	return reply.StatusCode, raw
}

// Real JWT HTTP sync must expose exact large positions and current permissions;
// the retained history endpoint must see rows committed through Logic RPC.
func TestIntegrationLogicHTTPAuthenticatedSyncAndLegacyHistory(t *testing.T) {
	f := integrationNew(t)
	route, _ := f.connect(t, 0, 0)
	server := integrationHTTP(t, f)
	integrationDB(t, f.db.Exec("ALTER TABLE message AUTO_INCREMENT = 9007199254740993"))
	for _, content := range []string{"first through RPC", "second through RPC", "third through RPC"} {
		f.send(t, 0, f.request(route, f.users[1], content))
	}
	token := integrationJWT(t, f.users[0], time.Now().Add(time.Hour))
	path := "/api/v2/messages/sync?conversation_id=" + url.QueryEscape("direct:"+f.sessions[0]) + "&limit=2"
	for _, rawToken := range []string{"", "invalid.jwt", integrationJWT(t, f.users[0], time.Now().Add(-time.Minute))} {
		if code, body := integrationHTTPCall(t, server, http.MethodGet, path, rawToken, nil); code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated sync=%d %s", code, body)
		}
	}
	code, body := integrationHTTPCall(t, server, http.MethodGet, path, token, nil)
	var first im.SyncPage
	if code != http.StatusOK || json.Unmarshal(body, &first) != nil || len(first.Messages) != 2 || !first.HasMore || first.Messages[0].Position != "9007199254740993" || first.Messages[1].Position != "9007199254740994" {
		t.Fatalf("HTTP sync=%d %s", code, body)
	}
	next := path + "&cursor=" + url.QueryEscape(first.NextCursor)
	code, body = integrationHTTPCall(t, server, http.MethodGet, next, token, nil)
	var second im.SyncPage
	if code != 200 || json.Unmarshal(body, &second) != nil || len(second.Messages) != 1 || second.HasMore || second.Messages[0].Position != "9007199254740995" {
		t.Fatalf("HTTP next=%d %s", code, body)
	}
	outsider := integrationJWT(t, f.users[2], time.Now().Add(time.Hour))
	if code, body = integrationHTTPCall(t, server, http.MethodGet, path, outsider, nil); code != 403 {
		t.Fatalf("foreign sync=%d %s", code, body)
	}
	for _, invalid := range []string{"/api/v2/messages/sync?conversation_id=" + url.QueryEscape("direct:"+f.sessions[0]) + "&limit=501", path + "&cursor=not-a-cursor"} {
		if code, body = integrationHTTPCall(t, server, http.MethodGet, invalid, token, nil); code != 400 {
			t.Fatalf("invalid sync query=%d %s", code, body)
		}
	}
	raw, _ := json.Marshal(map[string]string{"user_one_id": f.users[0], "user_two_id": f.users[1]})
	code, body = integrationHTTPCall(t, server, http.MethodPost, "/message/getMessageList", token, raw)
	var history []response.MessageResponse
	if code != 200 || json.Unmarshal(body, &history) != nil || len(history) != 3 {
		t.Fatalf("legacy history cannot read Logic writes=%d %s", code, body)
	}
	found := map[string]bool{}
	for _, message := range history {
		found[message.Content] = true
		if message.SessionID != f.sessions[0] || message.Origin != 0 {
			t.Fatalf("history=%+v", message)
		}
	}
	for _, content := range []string{"first through RPC", "second through RPC", "third through RPC"} {
		if !found[content] {
			t.Fatalf("history lost %q", content)
		}
	}
	for _, wsPath := range []string{"/wss", "/wss/v2"} {
		if code, body = integrationHTTPCall(t, server, http.MethodGet, wsPath, token, nil); code != 404 {
			t.Fatalf("Logic exposed WebSocket endpoint %s: %d %s", wsPath, code, body)
		}
	}
	integrationDB(t, f.db.Where("uuid = ?", f.sessions[0]).Delete(&model.Session{}))
	if code, body = integrationHTTPCall(t, server, http.MethodGet, next, token, nil); code != 403 {
		t.Fatalf("sync ignored current permission=%d %s", code, body)
	}
}

type integrationBearerTransport struct{ token string }

func (r integrationBearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	cloned.Header.Set("Authorization", "Bearer "+r.token)
	return http.DefaultTransport.RoundTrip(cloned)
}
func integrationMCPOutput(t *testing.T, result *mcp.CallToolResult) contract.SendMessageOutput {
	t.Helper()
	if result == nil || result.IsError {
		t.Fatalf("MCP send returned tool error: %+v", result)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var output contract.SendMessageOutput
	if err = json.Unmarshal(raw, &output); err != nil {
		t.Fatal(err)
	}
	if output.Result == nil || !output.Result.Stored || output.Result.Message.MessageID == "" || output.Message.MessageUUID != output.Result.Message.MessageID {
		t.Fatalf("MCP omitted commit/dispatch result: %s", raw)
	}
	return output
}

// MCP must reach the production Logic pipeline over the real authenticated
// endpoint, with SQL whitelist/idempotency, AI normalization, and dispatch state.
func TestIntegrationLogicMCPRealEndpointUsesLogicAndReauthorizesReplay(t *testing.T) {
	f := integrationNew(t)
	server := integrationHTTP(t, f)
	// A live legacy socket is an observer: Logic MCP must not route through the
	// old process-global ChatServer in addition to publishing a push task.
	oldChat := chatservice.ChatServer
	legacy := chatservice.NewServer(8)
	chatservice.ChatServer = legacy
	go legacy.Start()
	t.Cleanup(func() { legacy.Close(); chatservice.ChatServer = oldChat })
	upgraded := make(chan error, 1)
	upgrader := websocket.Upgrader{}
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			if !chatservice.NewClient(legacy, socket, f.users[0], 8).Start() {
				err = io.ErrClosedPipe
			}
		}
		upgraded <- err
	}))
	t.Cleanup(wsServer.Close)
	socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = socket.Close() })
	if err = <-upgraded; err != nil {
		t.Fatal(err)
	}
	token := integrationJWT(t, f.users[0], time.Now().Add(time.Hour))
	client := mcp.NewClient(&mcp.Implementation{Name: "real-logic-integration", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: integrationBearerTransport{token}, Timeout: 3 * time.Second}, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	id := uuid.NewString()
	arguments := map[string]any{"session_uuid": f.sessions[0], "client_message_id": id, "content": " \nAI through HTTP MCP\t "}
	denied, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: arguments})
	if err != nil || denied == nil || !denied.IsError {
		t.Fatalf("MCP absent whitelist=%+v %v", denied, err)
	}
	f.count(t, 0)
	integrationDB(t, f.db.Create(&model.UserAISessionAccess{UserUUID: f.users[0], SessionUUID: f.sessions[0]}))
	f.publisher.fail(logic.ErrNotPublished)
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	first := integrationMCPOutput(t, result)
	if first.Message.Content != "AI through HTTP MCP" || first.Message.Origin != 1 || first.Result.Message.Origin != im.MessageOriginAI || first.Result.Dispatch != im.DispatchNotQueued || first.Result.Replayed || first.Result.ClientMessageID != id {
		t.Fatalf("MCP normalization/dispatch=%+v", first)
	}
	f.publisher.fail(nil)
	arguments["content"] = "AI through HTTP MCP"
	result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	replay := integrationMCPOutput(t, result)
	if !replay.Result.Replayed || replay.Message.MessageUUID != first.Message.MessageUUID || replay.Result.Dispatch != im.DispatchQueued {
		t.Fatalf("MCP replay=%+v", replay)
	}
	f.count(t, 1)
	delete(arguments, "client_message_id")
	arguments["content"] = " generated ID "
	result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	generated := integrationMCPOutput(t, result)
	if uuid.Validate(generated.Result.ClientMessageID) != nil || generated.Message.Content != "generated ID" || generated.Result.Replayed {
		t.Fatalf("optional MCP id=%+v", generated)
	}
	f.count(t, 2)
	integrationDB(t, f.db.Where("user_uuid = ?", f.users[0]).Delete(&model.UserAISessionAccess{}))
	arguments["client_message_id"] = id
	arguments["content"] = "AI through HTTP MCP"
	denied, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: arguments})
	if err != nil || denied == nil || !denied.IsError {
		t.Fatalf("revoked MCP replay=%+v %v", denied, err)
	}
	f.count(t, 2)
	if len(f.publisher.records()) != 3 {
		t.Fatal("denied MCP sent a push task")
	}
	for _, record := range f.publisher.records() {
		task := integrationTask(t, record)
		if task.Message.Origin != im.MessageOriginAI || task.Message.SendID != f.users[0] || task.Message.ConversationID != "direct:"+f.sessions[0] {
			t.Fatalf("MCP push=%+v", task)
		}
	}
	_ = socket.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, _, err = socket.ReadMessage(); err == nil {
		t.Fatal("Logic MCP sent a duplicate through legacy ChatServer")
	}
	if netErr, ok := err.(interface{ Timeout() bool }); !ok || !netErr.Timeout() {
		t.Fatalf("legacy observer closed unexpectedly: %v", err)
	}
	if code, body := integrationHTTPCall(t, server, http.MethodPost, "/mcp", "", []byte(`{}`)); code != 401 {
		t.Fatalf("anonymous MCP=%d %s", code, body)
	}
	raw, _ := json.Marshal(map[string]string{"user_one_id": f.users[0], "user_two_id": f.users[1]})
	code, body := integrationHTTPCall(t, server, http.MethodPost, "/message/getMessageList", token, raw)
	var history []response.MessageResponse
	if code != 200 || json.Unmarshal(body, &history) != nil || len(history) != 2 {
		t.Fatalf("legacy history cannot read MCP Logic writes=%d %s", code, body)
	}
	for _, message := range history {
		if message.Origin != 1 {
			t.Fatalf("legacy history lost AI origin=%+v", message)
		}
	}
}
