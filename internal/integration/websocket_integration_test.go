//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"mychat/internal/dao"
	"mychat/internal/model"
	"mychat/internal/service/chatservice"

	"github.com/gorilla/websocket"
)

func TestWebSocketMessageFlow(t *testing.T) {
	waitForOnlineConnections(t, 0)
	userA := createTestUser(t)
	userB := createTestUser(t)
	openSession(t, userA, userB)

	connA, responseA, err := websocket.DefaultDialer.Dial(websocketURL(t, userA), nil)
	if err != nil {
		t.Fatalf("connect user A: %v", err)
	}
	defer connA.Close()
	if responseA.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("user A websocket status = %d", responseA.StatusCode)
	}

	connB, responseB, err := websocket.DefaultDialer.Dial(websocketURL(t, userB), nil)
	if err != nil {
		t.Fatalf("connect user B: %v", err)
	}
	defer connB.Close()
	if responseB.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("user B websocket status = %d", responseB.StatusCode)
	}
	waitForOnlineConnections(t, 2)

	if err := connA.WriteJSON(wschatMessage{
		SendID:    userA.UUID,
		ReceiveID: userB.UUID,
		Content:   "hello from A",
	}); err != nil {
		t.Fatalf("send websocket message: %v", err)
	}

	received := waitForMessage(t, connB)
	if received.SendID != userA.UUID {
		t.Fatalf("send_id = %s, want %s", received.SendID, userA.UUID)
	}
	if received.ReceiveID != userB.UUID {
		t.Fatalf("receive_id = %s, want %s", received.ReceiveID, userB.UUID)
	}
	if received.Content != "hello from A" {
		t.Fatalf("content = %q", received.Content)
	}

	if err := connB.WriteJSON(wschatMessage{
		SendID:    userB.UUID,
		ReceiveID: userA.UUID,
		Content:   "hello from B",
	}); err != nil {
		t.Fatalf("send reverse websocket message: %v", err)
	}

	reverseReceived := waitForMessage(t, connA)
	if reverseReceived.SendID != userB.UUID || reverseReceived.ReceiveID != userA.UUID {
		t.Fatalf("reverse message route = %s -> %s, want %s -> %s",
			reverseReceived.SendID, reverseReceived.ReceiveID, userB.UUID, userA.UUID)
	}
	if reverseReceived.Content != "hello from B" {
		t.Fatalf("reverse content = %q", reverseReceived.Content)
	}

	var count int64
	if err := dao.GormDB.Model(&model.Message{}).
		Where("send_id = ? AND receive_id = ? AND content IN ?", userA.UUID, userB.UUID, []string{"hello from A", "hello from B"}).
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("persisted forward message count = %d, want 1", count)
	}
	if err := dao.GormDB.Model(&model.Message{}).
		Where("send_id = ? AND receive_id = ? AND content = ?", userB.UUID, userA.UUID, "hello from B").
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("persisted reverse message count = %d, want 1", count)
	}

	_ = connA.Close()
	_ = connB.Close()
	waitForOnlineConnections(t, 0)
}

func TestWebSocketPersistsMessageForOfflineReceiver(t *testing.T) {
	waitForOnlineConnections(t, 0)
	userA := createTestUser(t)
	userB := createTestUser(t)
	openSession(t, userA, userB)

	connA, _, err := websocket.DefaultDialer.Dial(websocketURL(t, userA), nil)
	if err != nil {
		t.Fatalf("connect user A: %v", err)
	}
	defer connA.Close()
	waitForOnlineConnections(t, 1)

	if err := connA.WriteJSON(wschatMessage{
		SendID:    userA.UUID,
		ReceiveID: userB.UUID,
		Content:   "message for offline user",
	}); err != nil {
		t.Fatalf("send websocket message: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var count int64
		if err := dao.GormDB.Model(&model.Message{}).
			Where("send_id = ? AND receive_id = ? AND content = ?", userA.UUID, userB.UUID, "message for offline user").
			Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("offline receiver message was not persisted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if chatservice.ChatServer.OnlineCount() != 1 {
		t.Fatalf("server online count = %d, want sender to remain connected", chatservice.ChatServer.OnlineCount())
	}

	_ = connA.Close()
	waitForOnlineConnections(t, 0)
}

func TestWebSocketRejectsMissingToken(t *testing.T) {
	parsedURL, err := url.Parse(testServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	parsedURL.Scheme = "ws"
	parsedURL.Path = "/wss"

	conn, response, err := websocket.DefaultDialer.Dial(parsedURL.String(), nil)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("websocket connection succeeded without a token")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		if response == nil {
			t.Fatal("missing HTTP response for rejected websocket")
		}
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}
}

func waitForOnlineConnections(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if chatservice.ChatServer.OnlineCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("online connection count = %d, want %d", chatservice.ChatServer.OnlineCount(), want)
}
