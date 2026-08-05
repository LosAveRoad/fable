//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"mychat/internal/dao"
	"mychat/internal/model"

	"github.com/gorilla/websocket"
)

func TestWebSocketMessageFlow(t *testing.T) {
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

	if err := connA.WriteJSON(wschatMessage{
		SendID:    userA.UUID,
		ReceiveID: userB.UUID,
		Content:   "hello from integration test",
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
	if received.Content != "hello from integration test" {
		t.Fatalf("content = %q", received.Content)
	}

	var count int64
	if err := dao.GormDB.Model(&model.Message{}).
		Where("send_id = ? AND receive_id = ? AND content = ?", userA.UUID, userB.UUID, received.Content).
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("persisted message count = %d, want 1", count)
	}

	// Closing both clients gives the current pump implementation time to unregister them.
	_ = connA.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_ = connB.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
}

func TestWebSocketRejectsInvalidClientID(t *testing.T) {
	user := createTestUser(t)
	parsedURL, err := url.Parse(websocketURL(t, user))
	if err != nil {
		t.Fatal(err)
	}
	query := parsedURL.Query()
	query.Set("client_id", "U-not-the-token-user")
	parsedURL.RawQuery = query.Encode()

	conn, response, err := websocket.DefaultDialer.Dial(parsedURL.String(), nil)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("websocket connection succeeded with mismatched client_id")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		if response == nil {
			t.Fatal("missing HTTP response for rejected websocket")
		}
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
}
