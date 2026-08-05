//go:build integration

package integration

import (
	"net/http"
	"testing"
)

func TestUserSessionAndHistoryFlow(t *testing.T) {
	userA := createTestUser(t)
	userB := createTestUser(t)

	openSession(t, userA, userB)

	var sessions []struct {
		PeerUUID string `json:"peer_uuid"`
	}
	resp := postJSON(t, "/session/getUserSessionList", map[string]string{}, userA.Token, &sessions)
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("session list status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	resp.Body.Close()
	if len(sessions) != 1 || sessions[0].PeerUUID != userB.UUID {
		t.Fatalf("sessions = %#v, want one session with peer %s", sessions, userB.UUID)
	}

	var messages []struct {
		SendID    string `json:"send_id"`
		ReceiveID string `json:"receive_id"`
		Content   string `json:"content"`
	}
	resp = postJSON(t, "/message/getMessageList", map[string]string{
		"user_one_id": userA.UUID,
		"user_two_id": userB.UUID,
	}, userA.Token, &messages)
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("message list status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	resp.Body.Close()
	if len(messages) != 0 {
		t.Fatalf("new session has %d messages, want 0", len(messages))
	}
}

func TestProtectedEndpointRejectsMissingToken(t *testing.T) {
	resp := postJSON(t, "/session/getUserSessionList", map[string]string{}, "", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}
