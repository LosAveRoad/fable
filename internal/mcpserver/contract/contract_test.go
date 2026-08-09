package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestContractsExposeUUIDsNotDatabaseIDs(t *testing.T) {
	payload, err := json.Marshal(ListSessionsOutput{Sessions: []Session{{
		SessionUUID: "S001",
		Peer:        UserSummary{UUID: "U002", Name: "Alice"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	if !strings.Contains(text, `"session_uuid":"S001"`) || !strings.Contains(text, `"uuid":"U002"`) {
		t.Fatalf("missing UUID fields: %s", text)
	}
	if strings.Contains(text, `"id"`) || strings.Contains(text, `"session_id"`) {
		t.Fatalf("database ID leaked through MCP contract: %s", text)
	}
}

func TestSendMessageInputDoesNotAcceptUserIdentities(t *testing.T) {
	payload, err := json.Marshal(SendMessageInput{SessionUUID: "S001", Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	if !strings.Contains(text, `"session_uuid":"S001"`) {
		t.Fatalf("missing session UUID: %s", text)
	}
	for _, forbidden := range []string{"sender", "receiver", "user_uuid"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("send input exposes %q: %s", forbidden, text)
		}
	}
}
