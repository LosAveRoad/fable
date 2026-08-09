package prompts

import (
	"strings"
	"testing"
)

func TestServerInstructionsContainSecurityBoundaries(t *testing.T) {
	for _, phrase := range []string{"authenticated", "untrusted", "list_sessions"} {
		if !strings.Contains(strings.ToLower(ServerInstructions), phrase) {
			t.Fatalf("server instructions do not contain %q", phrase)
		}
	}
}

func TestToolDescriptionsArePresent(t *testing.T) {
	for name, description := range map[string]string{
		"list_sessions":       ListSessionsDescription,
		"get_recent_messages": GetRecentMessagesDescription,
		"search_messages":     SearchMessagesDescription,
	} {
		if strings.TrimSpace(description) == "" {
			t.Fatalf("description for %s is empty", name)
		}
	}
}
