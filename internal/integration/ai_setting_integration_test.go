//go:build integration

package integration

import (
	"net/http"
	"testing"

	"mychat/internal/dto/response"
)

func TestAISettingAuthorizationFlow(t *testing.T) {
	userA := createTestUser(t)
	userB := createTestUser(t)
	userC := createTestUser(t)
	openSession(t, userA, userB)
	openSession(t, userB, userC)

	var initial response.AISettingResponse
	resp := requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userA.Token, &initial)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial setting status = %d", resp.StatusCode)
	}
	if len(initial.Sessions) != 1 || initial.Sessions[0].Peer.UUID != userB.UUID || initial.Sessions[0].AIAccessAllowed {
		t.Fatalf("initial setting = %+v", initial)
	}
	sessionUUID := initial.Sessions[0].SessionUUID

	var changed response.AISettingResponse
	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{sessionUUID, sessionUUID},
	}, userA.Token, &changed)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("change setting status = %d", resp.StatusCode)
	}
	if len(changed.Sessions) != 1 || !changed.Sessions[0].AIAccessAllowed {
		t.Fatalf("changed setting = %+v", changed)
	}

	var userBSetting response.AISettingResponse
	resp = requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userB.Token, &userBSetting)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("user B setting status = %d", resp.StatusCode)
	}
	for _, session := range userBSetting.Sessions {
		if session.AIAccessAllowed {
			t.Fatalf("user A authorization leaked into user B setting: %+v", userBSetting)
		}
	}

	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{},
	}, userA.Token, &changed)
	if resp.StatusCode != http.StatusOK || changed.Sessions[0].AIAccessAllowed {
		t.Fatalf("revoked setting status=%d setting=%+v", resp.StatusCode, changed)
	}
}

func TestAISettingRejectsForeignSessionWithoutChangingExistingAccess(t *testing.T) {
	userA := createTestUser(t)
	userB := createTestUser(t)
	userC := createTestUser(t)
	openSession(t, userA, userB)
	openSession(t, userB, userC)

	var settingA response.AISettingResponse
	resp := requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userA.Token, &settingA)
	if resp.StatusCode != http.StatusOK || len(settingA.Sessions) != 1 {
		t.Fatalf("user A setting status=%d setting=%+v", resp.StatusCode, settingA)
	}
	var settingC response.AISettingResponse
	resp = requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userC.Token, &settingC)
	if resp.StatusCode != http.StatusOK || len(settingC.Sessions) != 1 {
		t.Fatalf("user C setting status=%d setting=%+v", resp.StatusCode, settingC)
	}

	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{settingA.Sessions[0].SessionUUID},
	}, userA.Token, &settingA)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initial authorization status = %d", resp.StatusCode)
	}

	resp = requestJSON(t, http.MethodPut, "/api/v1/ai/setting", map[string]any{
		"allowed_session_uuids": []string{settingA.Sessions[0].SessionUUID, settingC.Sessions[0].SessionUUID},
	}, userA.Token, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign session status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}

	var after response.AISettingResponse
	resp = requestJSON(t, http.MethodGet, "/api/v1/ai/setting", nil, userA.Token, &after)
	if resp.StatusCode != http.StatusOK || len(after.Sessions) != 1 || !after.Sessions[0].AIAccessAllowed {
		t.Fatalf("setting changed after rejected request: status=%d setting=%+v", resp.StatusCode, after)
	}
}
