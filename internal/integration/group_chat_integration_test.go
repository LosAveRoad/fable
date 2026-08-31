//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
	"mychat/internal/dao"
	"mychat/internal/model"
	"mychat/internal/service/redisservice"
)

func TestGroupChatHTTPAndWebSocketFlow(t *testing.T) {
	owner := createTestUser(t)
	member := createTestUser(t)

	var group struct {
		UUID    string   `json:"group_id"`
		Members []string `json:"members"`
	}
	resp := postJSON(t, "/group/create", map[string]string{"name": "integration group"}, owner.Token, &group)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create group status = %d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = postJSON(t, "/group/join", map[string]string{"group_id": group.UUID}, member.Token, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("join group status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	connOwner, _, err := websocket.DefaultDialer.Dial(websocketURL(t, owner), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connOwner.Close()
	connMember, _, err := websocket.DefaultDialer.Dial(websocketURL(t, member), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connMember.Close()
	waitForOnlineConnections(t, 2)
	if err := connOwner.WriteJSON(wschatMessage{SendID: owner.UUID, ReceiveID: group.UUID, ReceiveType: 1, Content: "hello group"}); err != nil {
		t.Fatal(err)
	}
	gotOwner := waitForMessage(t, connOwner)
	gotMember := waitForMessage(t, connMember)
	for _, got := range []wschatMessage{gotOwner, gotMember} {
		if got.ReceiveID != group.UUID || got.SendID != owner.UUID || got.Content != "hello group" || got.ReceiveType != 1 {
			t.Fatalf("group message = %+v", got)
		}
	}

	var count int64
	if err := dao.GormDB.Model(&model.Message{}).Where("receive_id = ? AND content = ?", group.UUID, "hello group").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("persisted group message count = %d, want 1", count)
	}
	var history []struct {
		Content string `json:"content"`
	}
	resp = postJSON(t, "/group/message/list", map[string]string{"group_id": group.UUID}, member.Token, &history)
	if resp.StatusCode != http.StatusOK || len(history) != 1 || history[0].Content != "hello group" {
		t.Fatalf("group history response: status=%d history=%+v", resp.StatusCode, history)
	}
	resp.Body.Close()
	if exists, err := dao.RedisClient.Exists(context.Background(), redisservice.GroupMessageListKey(group.UUID)).Result(); err != nil || exists != 1 {
		t.Fatalf("group message cache exists=%d err=%v", exists, err)
	}
	_ = connOwner.Close()
	_ = connMember.Close()
	waitForOnlineConnections(t, 0)
}
