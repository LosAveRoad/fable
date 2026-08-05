package chatservice

import (
	"mychat/internal/dao"
	"mychat/internal/dto/wschat"
	"mychat/internal/model"
	"slices"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	OnlineUsers []string
	mu          sync.Mutex
)

var ch = make(chan wschat.Message)

func RegisterUser(uuid string) {
	mu.Lock()
	OnlineUsers = append(OnlineUsers, uuid)
	mu.Unlock()
}

func ReadPump(conn *websocket.Conn, userUUID string) {
	defer func() {
		mu.Lock()
		for i, u := range OnlineUsers {
			if u == userUUID {
				OnlineUsers = append(OnlineUsers[:i], OnlineUsers[i+1:]...)
				break
			}
		}
		mu.Unlock()
		_ = conn.Close()
	}()
	for {
		var msg wschat.Message
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		if msg.SendId != userUUID {
			return
		}

		var session model.Session
		if err := dao.GormDB.Model(&model.Session{}).Where("send_id = ? AND receive_id = ?", userUUID, msg.ReceiveId).First(&session).Error; err != nil {
			return
		}

		messageUuid := "M" + uuid.NewString()
		dao.GormDB.Create(
			&model.Message{
				UUID:      messageUuid,
				SessionId: session.UUID,
				Type:      0,
				Content:   msg.Content,
				SendId:    msg.SendId,
				ReceiveId: msg.ReceiveId,
			})

		ch <- msg
	}
}

func WritePump(conn *websocket.Conn, userUUID string) {
	defer func() {
		mu.Lock()
		for i, u := range OnlineUsers {
			if u == userUUID {
				OnlineUsers = append(OnlineUsers[:i], OnlineUsers[i+1:]...)
				break
			}
		}
		mu.Unlock()
		_ = conn.Close()
	}()

	for {
		msg := <-ch

		if slices.Contains(OnlineUsers, msg.ReceiveId) {
			if msg.ReceiveId == userUUID {
				conn.WriteJSON(msg)
			} else {
				ch <- msg
			}
		}
	}
}
