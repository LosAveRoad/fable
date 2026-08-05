package chatservice

import (
	"mychat/internal/dao"
	"mychat/internal/dto/wschat"
	"mychat/internal/model"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	onlineUsers = make(map[string]*Client)
	mu          sync.Mutex
)

type Client struct {
	ch       chan wschat.Message
	userUUID string
}

func RegisterUser(userUUID string) {
	newClient := Client{
		ch:       make(chan wschat.Message),
		userUUID: userUUID,
	}
	mu.Lock()
	onlineUsers[userUUID] = &newClient
	mu.Unlock()
}

func ReadPump(conn *websocket.Conn, userUUID string) {
	defer func() {
		mu.Lock()
		delete(onlineUsers, userUUID)
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

		onlineUsers[msg.ReceiveId].ch <- msg
	}
}

func WritePump(conn *websocket.Conn, userUUID string) {
	defer func() {
		mu.Lock()
		delete(onlineUsers, userUUID)
		mu.Unlock()
		_ = conn.Close()
	}()
	for {
		msg := <-onlineUsers[userUUID].ch
		conn.WriteJSON(msg)
	}
}
