package wschat

import "time"

type ChatEvent struct {
	EventID     string    `json:"event_id"`
	SenderID    string    `json:"sender_id"`
	ReceiveID   string    `json:"receive_id"`
	ReceiveType int8      `json:"receive_type"`
	Content     string    `json:"content"`
	Origin      int8      `json:"origin"`
	CreatedAt   time.Time `json:"created_at"`
}
