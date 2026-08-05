package response

import "time"

type MessageResponse struct {
	UUID      string    `json:"uuid"`
	SessionID string    `json:"session_id"`
	Type      int8      `json:"type"`
	Content   string    `json:"content"`
	SendID    string    `json:"send_id"`
	ReceiveID string    `json:"receive_id"`
	CreatedAt time.Time `json:"created_at"`
}
