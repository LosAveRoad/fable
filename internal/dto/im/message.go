// Package im declares the new IM transport contracts. It contains no business
// logic, database models, RPC clients or adapters for the existing wire format.
package im

import "time"

type ReceiveType string

const (
	ReceiveTypeUser  ReceiveType = "user"
	ReceiveTypeGroup ReceiveType = "group"
)

type MessageType string

const MessageTypeText MessageType = "text"

type MessageOrigin string

const (
	MessageOriginUser MessageOrigin = "user"
	MessageOriginAI   MessageOrigin = "ai"
)

// Message is the public v2/Kafka view of a saved message, not a send command.
type Message struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	// Direct: S+UUID. Group: G+UUID, preserving the existing storage meaning.
	SessionID   string        `json:"session_id"`
	SendID      string        `json:"send_id"`
	ReceiveID   string        `json:"receive_id"`
	ReceiveType ReceiveType   `json:"receive_type"`
	Type        MessageType   `json:"type"`
	Content     string        `json:"content"`
	Origin      MessageOrigin `json:"origin"`
	CreatedAt   time.Time     `json:"created_at"`
	// Positive decimal int64 encoded as a string to avoid JavaScript precision loss.
	Position string `json:"position"`
}

type DispatchState string

const (
	DispatchQueued    DispatchState = "queued"
	DispatchNotQueued DispatchState = "not_queued"
	DispatchUnknown   DispatchState = "unknown"
)

type SendResult struct {
	ClientMessageID string        `json:"client_message_id"`
	Stored          bool          `json:"stored"`
	Message         Message       `json:"message"`
	Dispatch        DispatchState `json:"dispatch"`
	Replayed        bool          `json:"replayed"`
}
