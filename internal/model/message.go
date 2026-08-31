package model

import "time"

const MessageTypeText int8 = 0

const (
	MessageOriginUser int8 = iota
	MessageOriginAI
)

type Message struct {
	ID   int64  `gorm:"column:id;primaryKey"`
	UUID string `gorm:"column:uuid;uniqueIndex;type:char(37);not null"`

	SessionId string `gorm:"column:session_id;index;type:char(37);not null"`
	Type      int8   `gorm:"column:type;not null"`
	Content   string `gorm:"column:content;type:text;not null"`
	Origin    int8   `gorm:"column:origin;not null;default:0"`

	SendId    string `gorm:"column:send_id;index;type:char(37);not null"`
	ReceiveId string `gorm:"column:receive_id;index;type:char(37);not null"`

	CreatedAt time.Time `gorm:"column:created_at;index;not null"`
}

// ReceiveID is a user UUID for direct messages and a group UUID (G...) for
// group messages. Keeping one message table makes history and delivery share
// the same persistence path.

func (Message) TableName() string {
	return "message"
}
