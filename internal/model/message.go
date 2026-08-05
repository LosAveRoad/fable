package model

import "time"

type Message struct {
	ID   int64  `gorm:"column:id;primaryKey"`
	UUID string `gorm:"column:uuid;uniqueIndex;type:char(37);not null"`

	SessionId string `gorm:"column:session_id;index;type:char(37);not null"`
	Type      int8   `gorm:"column:type;not null"`
	Content   string `gorm:"column:content;type:text;not null"`

	SendId    string `gorm:"column:send_id;index;type:char(37);not null"`
	ReceiveId string `gorm:"column:receive_id;index;type:char(37);not null"`

	CreatedAt time.Time `gorm:"column:created_at;index;not null"`
}

func (Message) TableName() string {
	return "message"
}
