package model

import "time"

type Session struct {
	ID   int64  `gorm:"column:id;primaryKey"`
	UUID string `gorm:"column:uuid;uniqueIndex;type:char(37);not null"`

	SendId    string `gorm:"column:send_id;index;uniqueIndex:uk_session_users;type:char(37);not null"`
	ReceiveId string `gorm:"column:receive_id;index;uniqueIndex:uk_session_users;type:char(37);not null"`

	CreatedAt time.Time `gorm:"column:created_at;index;not null"`
}

func (Session) TableName() string {
	return "session"
}
