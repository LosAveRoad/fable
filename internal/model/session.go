package model

import "time"

type Session struct {
	ID   int64  `gorm:"column:id;primaryKey"`
	UUID string `gorm:"column:uuid;uniqueIndex;type:char(37);not null"`

	SendId    string `gorm:"column:send_id;index;uniqueIndex:uk_session_users;type:char(37);not null"`
	ReceiveId string `gorm:"column:receive_id;index;uniqueIndex:uk_session_users;type:char(37);not null"`
	// Type is kept backward-compatible with the existing direct-session rows.
	// Group rows are user-owned: SendId is the owner and ReceiveId is Gxxx.
	Type int8 `gorm:"column:type;index;not null;default:0"`

	CreatedAt time.Time `gorm:"column:created_at;index;not null"`
}

const (
	SessionTypeUser int8 = iota
	SessionTypeGroup
)

func (Session) TableName() string {
	return "session"
}
