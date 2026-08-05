package model

import "time"

type UserInfo struct {
	ID           int64  `gorm:"primaryKey"`
	UUID         string `gorm:"uniqueIndex"`
	Nickname     string
	Telephone    string `gorm:"uniqueIndex"`
	PasswordHash string
	CreatedAt    time.Time
}

func (UserInfo) TableName() string {
	return "user_info"
}
