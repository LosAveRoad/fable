package model

import "time"

type UserInfo struct {
	ID           int64  `gorm:"primaryKey"`
	UUID         string `gorm:"type:char(37);uniqueIndex;not null"`
	Nickname     string `gorm:"type:varchar(64);not null"`
	Telephone    string `gorm:"type:varchar(32);uniqueIndex;not null"`
	PasswordHash string `gorm:"type:varchar(255);not null"`
	CreatedAt    time.Time
}

func (UserInfo) TableName() string {
	return "user_info"
}
