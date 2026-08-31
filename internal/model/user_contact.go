package model

import (
	"gorm.io/gorm"
	"time"
)

const (
	ContactTypeUser int8 = iota
	ContactTypeGroup
)

const (
	ContactStatusNormal int8 = iota
	ContactStatusBlack
	ContactStatusPending
)

type UserContact struct {
	ID          int64  `gorm:"primaryKey"`
	UserID      string `gorm:"column:user_id;index;uniqueIndex:uk_user_contact;type:char(37);not null"`
	ContactID   string `gorm:"column:contact_id;index;uniqueIndex:uk_user_contact;type:char(37);not null"`
	ContactType int8   `gorm:"column:contact_type;uniqueIndex:uk_user_contact;not null"`
	Status      int8   `gorm:"column:status;not null;default:0"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}

func (UserContact) TableName() string { return "user_contact" }
