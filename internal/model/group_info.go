package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	GroupAddModeDirect int8 = iota
	GroupAddModeApprove
	GroupAddModeForbidden
)

const (
	GroupStatusNormal int8 = iota
	GroupStatusDisabled
	GroupStatusDismissed
)

// GroupInfo follows KamaChat's first version: members and admins are JSON
// arrays. The service layer updates both in a transaction.
type GroupInfo struct {
	ID          int64          `gorm:"primaryKey"`
	UUID        string         `gorm:"column:uuid;uniqueIndex;type:char(37);not null"`
	Name        string         `gorm:"column:name;type:varchar(100);not null"`
	Avatar      string         `gorm:"column:avatar;type:varchar(255)"`
	Notice      string         `gorm:"column:notice;type:text"`
	OwnerID     string         `gorm:"column:owner_id;index;type:char(37);not null"`
	Members     datatypes.JSON `gorm:"column:members;type:json;not null"`
	Admins      datatypes.JSON `gorm:"column:admins;type:json;not null"`
	MemberCount int            `gorm:"column:member_count;not null;default:1"`
	AddMode     int8           `gorm:"column:add_mode;not null;default:0"`
	Status      int8           `gorm:"column:status;not null;default:0"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}

func (GroupInfo) TableName() string { return "group_info" }
