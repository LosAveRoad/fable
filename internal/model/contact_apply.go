package model

import "time"

const (
	ContactApplyPending int8 = iota
	ContactApplyAgreed
	ContactApplyRefused
)

type ContactApply struct {
	ID          int64  `gorm:"primaryKey"`
	ApplicantID string `gorm:"column:applicant_id;index;type:char(37);not null"`
	ContactID   string `gorm:"column:contact_id;index;type:char(37);not null"`
	ContactType int8   `gorm:"column:contact_type;not null"`
	Status      int8   `gorm:"column:status;not null;default:0"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (ContactApply) TableName() string { return "contact_apply" }
