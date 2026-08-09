package model

import "time"

// UserAISessionAccess is a sparse allow-list of sessions a user permits AI to access.
// Absence of a row means access is not allowed.
type UserAISessionAccess struct {
	ID int64 `gorm:"column:id;primaryKey"`

	UserUUID    string `gorm:"column:user_uuid;index;uniqueIndex:uk_user_ai_session;type:char(37);not null"`
	SessionUUID string `gorm:"column:session_uuid;index;uniqueIndex:uk_user_ai_session;type:char(37);not null"`

	CreatedAt time.Time `gorm:"column:created_at;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null"`
}

func (UserAISessionAccess) TableName() string {
	return "user_ai_session_access"
}
