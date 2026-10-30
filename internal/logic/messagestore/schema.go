package messagestore

import (
	"context"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"mychat/internal/model"
)

// message is private so the legacy model and legacy handlers remain unchanged.
// InitSchema is for a new schema only; there is deliberately no old-row backfill.
type message struct {
	ID                 int64          `gorm:"column:id;primaryKey;autoIncrement"`
	UUID               string         `gorm:"column:uuid;uniqueIndex;type:char(37);not null"`
	SessionID          string         `gorm:"column:session_id;index:idx_message_session_position,priority:1;type:char(37);not null"`
	Type               int8           `gorm:"column:type;not null"`
	Content            string         `gorm:"column:content;type:text;not null"`
	Origin             int8           `gorm:"column:origin;not null;default:0"`
	SendID             string         `gorm:"column:send_id;index;uniqueIndex:uk_message_sender_client,priority:1;type:char(37);not null"`
	ReceiveID          string         `gorm:"column:receive_id;index;type:char(37);not null"`
	CreatedAt          time.Time      `gorm:"column:created_at;index;not null;precision:6"`
	ClientMessageID    string         `gorm:"column:client_message_id;uniqueIndex:uk_message_sender_client,priority:2;type:char(36);not null"`
	RequestFingerprint []byte         `gorm:"column:request_fingerprint;type:binary(32);not null"`
	RecipientIDs       datatypes.JSON `gorm:"column:recipient_ids;type:json;not null"`
}

func (message) TableName() string { return "message" }
func InitSchema(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return ErrDatabase
	}
	if err := db.WithContext(ctx).AutoMigrate(&model.UserInfo{}, &model.Session{}, &model.GroupInfo{}, &model.UserAISessionAccess{}, &model.UserContact{}, &model.ContactApply{}, &message{}); err != nil {
		return ErrDatabase
	}
	return nil
}
