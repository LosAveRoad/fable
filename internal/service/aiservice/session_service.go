package aiservice

import (
	"strings"
	"time"

	"mychat/internal/dao"
	"mychat/internal/model"
)

const (
	DefaultSessionLimit = 50
	MaxSessionLimit     = 100
)

type AllowedSession struct {
	UUID      string
	PeerUUID  string
	PeerName  string
	CreatedAt time.Time
}

func ListAllowedSessions(userUUID string, limit int) ([]AllowedSession, error) {
	if strings.TrimSpace(userUUID) == "" {
		return nil, ErrInvalidToolInput
	}
	limit, err := normalizeLimit(limit, DefaultSessionLimit, MaxSessionLimit)
	if err != nil {
		return nil, err
	}

	var sessions []AllowedSession
	err = dao.GormDB.Table("session AS s").
		Select(`s.uuid, CASE WHEN s.send_id = ? THEN s.receive_id ELSE s.send_id END AS peer_uuid, peer.nickname AS peer_name, s.created_at`, userUUID).
		Joins("JOIN user_ai_session_access AS access ON access.session_uuid = s.uuid AND access.user_uuid = ?", userUUID).
		Joins("JOIN user_info AS peer ON peer.uuid = CASE WHEN s.send_id = ? THEN s.receive_id ELSE s.send_id END", userUUID).
		Where("s.send_id = ? OR s.receive_id = ?", userUUID, userUUID).
		Order("s.created_at DESC, s.id DESC").
		Limit(limit).
		Scan(&sessions).Error
	if err != nil {
		return nil, ErrDatabase
	}
	return sessions, nil
}

func ensureSessionAccess(userUUID, sessionUUID string) error {
	if strings.TrimSpace(userUUID) == "" || strings.TrimSpace(sessionUUID) == "" {
		return ErrInvalidToolInput
	}
	var count int64
	err := dao.GormDB.Model(&model.Session{}).
		Joins("JOIN user_ai_session_access AS access ON access.session_uuid = session.uuid AND access.user_uuid = ?", userUUID).
		Where("session.uuid = ? AND (session.send_id = ? OR session.receive_id = ?)", sessionUUID, userUUID, userUUID).
		Count(&count).Error
	if err != nil {
		return ErrDatabase
	}
	if count != 1 {
		return ErrForbidden
	}
	return nil
}

func normalizeLimit(value, defaultValue, maximum int) (int, error) {
	if value == 0 {
		return defaultValue, nil
	}
	if value < 1 || value > maximum {
		return 0, ErrInvalidToolInput
	}
	return value, nil
}
