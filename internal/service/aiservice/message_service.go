package aiservice

import (
	"strings"
	"time"

	"mychat/internal/dao"
	"mychat/internal/model"
	"mychat/internal/service/gormservice"

	"gorm.io/gorm"
)

const (
	DefaultRecentMessageLimit = 50
	MaxRecentMessageLimit     = 100
	DefaultSearchMessageLimit = 30
	MaxSearchMessageLimit     = 50
	MaxSearchQueryLength      = 200
	MaxAIMessageContentLength = 4000
)

type AIMessage struct {
	UUID         string
	SessionUUID  string
	SenderUUID   string
	SenderName   string
	ReceiverUUID string
	Type         int8
	Content      string
	Origin       int8
	CreatedAt    time.Time
}

type MessagePage struct {
	Messages []AIMessage
	HasMore  bool
}

func SendMessage(userUUID, sessionUUID, content string) (AIMessage, error) {
	userUUID = strings.TrimSpace(userUUID)
	sessionUUID = strings.TrimSpace(sessionUUID)
	content = strings.TrimSpace(content)
	if userUUID == "" || sessionUUID == "" || content == "" || len([]rune(content)) > MaxAIMessageContentLength {
		return AIMessage{}, ErrInvalidToolInput
	}

	var session model.Session
	err := dao.GormDB.Model(&model.Session{}).
		Joins("JOIN user_ai_session_access AS access ON access.session_uuid = session.uuid AND access.user_uuid = ?", userUUID).
		Where("session.uuid = ? AND (session.send_id = ? OR session.receive_id = ?)", sessionUUID, userUUID, userUUID).
		First(&session).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return AIMessage{}, ErrForbidden
		}
		return AIMessage{}, ErrDatabase
	}

	peerUUID := session.SendId
	if peerUUID == userUUID {
		peerUUID = session.ReceiveId
	}

	var sender model.UserInfo
	if err := dao.GormDB.Select("uuid", "nickname").Where("uuid = ?", userUUID).First(&sender).Error; err != nil {
		return AIMessage{}, ErrDatabase
	}

	created, err := gormservice.SendAIMessage(userUUID, peerUUID, content)
	if err != nil {
		return AIMessage{}, ErrDatabase
	}

	return AIMessage{
		UUID:         created.UUID,
		SessionUUID:  created.SessionID,
		SenderUUID:   created.SendID,
		SenderName:   sender.Nickname,
		ReceiverUUID: created.ReceiveID,
		Type:         created.Type,
		Content:      created.Content,
		Origin:       created.Origin,
		CreatedAt:    created.CreatedAt,
	}, nil
}

func GetRecentMessages(userUUID, sessionUUID, beforeMessageUUID string, limit int) (MessagePage, error) {
	if err := ensureSessionAccess(userUUID, sessionUUID); err != nil {
		return MessagePage{}, err
	}
	limit, err := normalizeLimit(limit, DefaultRecentMessageLimit, MaxRecentMessageLimit)
	if err != nil {
		return MessagePage{}, err
	}

	query := dao.GormDB.Where("session_id = ?", sessionUUID)
	if beforeMessageUUID != "" {
		var boundary model.Message
		err := dao.GormDB.Where("uuid = ? AND session_id = ?", beforeMessageUUID, sessionUUID).
			First(&boundary).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return MessagePage{}, ErrMessageNotFound
			}
			return MessagePage{}, ErrDatabase
		}
		query = query.Where("created_at < ? OR (created_at = ? AND id < ?)", boundary.CreatedAt, boundary.CreatedAt, boundary.ID)
	}

	var messages []model.Message
	if err := query.Order("created_at DESC, id DESC").Limit(limit + 1).Find(&messages).Error; err != nil {
		return MessagePage{}, ErrDatabase
	}
	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	reverseMessages(messages)

	result, err := mapMessages(messages)
	if err != nil {
		return MessagePage{}, err
	}
	return MessagePage{Messages: result, HasMore: hasMore}, nil
}

func SearchMessages(userUUID, queryText, sessionUUID string, limit int) ([]AIMessage, error) {
	queryText = strings.TrimSpace(queryText)
	if userUUID == "" || queryText == "" || len([]rune(queryText)) > MaxSearchQueryLength {
		return nil, ErrInvalidToolInput
	}
	limit, err := normalizeLimit(limit, DefaultSearchMessageLimit, MaxSearchMessageLimit)
	if err != nil {
		return nil, err
	}

	query := dao.GormDB.Model(&model.Message{}).
		Where("content LIKE ?", "%"+escapeLike(queryText)+"%")
	if sessionUUID != "" {
		if err := ensureSessionAccess(userUUID, sessionUUID); err != nil {
			return nil, err
		}
		query = query.Where("session_id = ?", sessionUUID)
	} else {
		allowedSessions := dao.GormDB.Table("session AS s").
			Select("s.uuid").
			Joins("JOIN user_ai_session_access AS access ON access.session_uuid = s.uuid AND access.user_uuid = ?", userUUID).
			Where("s.send_id = ? OR s.receive_id = ?", userUUID, userUUID)
		query = query.Where("session_id IN (?)", allowedSessions)
	}

	var messages []model.Message
	if err := query.Order("created_at DESC, id DESC").Limit(limit).Find(&messages).Error; err != nil {
		return nil, ErrDatabase
	}
	return mapMessages(messages)
}

func mapMessages(messages []model.Message) ([]AIMessage, error) {
	result := make([]AIMessage, 0, len(messages))
	if len(messages) == 0 {
		return result, nil
	}

	senderUUIDs := make([]string, 0, len(messages))
	seen := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		if _, ok := seen[message.SendId]; !ok {
			seen[message.SendId] = struct{}{}
			senderUUIDs = append(senderUUIDs, message.SendId)
		}
	}
	var senders []model.UserInfo
	if err := dao.GormDB.Where("uuid IN ?", senderUUIDs).Find(&senders).Error; err != nil {
		return nil, ErrDatabase
	}
	names := make(map[string]string, len(senders))
	for _, sender := range senders {
		names[sender.UUID] = sender.Nickname
	}

	for _, message := range messages {
		result = append(result, AIMessage{
			UUID:         message.UUID,
			SessionUUID:  message.SessionId,
			SenderUUID:   message.SendId,
			SenderName:   names[message.SendId],
			ReceiverUUID: message.ReceiveId,
			Type:         message.Type,
			Content:      message.Content,
			Origin:       message.Origin,
			CreatedAt:    message.CreatedAt,
		})
	}
	return result, nil
}

func reverseMessages(messages []model.Message) {
	for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
		messages[left], messages[right] = messages[right], messages[left]
	}
}

func escapeLike(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(value)
}
