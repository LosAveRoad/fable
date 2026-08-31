package gormservice

import (
	"context"
	"errors"

	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"
	"mychat/internal/service/redisservice"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func SendMessage(senderUUID string, receiverUUID string, content string) (response.MessageResponse, error) {
	return sendMessage(senderUUID, receiverUUID, content, model.MessageOriginUser)
}

func SendAIMessage(senderUUID string, receiverUUID string, content string) (response.MessageResponse, error) {
	return sendMessage(senderUUID, receiverUUID, content, model.MessageOriginAI)
}

func sendMessage(senderUUID string, receiverUUID string, content string, origin int8) (response.MessageResponse, error) {
	if senderUUID == "" || receiverUUID == "" || senderUUID == receiverUUID {
		return response.MessageResponse{}, ErrInvalidUserPair
	}
	if content == "" {
		return response.MessageResponse{}, ErrInvalidMessageContent
	}

	var session model.Session
	err := dao.GormDB.Where(
		"(send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?)",
		senderUUID, receiverUUID, receiverUUID, senderUUID,
	).First(&session).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return response.MessageResponse{}, ErrInvalidSession
		}
		return response.MessageResponse{}, ErrDatabase
	}

	message := model.Message{
		UUID:      "M" + uuid.NewString(),
		SessionId: session.UUID,
		Type:      model.MessageTypeText,
		Content:   content,
		Origin:    origin,
		SendId:    senderUUID,
		ReceiveId: receiverUUID,
	}
	if err := dao.GormDB.Create(&message).Error; err != nil {
		return response.MessageResponse{}, ErrDatabase
	}
	result := response.MessageResponse{
		UUID: message.UUID, SessionID: message.SessionId, Type: message.Type,
		Content: message.Content, Origin: message.Origin, SendID: message.SendId,
		ReceiveID: message.ReceiveId, CreatedAt: message.CreatedAt,
	}
	_ = redisservice.AppendJSON(context.Background(), redisservice.MessageListKey(senderUUID, receiverUUID), result)
	_ = redisservice.Delete(context.Background(), redisservice.SessionListKey(senderUUID), redisservice.SessionListKey(receiverUUID))
	return result, nil
}

func GetMessageList(userOneID string, userTwoID string) ([]response.MessageResponse, error) {
	if userOneID == "" || userTwoID == "" || userOneID == userTwoID {
		return nil, ErrInvalidUUID
	}

	var cached []response.MessageResponse
	if err := redisservice.GetJSON(context.Background(), redisservice.MessageListKey(userOneID, userTwoID), &cached); err == nil {
		return cached, nil
	}
	var messages []model.Message
	if err := dao.GormDB.Where(
		"(send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?)",
		userOneID, userTwoID, userTwoID, userOneID,
	).Order("created_at ASC, id ASC").Find(&messages).Error; err != nil {
		return nil, ErrDatabase
	}

	result := make([]response.MessageResponse, 0, len(messages))
	for _, message := range messages {
		result = append(result, response.MessageResponse{
			UUID:      message.UUID,
			SessionID: message.SessionId,
			Type:      message.Type,
			Content:   message.Content,
			Origin:    message.Origin,
			SendID:    message.SendId,
			ReceiveID: message.ReceiveId,
			CreatedAt: message.CreatedAt,
		})
	}

	_ = redisservice.SetJSON(context.Background(), redisservice.MessageListKey(userOneID, userTwoID), result, redisservice.DefaultCacheTTL)
	return result, nil
}
