package gormservice

import (
	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"
)

func GetMessageList(userOneID string, userTwoID string) ([]response.MessageResponse, error) {
	if userOneID == "" || userTwoID == "" || userOneID == userTwoID {
		return nil, ErrInvalidUUID
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
			SendID:    message.SendId,
			ReceiveID: message.ReceiveId,
			CreatedAt: message.CreatedAt,
		})
	}

	return result, nil
}
