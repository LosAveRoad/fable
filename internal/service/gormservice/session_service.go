package gormservice

import (
	"sort"

	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func OpenSession(sendUUID string, peerUUID string) (response.OpenSessionResponse, error) {
	if sendUUID == "" || peerUUID == "" || sendUUID == peerUUID {
		return response.OpenSessionResponse{}, ErrInvalidSession
	}

	var userCount int64
	if err := dao.GormDB.Model(&model.UserInfo{}).
		Where("uuid = ?", peerUUID).
		Count(&userCount).Error; err != nil {
		return response.OpenSessionResponse{}, ErrDatabase
	}
	if userCount == 0 {
		return response.OpenSessionResponse{}, ErrUserNotFound
	}

	users := []string{sendUUID, peerUUID}
	sort.Strings(users)

	var session model.Session
	err := dao.GormDB.Where(
		"send_id = ? AND receive_id = ?",
		users[0], users[1],
	).First(&session).Error
	if err == nil {
		return response.OpenSessionResponse{SessionUUID: session.UUID}, nil
	}
	if err != gorm.ErrRecordNotFound {
		return response.OpenSessionResponse{}, ErrDatabase
	}

	session = model.Session{
		UUID:      "S" + uuid.NewString(),
		SendId:    users[0],
		ReceiveId: users[1],
	}
	if err := dao.GormDB.Create(&session).Error; err != nil {
		return response.OpenSessionResponse{}, ErrSessionCreateFail
	}

	return response.OpenSessionResponse{SessionUUID: session.UUID}, nil
}

func GetUserSessionList(userUUID string) ([]response.UserSessionListResponse, error) {
	if userUUID == "" {
		return nil, ErrInvalidUUID
	}

	var sessions []model.Session
	if err := dao.GormDB.Where(
		"send_id = ? OR receive_id = ?", userUUID, userUUID,
	).Order("created_at ASC, id ASC").Find(&sessions).Error; err != nil {
		return nil, ErrDatabase
	}

	result := make([]response.UserSessionListResponse, 0, len(sessions))
	for _, session := range sessions {
		peerUUID := session.SendId
		if peerUUID == userUUID {
			peerUUID = session.ReceiveId
		}
		result = append(result, response.UserSessionListResponse{
			SessionUUID: session.UUID,
			PeerUUID:    peerUUID,
		})
	}

	return result, nil
}

func GetSessionUsers(sessionUUID string) (string, string, error) {
	var session model.Session
	if err := dao.GormDB.Model(model.Session{}).Where("uuid = ?", sessionUUID).First(&session).Error; err != nil {
		return "", "", err
	}
	return session.SendId, session.ReceiveId, nil
}
