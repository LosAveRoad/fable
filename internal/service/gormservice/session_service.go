package gormservice

import (
	"context"
	"sort"

	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"
	"mychat/internal/service/redisservice"

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
	_ = redisservice.Delete(context.Background(), redisservice.SessionPairKey(sendUUID, peerUUID), redisservice.SessionListKey(sendUUID), redisservice.SessionListKey(peerUUID))

	return response.OpenSessionResponse{SessionUUID: session.UUID}, nil
}

func GetUserSessionList(userUUID string) ([]response.UserSessionListResponse, error) {
	if userUUID == "" {
		return nil, ErrInvalidUUID
	}

	var cached []response.UserSessionListResponse
	if err := redisservice.GetJSON(context.Background(), redisservice.SessionListKey(userUUID), &cached); err == nil {
		return cached, nil
	}
	var sessions []model.Session
	if err := dao.GormDB.Where(
		"send_id = ? OR receive_id = ?", userUUID, userUUID,
	).Order("created_at ASC, id ASC").Find(&sessions).Error; err != nil {
		return nil, ErrDatabase
	}

	result := make([]response.UserSessionListResponse, 0, len(sessions))
	for _, session := range sessions {
		if session.Type != model.SessionTypeUser {
			continue
		}
		peerUUID := session.SendId
		if peerUUID == userUUID {
			peerUUID = session.ReceiveId
		}
		result = append(result, response.UserSessionListResponse{
			SessionUUID: session.UUID,
			PeerUUID:    peerUUID,
		})
	}

	_ = redisservice.SetJSON(context.Background(), redisservice.SessionListKey(userUUID), result, redisservice.DefaultCacheTTL)
	return result, nil
}

func GetGroupSessionList(userUUID string) ([]response.UserSessionListResponse, error) {
	if userUUID == "" {
		return nil, ErrInvalidUUID
	}
	var cached []response.UserSessionListResponse
	if err := redisservice.GetJSON(context.Background(), redisservice.GroupSessionListKey(userUUID), &cached); err == nil {
		return cached, nil
	}
	var sessions []model.Session
	if err := dao.GormDB.Where("send_id = ? AND type = ?", userUUID, model.SessionTypeGroup).Order("created_at ASC, id ASC").Find(&sessions).Error; err != nil {
		return nil, ErrDatabase
	}
	result := make([]response.UserSessionListResponse, 0, len(sessions))
	for _, s := range sessions {
		result = append(result, response.UserSessionListResponse{SessionUUID: s.UUID, PeerUUID: s.ReceiveId})
	}
	_ = redisservice.SetJSON(context.Background(), redisservice.GroupSessionListKey(userUUID), result, redisservice.DefaultCacheTTL)
	return result, nil
}

func GetSessionUsers(sessionUUID string) (string, string, error) {
	var session model.Session
	if err := dao.GormDB.Model(model.Session{}).Where("uuid = ?", sessionUUID).First(&session).Error; err != nil {
		return "", "", err
	}
	return session.SendId, session.ReceiveId, nil
}
