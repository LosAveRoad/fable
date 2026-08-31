package gormservice

import (
	"context"
	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"
	"mychat/internal/service/redisservice"
)

func GetContactUserList(userID string) ([]response.GetUserInfoResponse, error) {
	if userID == "" {
		return nil, ErrInvalidUUID
	}
	var cached []response.GetUserInfoResponse
	if err := redisservice.GetJSON(context.Background(), redisservice.ContactUserListKey(userID), &cached); err == nil {
		return cached, nil
	}
	var contacts []model.UserContact
	if err := dao.GormDB.Where("user_id = ? AND contact_type = ? AND status = ?", userID, model.ContactTypeUser, model.ContactStatusNormal).Find(&contacts).Error; err != nil {
		return nil, ErrDatabase
	}
	ids := make([]string, 0, len(contacts))
	for _, c := range contacts {
		ids = append(ids, c.ContactID)
	}
	var users []model.UserInfo
	if len(ids) > 0 {
		if err := dao.GormDB.Where("uuid IN ?", ids).Find(&users).Error; err != nil {
			return nil, ErrDatabase
		}
	}
	result := make([]response.GetUserInfoResponse, 0, len(users))
	for _, u := range users {
		result = append(result, response.GetUserInfoResponse{UUID: u.UUID, Nickname: u.Nickname, Telephone: u.Telephone})
	}
	_ = redisservice.SetJSON(context.Background(), redisservice.ContactUserListKey(userID), result, redisservice.DefaultCacheTTL)
	return result, nil
}
