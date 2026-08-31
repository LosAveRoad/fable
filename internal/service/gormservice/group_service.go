package gormservice

import (
	"context"
	"encoding/json"
	"errors"

	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"
	"mychat/internal/service/redisservice"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var groupCacheContext = context.Background()

type GroupResponse struct {
	UUID        string   `json:"group_id"`
	Name        string   `json:"group_name"`
	OwnerID     string   `json:"owner_id"`
	Members     []string `json:"members"`
	Admins      []string `json:"admins"`
	MemberCount int      `json:"member_count"`
	AddMode     int8     `json:"add_mode"`
}

func decodeIDs(raw []byte) ([]string, error) {
	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, ErrDatabase
	}
	return ids, nil
}
func containsID(ids []string, id string) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
func groupResponse(g model.GroupInfo) (GroupResponse, error) {
	members, err := decodeIDs(g.Members)
	if err != nil {
		return GroupResponse{}, err
	}
	admins, err := decodeIDs(g.Admins)
	if err != nil {
		return GroupResponse{}, err
	}
	return GroupResponse{UUID: g.UUID, Name: g.Name, OwnerID: g.OwnerID, Members: members, Admins: admins, MemberCount: g.MemberCount, AddMode: g.AddMode}, nil
}

func CreateGroup(ownerID, name string, addModes ...int8) (GroupResponse, error) {
	if ownerID == "" || name == "" {
		return GroupResponse{}, ErrInvalidGroup
	}
	var count int64
	if err := dao.GormDB.Model(&model.UserInfo{}).Where("uuid = ?", ownerID).Count(&count).Error; err != nil {
		return GroupResponse{}, ErrDatabase
	}
	if count == 0 {
		return GroupResponse{}, ErrUserNotFound
	}
	addMode := int8(model.GroupAddModeDirect)
	if len(addModes) > 0 {
		addMode = addModes[0]
	}
	if addMode < model.GroupAddModeDirect || addMode > model.GroupAddModeForbidden {
		return GroupResponse{}, ErrInvalidGroup
	}
	members, _ := json.Marshal([]string{ownerID})
	admins, _ := json.Marshal([]string{})
	g := model.GroupInfo{UUID: "G" + uuid.NewString(), Name: name, OwnerID: ownerID, Members: members, Admins: admins, MemberCount: 1, AddMode: addMode, Status: model.GroupStatusNormal}
	if err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&g).Error; err != nil {
			return err
		}
		if err := tx.Create(&model.UserContact{UserID: ownerID, ContactID: g.UUID, ContactType: model.ContactTypeGroup, Status: model.ContactStatusNormal}).Error; err != nil {
			return err
		}
		return tx.Create(&model.Session{UUID: "S" + uuid.NewString(), SendId: ownerID, ReceiveId: g.UUID, Type: model.SessionTypeGroup}).Error
	}); err != nil {
		return GroupResponse{}, ErrDatabase
	}
	_ = redisservice.Delete(groupCacheContext, redisservice.OwnedGroupListKey(ownerID), redisservice.JoinedGroupListKey(ownerID), redisservice.GroupInfoKey(g.UUID), redisservice.GroupMemberListKey(g.UUID))
	return groupResponse(g)
}

func GetGroup(groupID string) (GroupResponse, error) {
	if groupID == "" {
		return GroupResponse{}, ErrInvalidGroup
	}
	var cached GroupResponse
	if err := redisservice.GetJSON(groupCacheContext, redisservice.GroupInfoKey(groupID), &cached); err == nil {
		return cached, nil
	}
	var g model.GroupInfo
	if err := dao.GormDB.Where("uuid = ?", groupID).First(&g).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return GroupResponse{}, ErrGroupNotFound
		}
		return GroupResponse{}, ErrDatabase
	}
	if g.Status != model.GroupStatusNormal {
		return GroupResponse{}, ErrGroupUnavailable
	}
	result, err := groupResponse(g)
	if err != nil {
		return GroupResponse{}, err
	}
	_ = redisservice.SetJSON(groupCacheContext, redisservice.GroupInfoKey(groupID), result, redisservice.DefaultCacheTTL)
	_ = redisservice.SetJSON(groupCacheContext, redisservice.GroupMemberListKey(groupID), result.Members, redisservice.DefaultCacheTTL)
	return result, nil
}

func GetJoinedGroupList(userID string) ([]GroupResponse, error) {
	if userID == "" {
		return nil, ErrInvalidUUID
	}
	var cached []GroupResponse
	if err := redisservice.GetJSON(groupCacheContext, redisservice.JoinedGroupListKey(userID), &cached); err == nil {
		return cached, nil
	}
	var groups []model.GroupInfo
	if err := dao.GormDB.Table("group_info").Joins("JOIN user_contact ON user_contact.contact_id = group_info.uuid AND user_contact.contact_type = ? AND user_contact.user_id = ? AND user_contact.status = ?", model.ContactTypeGroup, userID, model.ContactStatusNormal).Where("group_info.status = ?", model.GroupStatusNormal).Find(&groups).Error; err != nil {
		return nil, ErrDatabase
	}
	result := make([]GroupResponse, 0, len(groups))
	for _, g := range groups {
		r, err := groupResponse(g)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	_ = redisservice.SetJSON(groupCacheContext, redisservice.JoinedGroupListKey(userID), result, redisservice.DefaultCacheTTL)
	return result, nil
}

func GetOwnedGroupList(userID string) ([]GroupResponse, error) {
	if userID == "" {
		return nil, ErrInvalidUUID
	}
	var cached []GroupResponse
	if err := redisservice.GetJSON(groupCacheContext, redisservice.OwnedGroupListKey(userID), &cached); err == nil {
		return cached, nil
	}
	var groups []model.GroupInfo
	if err := dao.GormDB.Where("owner_id = ? AND status = ?", userID, model.GroupStatusNormal).Find(&groups).Error; err != nil {
		return nil, ErrDatabase
	}
	result := make([]GroupResponse, 0, len(groups))
	for _, g := range groups {
		r, err := groupResponse(g)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	_ = redisservice.SetJSON(groupCacheContext, redisservice.OwnedGroupListKey(userID), result, redisservice.DefaultCacheTTL)
	return result, nil
}

func JoinGroup(userID, groupID string) error {
	if userID == "" || groupID == "" {
		return ErrInvalidGroup
	}
	pendingRequest := false
	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		var userCount int64
		if err := tx.Model(&model.UserInfo{}).Where("uuid = ?", userID).Count(&userCount).Error; err != nil {
			return ErrDatabase
		}
		if userCount == 0 {
			return ErrUserNotFound
		}
		var g model.GroupInfo
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("uuid = ?", groupID).First(&g).Error; err != nil {
			return ErrGroupNotFound
		}
		if g.Status != model.GroupStatusNormal {
			return ErrGroupUnavailable
		}
		members, err := decodeIDs(g.Members)
		if err != nil {
			return err
		}
		if containsID(members, userID) {
			return nil
		}
		if g.AddMode == model.GroupAddModeForbidden {
			return ErrGroupJoinForbidden
		}
		if g.AddMode == model.GroupAddModeApprove {
			var pending int64
			if err := tx.Model(&model.ContactApply{}).Where("applicant_id = ? AND contact_id = ? AND contact_type = ? AND status = ?", userID, groupID, model.ContactTypeGroup, model.ContactApplyPending).Count(&pending).Error; err != nil {
				return ErrDatabase
			}
			if pending == 0 {
				if err := tx.Create(&model.ContactApply{ApplicantID: userID, ContactID: groupID, ContactType: model.ContactTypeGroup, Status: model.ContactApplyPending}).Error; err != nil {
					return ErrDatabase
				}
			}
			pendingRequest = true
			return nil
		}
		members = append(members, userID)
		raw, _ := json.Marshal(members)
		if err := tx.Model(&g).Updates(map[string]any{"members": raw, "member_count": len(members)}).Error; err != nil {
			return err
		}
		if err := tx.Create(&model.UserContact{UserID: userID, ContactID: groupID, ContactType: model.ContactTypeGroup, Status: model.ContactStatusNormal}).Error; err != nil {
			return err
		}
		return tx.Create(&model.Session{UUID: "S" + uuid.NewString(), SendId: userID, ReceiveId: groupID, Type: model.SessionTypeGroup}).Error
	})
	if err != nil {
		return err
	}
	if pendingRequest {
		return ErrGroupJoinPending
	}
	_ = redisservice.Delete(groupCacheContext, redisservice.GroupInfoKey(groupID), redisservice.GroupMemberListKey(groupID), redisservice.JoinedGroupListKey(userID))
	return nil
}

func LeaveGroup(userID, groupID string) error {
	if userID == "" || groupID == "" {
		return ErrInvalidGroup
	}
	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		var g model.GroupInfo
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("uuid = ?", groupID).First(&g).Error; err != nil {
			return ErrGroupNotFound
		}
		if g.Status != model.GroupStatusNormal {
			return ErrGroupUnavailable
		}
		if g.OwnerID == userID {
			return ErrGroupOwnerCannotLeave
		}
		members, err := decodeIDs(g.Members)
		if err != nil {
			return err
		}
		filtered := make([]string, 0, len(members))
		for _, id := range members {
			if id != userID {
				filtered = append(filtered, id)
			}
		}
		if len(filtered) == len(members) {
			return nil
		}
		raw, _ := json.Marshal(filtered)
		if err := tx.Model(&g).Updates(map[string]any{"members": raw, "member_count": len(filtered)}).Error; err != nil {
			return err
		}
		if err := tx.Unscoped().Where("user_id = ? AND contact_id = ? AND contact_type = ?", userID, groupID, model.ContactTypeGroup).Delete(&model.UserContact{}).Error; err != nil {
			return err
		}
		return tx.Unscoped().Where("send_id = ? AND receive_id = ? AND type = ?", userID, groupID, model.SessionTypeGroup).Delete(&model.Session{}).Error
	})
	if err == nil {
		_ = redisservice.Delete(groupCacheContext, redisservice.GroupInfoKey(groupID), redisservice.GroupMemberListKey(groupID), redisservice.JoinedGroupListKey(userID), redisservice.GroupSessionListKey(userID))
	}
	return err
}

// ApproveGroupJoin accepts a pending group application. Only the owner or an
// existing administrator may approve it.
func ApproveGroupJoin(operatorID, applicantID, groupID string) error {
	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		var g model.GroupInfo
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("uuid = ?", groupID).First(&g).Error; err != nil {
			return ErrGroupNotFound
		}
		admins, err := decodeIDs(g.Admins)
		if err != nil {
			return err
		}
		if operatorID != g.OwnerID && !containsID(admins, operatorID) {
			return ErrNotGroupAdmin
		}
		var apply model.ContactApply
		if err := tx.Where("applicant_id = ? AND contact_id = ? AND contact_type = ? AND status = ?", applicantID, groupID, model.ContactTypeGroup, model.ContactApplyPending).First(&apply).Error; err != nil {
			return ErrGroupJoinPending
		}
		members, err := decodeIDs(g.Members)
		if err != nil {
			return err
		}
		if !containsID(members, applicantID) {
			members = append(members, applicantID)
		}
		raw, _ := json.Marshal(members)
		if err := tx.Model(&g).Updates(map[string]any{"members": raw, "member_count": len(members)}).Error; err != nil {
			return err
		}
		if err := tx.Model(&apply).Update("status", model.ContactApplyAgreed).Error; err != nil {
			return err
		}
		if err := tx.Create(&model.UserContact{UserID: applicantID, ContactID: groupID, ContactType: model.ContactTypeGroup, Status: model.ContactStatusNormal}).Error; err != nil {
			return err
		}
		return tx.Create(&model.Session{UUID: "S" + uuid.NewString(), SendId: applicantID, ReceiveId: groupID, Type: model.SessionTypeGroup}).Error
	})
	if err == nil {
		_ = redisservice.Delete(groupCacheContext, redisservice.GroupInfoKey(groupID), redisservice.GroupMemberListKey(groupID), redisservice.JoinedGroupListKey(applicantID), redisservice.GroupSessionListKey(applicantID))
	}
	return err
}

func SendGroupMessage(senderID, groupID, content string) (response.MessageResponse, error) {
	if senderID == "" || groupID == "" {
		return response.MessageResponse{}, ErrInvalidGroup
	}
	if content == "" {
		return response.MessageResponse{}, ErrInvalidMessageContent
	}
	g, err := GetGroup(groupID)
	if err != nil {
		return response.MessageResponse{}, err
	}
	if !containsID(g.Members, senderID) {
		return response.MessageResponse{}, ErrNotGroupMember
	}
	m := model.Message{UUID: "M" + uuid.NewString(), SessionId: groupID, Type: model.MessageTypeText, Content: content, Origin: model.MessageOriginUser, SendId: senderID, ReceiveId: groupID}
	if err := dao.GormDB.Create(&m).Error; err != nil {
		return response.MessageResponse{}, ErrDatabase
	}
	result := response.MessageResponse{UUID: m.UUID, SessionID: m.SessionId, Type: m.Type, Content: m.Content, Origin: m.Origin, SendID: m.SendId, ReceiveID: m.ReceiveId, CreatedAt: m.CreatedAt}
	_ = redisservice.AppendJSON(groupCacheContext, redisservice.GroupMessageListKey(groupID), result)
	return result, nil
}

func GetGroupMessageList(userID, groupID string) ([]response.MessageResponse, error) {
	g, err := GetGroup(groupID)
	if err != nil {
		return nil, err
	}
	if !containsID(g.Members, userID) {
		return nil, ErrNotGroupMember
	}
	var cached []response.MessageResponse
	if err := redisservice.GetJSON(groupCacheContext, redisservice.GroupMessageListKey(groupID), &cached); err == nil {
		return cached, nil
	}
	result, err := getGroupMessageList(groupID)
	if err != nil {
		return nil, err
	}
	_ = redisservice.SetJSON(groupCacheContext, redisservice.GroupMessageListKey(groupID), result, redisservice.DefaultCacheTTL)
	return result, nil
}

func getGroupMessageList(groupID string) ([]response.MessageResponse, error) {
	if groupID == "" {
		return nil, ErrInvalidGroup
	}
	var ms []model.Message
	if err := dao.GormDB.Where("receive_id = ?", groupID).Order("created_at ASC, id ASC").Find(&ms).Error; err != nil {
		return nil, ErrDatabase
	}
	out := make([]response.MessageResponse, 0, len(ms))
	for _, m := range ms {
		out = append(out, response.MessageResponse{UUID: m.UUID, SessionID: m.SessionId, Type: m.Type, Content: m.Content, Origin: m.Origin, SendID: m.SendId, ReceiveID: m.ReceiveId, CreatedAt: m.CreatedAt})
	}
	return out, nil
}
