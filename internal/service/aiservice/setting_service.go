package aiservice

import (
	"sort"
	"strings"

	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/model"

	"gorm.io/gorm"
)

const MaxAllowedSessions = 200

func GetAISetting(userUUID string) (response.AISettingResponse, error) {
	if strings.TrimSpace(userUUID) == "" {
		return response.AISettingResponse{}, ErrInvalidSetting
	}
	return getAISetting(dao.GormDB, userUUID)
}

func ChangeAISetting(userUUID string, allowedSessionUUIDs []string) (response.AISettingResponse, error) {
	if strings.TrimSpace(userUUID) == "" {
		return response.AISettingResponse{}, ErrInvalidSetting
	}

	uuids, err := normalizeSessionUUIDs(allowedSessionUUIDs)
	if err != nil {
		return response.AISettingResponse{}, err
	}

	err = dao.GormDB.Transaction(func(tx *gorm.DB) error {
		if len(uuids) > 0 {
			var count int64
			if err := tx.Model(&model.Session{}).
				Where("uuid IN ? AND (send_id = ? OR receive_id = ?)", uuids, userUUID, userUUID).
				Count(&count).Error; err != nil {
				return ErrDatabase
			}
			if count != int64(len(uuids)) {
				return ErrForbidden
			}
		}

		query := tx.Where("user_uuid = ?", userUUID)
		if len(uuids) > 0 {
			query = query.Where("session_uuid NOT IN ?", uuids)
		}
		if err := query.Delete(&model.UserAISessionAccess{}).Error; err != nil {
			return ErrDatabase
		}

		if len(uuids) == 0 {
			return nil
		}

		var existing []model.UserAISessionAccess
		if err := tx.Where("user_uuid = ? AND session_uuid IN ?", userUUID, uuids).
			Find(&existing).Error; err != nil {
			return ErrDatabase
		}
		existingIDs := make(map[string]struct{}, len(existing))
		for _, access := range existing {
			existingIDs[access.SessionUUID] = struct{}{}
		}

		toCreate := make([]model.UserAISessionAccess, 0, len(uuids)-len(existing))
		for _, sessionUUID := range uuids {
			if _, ok := existingIDs[sessionUUID]; !ok {
				toCreate = append(toCreate, model.UserAISessionAccess{
					UserUUID:    userUUID,
					SessionUUID: sessionUUID,
				})
			}
		}
		if len(toCreate) > 0 && tx.Create(&toCreate).Error != nil {
			return ErrDatabase
		}
		return nil
	})
	if err != nil {
		return response.AISettingResponse{}, err
	}

	return getAISetting(dao.GormDB, userUUID)
}

// ListAllowedSessionUUIDs is shared by MCP tools so authorization stays in one service.
func ListAllowedSessionUUIDs(userUUID string) ([]string, error) {
	if strings.TrimSpace(userUUID) == "" {
		return nil, ErrInvalidSetting
	}
	var accesses []model.UserAISessionAccess
	if err := dao.GormDB.Where("user_uuid = ?", userUUID).
		Order("session_uuid ASC").Find(&accesses).Error; err != nil {
		return nil, ErrDatabase
	}
	ids := make([]string, 0, len(accesses))
	for _, access := range accesses {
		ids = append(ids, access.SessionUUID)
	}
	return ids, nil
}

func getAISetting(db *gorm.DB, userUUID string) (response.AISettingResponse, error) {
	var sessions []model.Session
	if err := db.Where("send_id = ? OR receive_id = ?", userUUID, userUUID).
		Order("created_at ASC, id ASC").Find(&sessions).Error; err != nil {
		return response.AISettingResponse{}, ErrDatabase
	}

	result := response.AISettingResponse{
		Sessions: make([]response.AISettingSessionResponse, 0, len(sessions)),
	}
	if len(sessions) == 0 {
		return result, nil
	}

	sessionUUIDs := make([]string, 0, len(sessions))
	peerIDs := make([]string, 0, len(sessions))
	for _, session := range sessions {
		sessionUUIDs = append(sessionUUIDs, session.UUID)
		peerID := session.SendId
		if peerID == userUUID {
			peerID = session.ReceiveId
		}
		peerIDs = append(peerIDs, peerID)
	}

	var accesses []model.UserAISessionAccess
	if err := db.Where("user_uuid = ? AND session_uuid IN ?", userUUID, sessionUUIDs).
		Find(&accesses).Error; err != nil {
		return response.AISettingResponse{}, ErrDatabase
	}
	allowed := make(map[string]struct{}, len(accesses))
	for _, access := range accesses {
		allowed[access.SessionUUID] = struct{}{}
	}

	var peers []model.UserInfo
	if err := db.Where("uuid IN ?", peerIDs).Find(&peers).Error; err != nil {
		return response.AISettingResponse{}, ErrDatabase
	}
	peerNames := make(map[string]string, len(peers))
	for _, peer := range peers {
		peerNames[peer.UUID] = peer.Nickname
	}

	for i, session := range sessions {
		_, isAllowed := allowed[session.UUID]
		result.Sessions = append(result.Sessions, response.AISettingSessionResponse{
			SessionUUID: session.UUID,
			Peer: response.AISettingPeerResponse{
				UUID: peerIDs[i],
				Name: peerNames[peerIDs[i]],
			},
			AIAccessAllowed: isAllowed,
		})
	}
	return result, nil
}

func normalizeSessionUUIDs(uuids []string) ([]string, error) {
	if len(uuids) > MaxAllowedSessions {
		return nil, ErrInvalidSetting
	}
	unique := make(map[string]struct{}, len(uuids))
	for _, sessionUUID := range uuids {
		sessionUUID = strings.TrimSpace(sessionUUID)
		if sessionUUID == "" {
			return nil, ErrInvalidSetting
		}
		unique[sessionUUID] = struct{}{}
	}
	if len(unique) > MaxAllowedSessions {
		return nil, ErrInvalidSetting
	}
	result := make([]string, 0, len(unique))
	for sessionUUID := range unique {
		result = append(result, sessionUUID)
	}
	sort.Strings(result)
	return result, nil
}
