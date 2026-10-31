package messagestore

import (
	"context"
	"strconv"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"mychat/internal/model"

	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

func (s *Store) Sync(ctx context.Context, user, conversation, cursor string, limit int) (im.SyncPage, error) {
	if !validID(user, 'U') || limit < 0 || limit > 500 {
		return im.SyncPage{}, ErrInvalid
	}
	sessionID, group, err := parseConversation(conversation)
	if err != nil {
		return im.SyncPage{}, err
	}
	after, err := decodeCursor(conversation, cursor)
	if err != nil {
		return im.SyncPage{}, err
	}
	if s == nil || s.db == nil {
		return im.SyncPage{}, ErrDatabase
	}
	if limit == 0 {
		limit = 100
	}
	result := im.SyncPage{ConversationID: conversation, Messages: make([]im.Message, 0), NextCursor: cursor}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockUser(tx, user); err != nil {
			return err
		}
		// Current locking reads bypass cache and stale RR snapshots; holding the
		// conversation lock also excludes writers until this page has been read.
		if group {
			if _, _, err := lockGroup(tx, user, sessionID); err != nil {
				return err
			}
		} else {
			if _, err := lockDirect(tx, user, "", sessionID); err != nil {
				return err
			}
		}
		var rows []message
		if err := tx.Where("session_id = ? AND id > ?", sessionID, after).Order("id ASC").Limit(limit + 1).Find(&rows).Error; err != nil {
			return ErrDatabase
		}
		result.HasMore = len(rows) > limit
		if result.HasMore {
			rows = rows[:limit]
		}
		for _, row := range rows {
			view, err := messageView(row)
			if err != nil {
				return err
			}
			result.Messages = append(result.Messages, publicMessage(view))
			result.NextCursor = encodeCursor(conversation, row.ID)
		}
		return nil
	})
	if err != nil {
		return im.SyncPage{}, storeError(err)
	}
	return result, nil
}

func messageView(row message) (*pb.MessageView, error) {
	if row.ID <= 0 || !validID(row.UUID, 'M') || !validID(row.SendID, 'U') || !validText(row.Content) || row.Type != model.MessageTypeText || row.CreatedAt.IsZero() {
		return nil, ErrDatabase
	}
	origin := pb.MessageOrigin_MESSAGE_ORIGIN_USER
	switch row.Origin {
	case model.MessageOriginUser:
	case model.MessageOriginAI:
		origin = pb.MessageOrigin_MESSAGE_ORIGIN_AI
	default:
		return nil, ErrDatabase
	}
	kind, conversation := pb.ReceiveType_RECEIVE_TYPE_USER, "direct:"+row.SessionID
	if validID(row.SessionID, 'G') {
		kind, conversation = pb.ReceiveType_RECEIVE_TYPE_GROUP, "group:"+row.SessionID
		if row.ReceiveID != row.SessionID {
			return nil, ErrDatabase
		}
	} else if !validID(row.SessionID, 'S') || !validID(row.ReceiveID, 'U') || row.SendID == row.ReceiveID {
		return nil, ErrDatabase
	}
	created := timestamppb.New(row.CreatedAt.UTC())
	if created.CheckValid() != nil {
		return nil, ErrDatabase
	}
	return &pb.MessageView{MessageId: row.UUID, ConversationId: conversation, SessionId: row.SessionID, SenderId: row.SendID, Destination: &pb.Destination{ReceiveType: kind, ReceiveId: row.ReceiveID}, MessageType: pb.MessageType_MESSAGE_TYPE_TEXT, Content: row.Content, Origin: origin, CreatedAt: created, Position: uint64(row.ID)}, nil
}
func publicMessage(view *pb.MessageView) im.Message {
	kind, origin := im.ReceiveTypeUser, im.MessageOriginUser
	if view.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP {
		kind = im.ReceiveTypeGroup
	}
	if view.Origin == pb.MessageOrigin_MESSAGE_ORIGIN_AI {
		origin = im.MessageOriginAI
	}
	return im.Message{MessageID: view.MessageId, ConversationID: view.ConversationId, SessionID: view.SessionId, SendID: view.SenderId, ReceiveID: view.Destination.ReceiveId, ReceiveType: kind, Type: im.MessageTypeText, Content: view.Content, Origin: origin, CreatedAt: view.CreatedAt.AsTime().UTC(), Position: strconv.FormatUint(view.Position, 10)}
}
