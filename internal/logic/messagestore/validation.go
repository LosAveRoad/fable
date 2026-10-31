package messagestore

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	pb "mychat/api/gen/fable/v1"
)

func validUUID(s string) bool {
	return len(s) == 36 && s != uuid.Nil.String() && uuid.Validate(s) == nil
}
func validID(s string, prefix byte) bool { return len(s) == 37 && s[0] == prefix && validUUID(s[1:]) }
func validateInput(in Input) error {
	if !validID(in.SenderID, 'U') || !validUUID(in.ClientMessageID) || in.Destination == nil || !validText(in.Content) {
		return ErrInvalid
	}
	switch in.Destination.ReceiveType {
	case pb.ReceiveType_RECEIVE_TYPE_USER:
		if !validID(in.Destination.ReceiveId, 'U') || in.Destination.ReceiveId == in.SenderID {
			return ErrInvalid
		}
	case pb.ReceiveType_RECEIVE_TYPE_GROUP:
		if !validID(in.Destination.ReceiveId, 'G') {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func validText(s string) bool { return s != "" && len(s) <= 16*1024 && utf8.ValidString(s) }

type syncCursor struct {
	Version        int    `json:"v"`
	ConversationID string `json:"conversation_id"`
	AfterPosition  string `json:"after_position"`
}

func encodeCursor(conversation string, after int64) string {
	b, _ := json.Marshal(syncCursor{1, conversation, strconv.FormatInt(after, 10)})
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeCursor(conversation, cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrInvalid
	}
	var c syncCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.ConversationID != conversation {
		return 0, ErrInvalid
	}
	after, err := strconv.ParseInt(c.AfterPosition, 10, 64)
	if err != nil || after <= 0 || strconv.FormatInt(after, 10) != c.AfterPosition || encodeCursor(conversation, after) != cursor {
		return 0, ErrInvalid
	}
	return after, nil
}
func parseConversation(conversation string) (string, bool, error) {
	if strings.HasPrefix(conversation, "direct:") {
		id := strings.TrimPrefix(conversation, "direct:")
		if validID(id, 'S') {
			return id, false, nil
		}
	}
	if strings.HasPrefix(conversation, "group:") {
		id := strings.TrimPrefix(conversation, "group:")
		if validID(id, 'G') {
			return id, true, nil
		}
	}
	return "", false, ErrInvalid
}
