package comet

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	pb "mychat/api/gen/fable/v1"

	"github.com/google/uuid"
)

func invalid(field string) error { return fmt.Errorf("%w: %s", ErrInvalid, field) }

func validUUID(s string) bool {
	return len(s) == 36 && s != uuid.Nil.String() && uuid.Validate(s) == nil
}

func validID(s string, prefix byte) bool {
	return len(s) == 37 && s[0] == prefix && validUUID(s[1:])
}

func validateComet(id *pb.CometIdentity) error {
	if id == nil || !validUUID(id.BootId) || len(id.InstanceId) == 0 || len(id.InstanceId) > 64 {
		return invalid("comet identity")
	}
	for _, c := range id.InstanceId {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return invalid("instance_id")
		}
	}
	return nil
}

func validateConnection(id *pb.ConnectionIdentity, instance string) error {
	if id == nil || !validID(id.UserId, 'U') || id.InstanceId != instance || !validUUID(id.ConnectionId) || len(id.RouteToken) > 128 {
		return invalid("connection identity")
	}
	raw, err := base64.RawURLEncoding.DecodeString(id.RouteToken)
	if err != nil || len(raw) < 16 || base64.RawURLEncoding.EncodeToString(raw) != id.RouteToken {
		return invalid("route_token")
	}
	return nil
}

// Unknown protobuf fields are extensions, not part of the identity fence.
func sameComet(a, b *pb.CometIdentity) bool {
	return a != nil && b != nil && a.InstanceId == b.InstanceId && a.BootId == b.BootId
}

func sameConnection(a, b *pb.ConnectionIdentity) bool {
	return a != nil && b != nil && a.UserId == b.UserId && a.InstanceId == b.InstanceId &&
		a.ConnectionId == b.ConnectionId && subtle.ConstantTimeCompare([]byte(a.RouteToken), []byte(b.RouteToken)) == 1
}

func validateMessage(m *pb.MessageView) error {
	if m == nil || !validID(m.MessageId, 'M') || !validID(m.SenderId, 'U') || m.Destination == nil {
		return invalid("message identity")
	}
	if m.MessageType != pb.MessageType_MESSAGE_TYPE_TEXT || m.Content == "" || len(m.Content) > 16*1024 || !utf8.ValidString(m.Content) {
		return invalid("message content")
	}
	if m.Origin != pb.MessageOrigin_MESSAGE_ORIGIN_USER && m.Origin != pb.MessageOrigin_MESSAGE_ORIGIN_AI {
		return invalid("message origin")
	}
	if m.CreatedAt == nil || m.CreatedAt.CheckValid() != nil || m.CreatedAt.AsTime().IsZero() || m.Position == 0 || m.Position > math.MaxInt64 {
		return invalid("message time or position")
	}
	switch m.Destination.ReceiveType {
	case pb.ReceiveType_RECEIVE_TYPE_USER:
		if !validID(m.SessionId, 'S') || !validID(m.Destination.ReceiveId, 'U') || m.SenderId == m.Destination.ReceiveId || m.ConversationId != "direct:"+m.SessionId {
			return invalid("direct destination")
		}
	case pb.ReceiveType_RECEIVE_TYPE_GROUP:
		if !validID(m.SessionId, 'G') || m.SessionId != m.Destination.ReceiveId || m.ConversationId != "group:"+m.SessionId {
			return invalid("group destination")
		}
	default:
		return invalid("receive_type")
	}
	return nil
}

func validateDelivery(taskID string, m *pb.MessageView, targets []*pb.ConnectionIdentity, instance string) error {
	if err := validateMessage(m); err != nil {
		return err
	}
	prefix := "push:v1:" + m.MessageId + ":"
	if !strings.HasPrefix(taskID, prefix) {
		return invalid("task_id")
	}
	chunk := strings.TrimPrefix(taskID, prefix)
	n, err := strconv.ParseUint(chunk, 10, 32)
	if err != nil || strconv.FormatUint(n, 10) != chunk {
		return invalid("chunk index")
	}
	if len(targets) == 0 || len(targets) > 100 {
		return invalid("target count")
	}
	type key struct{ user, instance, connection, token string }
	seen := make(map[key]struct{}, len(targets))
	for _, id := range targets {
		if err := validateConnection(id, instance); err != nil {
			return err
		}
		k := key{id.UserId, id.InstanceId, id.ConnectionId, id.RouteToken}
		if _, exists := seen[k]; exists {
			return invalid("duplicate target")
		}
		seen[k] = struct{}{}
	}
	return nil
}

func validRequestID(s string) bool {
	if len(s) > 128 {
		return false
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
