// Package job processes saved-message push tasks. It owns no business storage,
// Kafka offsets, or connection state; callers own the consumer lifecycle.
package job

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrInvalidTask = errors.New("invalid push task")

func invalidTask(field string) error { return fmt.Errorf("%w: %s", ErrInvalidTask, field) }

// DecodeTask rejects unsupported/ambiguous data before any RPC. A future consumer
// must retain the ORIGINAL bytes on error, never re-marshal the returned value.
func DecodeTask(raw []byte) (im.PushTask, error) {
	var task im.PushTask
	if len(raw) > 256*1024 || !utf8.Valid(raw) {
		return task, invalidTask("encoding or size")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := checkJSON(d, 0); err != nil {
		return task, invalidTask("JSON structure")
	}
	if _, err := d.Token(); err != io.EOF {
		return task, invalidTask("trailing JSON")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&task); err != nil {
		return task, invalidTask("JSON fields")
	}
	fields, ok := requiredObject(raw, "schema_version", "kind", "task_id", "message", "recipients", "targets", "chunk_index", "chunk_count")
	if !ok {
		return task, invalidTask("required fields")
	}
	if _, ok := requiredObject(fields["message"], "message_id", "conversation_id", "session_id", "send_id", "receive_id", "receive_type", "type", "content", "origin", "created_at", "position"); !ok {
		return task, invalidTask("message fields")
	}
	if task.SchemaVersion != 1 || task.Kind != "message_push" || task.ChunkCount == 0 || task.ChunkIndex >= task.ChunkCount || task.TaskID != "push:v1:"+task.Message.MessageID+":"+strconv.FormatUint(uint64(task.ChunkIndex), 10) {
		return task, invalidTask("task identity or version")
	}
	if _, err := messageView(task.Message); err != nil {
		return task, err
	}
	if len(task.Recipients) < 1 || len(task.Recipients) > 100 || task.Targets == nil {
		return task, invalidTask("recipients or targets")
	}
	recipients := make(map[string]bool, len(task.Recipients))
	for i, id := range task.Recipients {
		if !validID(id, 'U') || i > 0 && task.Recipients[i-1] >= id {
			return task, invalidTask("recipient ordering or identity")
		}
		if task.Message.ReceiveType == im.ReceiveTypeUser && id != task.Message.SendID && id != task.Message.ReceiveID {
			return task, invalidTask("direct recipient")
		}
		recipients[id] = true
	}
	seen := map[string]bool{}
	for _, h := range task.Targets {
		if !recipients[h.UserID] || seen[h.UserID] || !validConnection(connectionView(h)) {
			return task, invalidTask("route hints")
		}
		seen[h.UserID] = true
	}
	if request, exists := fields["request_id"]; exists && (bytes.Equal(request, []byte("null")) || !printable(task.RequestID, 128)) {
		return task, invalidTask("request_id")
	}
	_, subset := fields["pending_recipients"]
	_, run := fields["replay_run_id"]
	if subset != run || subset && (len(task.PendingRecipients) == 0 || !printable(task.ReplayRunID, 128)) {
		return task, invalidTask("replay context")
	}
	seen = map[string]bool{}
	for _, id := range task.PendingRecipients {
		if !recipients[id] || seen[id] {
			return task, invalidTask("pending recipients")
		}
		seen[id] = true
	}
	return task, nil
}

func requiredObject(raw []byte, keys ...string) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, false
	}
	for _, key := range keys {
		value, ok := fields[key]
		if !ok || bytes.Equal(value, []byte("null")) {
			return nil, false
		}
	}
	return fields, true
}

// Reject duplicate/case-aliased keys rather than accepting encoding/json's
// last-value-wins behavior for immutable task identities and replay subsets.
func checkJSON(d *json.Decoder, depth int) error {
	if depth > 16 {
		return ErrInvalidTask
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := k.(string)
			if !ok || seen[key] || key != strings.ToLower(key) {
				return ErrInvalidTask
			}
			seen[key] = true
			if err := checkJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := checkJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidTask
	}
	_, err = d.Token()
	return err
}

func validUUID(s string) bool {
	return len(s) == 36 && s != uuid.Nil.String() && uuid.Validate(s) == nil
}
func validID(s string, prefix byte) bool { return len(s) == 37 && s[0] == prefix && validUUID(s[1:]) }
func validInstance(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func printable(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
func validConnection(id *pb.ConnectionIdentity) bool {
	if id == nil || !validID(id.UserId, 'U') || !validInstance(id.InstanceId) || !validUUID(id.ConnectionId) || len(id.RouteToken) > 128 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(id.RouteToken)
	return err == nil && len(raw) >= 16 && base64.RawURLEncoding.EncodeToString(raw) == id.RouteToken
}
func connectionView(h im.ConnectionIdentity) *pb.ConnectionIdentity {
	return &pb.ConnectionIdentity{UserId: h.UserID, InstanceId: h.InstanceID, ConnectionId: h.ConnectionID, RouteToken: h.RouteToken}
}
func messageView(m im.Message) (*pb.MessageView, error) {
	position, err := strconv.ParseInt(m.Position, 10, 64)
	_, offset := m.CreatedAt.Zone()
	if !validID(m.MessageID, 'M') || !validID(m.SendID, 'U') || err != nil || position <= 0 || strconv.FormatInt(position, 10) != m.Position || m.CreatedAt.IsZero() || offset != 0 || timestamppb.New(m.CreatedAt).CheckValid() != nil {
		return nil, invalidTask("message identity, position or time")
	}
	if m.Type != im.MessageTypeText || m.Content == "" || len(m.Content) > 16*1024 || !utf8.ValidString(m.Content) {
		return nil, invalidTask("message content")
	}
	origin := pb.MessageOrigin_MESSAGE_ORIGIN_USER
	switch m.Origin {
	case im.MessageOriginUser:
	case im.MessageOriginAI:
		origin = pb.MessageOrigin_MESSAGE_ORIGIN_AI
	default:
		return nil, invalidTask("origin")
	}
	receive := pb.ReceiveType_RECEIVE_TYPE_USER
	switch m.ReceiveType {
	case im.ReceiveTypeUser:
		if !validID(m.SessionID, 'S') || !validID(m.ReceiveID, 'U') || m.SendID == m.ReceiveID || m.ConversationID != "direct:"+m.SessionID {
			return nil, invalidTask("direct destination")
		}
	case im.ReceiveTypeGroup:
		receive = pb.ReceiveType_RECEIVE_TYPE_GROUP
		if !validID(m.SessionID, 'G') || m.SessionID != m.ReceiveID || m.ConversationID != "group:"+m.SessionID {
			return nil, invalidTask("group destination")
		}
	default:
		return nil, invalidTask("receive_type")
	}
	return &pb.MessageView{MessageId: m.MessageID, ConversationId: m.ConversationID, SessionId: m.SessionID, SenderId: m.SendID, Destination: &pb.Destination{ReceiveType: receive, ReceiveId: m.ReceiveID}, MessageType: pb.MessageType_MESSAGE_TYPE_TEXT, Content: m.Content, Origin: origin, CreatedAt: timestamppb.New(m.CreatedAt), Position: uint64(position)}, nil
}
