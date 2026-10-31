package logic

import (
	"context"
	"encoding/json"
	"errors"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"mychat/internal/logic/messagestore"
	"strconv"
)

// Dispatch runs after known DB commit. No publishing failure may turn stored
// into false or return a misleading database failure to the sender.
func (s *Service) dispatch(ctx context.Context, clientID string, saved messagestore.Saved, requestID string) *pb.SendResult {
	result := &pb.SendResult{ClientMessageId: clientID, Stored: true, Message: saved.Message, Replayed: saved.Replayed, Dispatch: pb.DispatchState_DISPATCH_STATE_QUEUED}
	count := (len(saved.Recipients) + 99) / 100
	published := 0
	for index := 0; index < count; index++ {
		ids := append([]string(nil), saved.Recipients[index*100:min((index+1)*100, len(saved.Recipients))]...)
		targets := make([]im.ConnectionIdentity, 0, len(ids))
		// Hints are optional; recipients remain complete so Job can resolve later.
		if routes, e := s.routes.Lookup(ctx, ids); e == nil {
			for _, r := range routes {
				if lease := r.GetRoute(); lease != nil {
					a := lease.Identity
					targets = append(targets, im.ConnectionIdentity{UserID: a.UserId, InstanceID: a.InstanceId, ConnectionID: a.ConnectionId, RouteToken: a.RouteToken})
				}
			}
		}
		task := im.PushTask{SchemaVersion: 1, Kind: im.PushKind, TaskID: "push:v1:" + saved.Message.MessageId + ":" + strconv.Itoa(index), Message: publicMessage(saved.Message), Recipients: ids, Targets: targets, ChunkIndex: uint32(index), ChunkCount: uint32(count), RequestID: requestID}
		raw, e := json.Marshal(task)
		if e != nil || len(raw) > 256*1024 {
			if published == 0 {
				result.Dispatch = pb.DispatchState_DISPATCH_STATE_NOT_QUEUED
			} else {
				result.Dispatch = pb.DispatchState_DISPATCH_STATE_UNKNOWN
			}
			return result
		}
		if e = s.publisher.Publish(ctx, saved.Message.ConversationId, raw); e != nil {
			result.Dispatch = pb.DispatchState_DISPATCH_STATE_UNKNOWN
			if published == 0 && errors.Is(e, ErrNotPublished) {
				result.Dispatch = pb.DispatchState_DISPATCH_STATE_NOT_QUEUED
			}
			return result
		}
		published++
	}
	return result
}
func publicMessage(m *pb.MessageView) im.Message {
	receive := im.ReceiveTypeUser
	if m.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP {
		receive = im.ReceiveTypeGroup
	}
	origin := im.MessageOriginUser
	if m.Origin == pb.MessageOrigin_MESSAGE_ORIGIN_AI {
		origin = im.MessageOriginAI
	}
	return im.Message{MessageID: m.MessageId, ConversationID: m.ConversationId, SessionID: m.SessionId, SendID: m.SenderId, ReceiveID: m.Destination.ReceiveId, ReceiveType: receive, Type: im.MessageTypeText, Content: m.Content, Origin: origin, CreatedAt: m.CreatedAt.AsTime().UTC(), Position: strconv.FormatUint(m.Position, 10)}
}
