package comet

import (
	"context"
	"errors"
	"strconv"
	"time"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"mychat/internal/dto/wschat"

	"github.com/gorilla/websocket"
)

type Protocol string

const (
	ProtocolLegacy Protocol = "legacy"
	ProtocolV2     Protocol = "v2"
)

var ErrWriterAttached = errors.New("connection already has a websocket writer")

// WriteTo takes ownership of a socket after validation. The caller remains
// responsible for upgrade/authentication and inbound reads. Only one writer is
// permitted. Closing the Hub connection or context interrupts a blocked write.
func (c *Connection) WriteTo(ctx context.Context, socket *websocket.Conn, protocol Protocol) error {
	if socket == nil || protocol != ProtocolLegacy && protocol != ProtocolV2 {
		return invalid("websocket or protocol")
	}
	if !c.writer.CompareAndSwap(false, true) {
		return ErrWriterAttached
	}
	finished := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = socket.Close()
		case <-c.done:
			_ = socket.Close()
		case <-finished:
		}
	}()
	defer func() {
		close(finished)
		_ = socket.Close()
		<-watcherDone
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = c.hub.Unregister(cleanup, c.identity)
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-c.done:
			return nil
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return nil
		case message := <-c.outbound:
			if err := ctx.Err(); err != nil {
				return err
			}
			select {
			case <-c.done:
				return nil
			default:
			}
			if err := socket.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return err
			}
			var frame any
			if protocol == ProtocolV2 {
				frame = im.MessageFrame{Event: im.EventMessage, Message: publicMessage(message)}
			} else {
				frame = legacyMessage(message)
			}
			if err := socket.WriteJSON(frame); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				select {
				case <-c.done:
					return nil
				default:
					return err
				}
			}
		}
	}
}

func publicMessage(m *pb.MessageView) im.Message {
	receiveType := im.ReceiveTypeUser
	if m.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP {
		receiveType = im.ReceiveTypeGroup
	}
	origin := im.MessageOriginUser
	if m.Origin == pb.MessageOrigin_MESSAGE_ORIGIN_AI {
		origin = im.MessageOriginAI
	}
	return im.Message{MessageID: m.MessageId, ConversationID: m.ConversationId, SessionID: m.SessionId, SendID: m.SenderId,
		ReceiveID: m.Destination.ReceiveId, ReceiveType: receiveType, Type: im.MessageTypeText, Content: m.Content, Origin: origin,
		CreatedAt: m.CreatedAt.AsTime().UTC(), Position: strconv.FormatUint(m.Position, 10)}
}

func legacyMessage(m *pb.MessageView) wschat.Message {
	var origin, receiveType int8
	if m.Origin == pb.MessageOrigin_MESSAGE_ORIGIN_AI {
		origin = 1
	}
	if m.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP {
		receiveType = 1
	}
	return wschat.Message{SendID: m.SenderId, ReceiveID: m.Destination.ReceiveId, Content: m.Content, Origin: origin, ReceiveType: receiveType}
}
