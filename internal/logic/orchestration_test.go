package logic

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"mychat/internal/logic/messagestore"
	"mychat/internal/logic/routing"
	"strings"
	"testing"
	"time"
)

type capturePublisher struct {
	payloads [][]byte
	keys     []string
	failAt   int
	failure  error
}

func (p *capturePublisher) Publish(_ context.Context, key string, payload []byte) error {
	p.keys = append(p.keys, key)
	p.payloads = append(p.payloads, append([]byte(nil), payload...))
	if p.failAt == len(p.payloads) {
		return p.failure
	}
	return nil
}
func sampleSaved() messagestore.Saved {
	return messagestore.Saved{Message: &pb.MessageView{MessageId: "M30000000-0000-4000-8000-000000000001", ConversationId: "direct:S40000000-0000-4000-8000-000000000001", SessionId: "S40000000-0000-4000-8000-000000000001", SenderId: "U10000000-0000-4000-8000-000000000001", Destination: &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_USER, ReceiveId: "U10000000-0000-4000-8000-000000000002"}, MessageType: pb.MessageType_MESSAGE_TYPE_TEXT, Content: "hello", Origin: pb.MessageOrigin_MESSAGE_ORIGIN_USER, CreatedAt: timestamppb.New(time.Now()), Position: 9007199254740993}, Recipients: []string{"U10000000-0000-4000-8000-000000000001", "U10000000-0000-4000-8000-000000000002"}}
}
func TestPostCommitPublishFailureNeverPretendsNotStored(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		want    pb.DispatchState
	}{{"ack", nil, pb.DispatchState_DISPATCH_STATE_QUEUED}, {"uncertain", errors.New("body secret"), pb.DispatchState_DISPATCH_STATE_UNKNOWN}, {"definite", ErrNotPublished, pb.DispatchState_DISPATCH_STATE_NOT_QUEUED}} {
		t.Run(tc.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()
			routes, e := routing.New(client, "test:", time.Minute, time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			pub := &capturePublisher{}
			if tc.failure != nil {
				pub.failAt = 1
				pub.failure = tc.failure
			}
			s := &Service{routes: routes, publisher: pub}
			got := s.dispatch(context.Background(), "20000000-0000-4000-8000-000000000001", sampleSaved(), "request-1")
			if !got.Stored || got.Dispatch != tc.want || got.Message.Position != 9007199254740993 {
				t.Fatalf("lost commit result: %v", got)
			}
			var task im.PushTask
			if len(pub.payloads) != 1 || json.Unmarshal(pub.payloads[0], &task) != nil || len(task.Recipients) != 2 || task.Targets == nil || len(task.Targets) != 0 || task.Message.Position != "9007199254740993" {
				t.Fatal("lost offline recipients or wire precision")
			}
			if !strings.HasPrefix(task.TaskID, "push:v1:M30000000-0000-4000-8000-000000000001:") || pub.keys[0] != "direct:S40000000-0000-4000-8000-000000000001" {
				t.Fatal("bad Kafka identity/key")
			}
		})
	}
}
