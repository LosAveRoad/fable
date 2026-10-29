package job

import (
	"context"
	"errors"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"strings"
	"testing"
	"time"
)

func TestReviewRejectLoneSurrogate(t *testing.T) {
	raw := strings.Replace(string(taskBytes(t, taskFixture())), `"content":"hello"`, `"content":"\ud800"`, 1)
	got, err := DecodeTask([]byte(raw))
	if !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("accepted unpaired surrogate, content=%q, err=%v", got.Message.Content, err)
	}
}

func TestReviewLeaseExpiresBeforeSecondDelivery(t *testing.T) {
	s := newScenario(t)
	a, sa := s.addComet("comet-a")
	b, sb := s.addComet("comet-b")
	ca := s.register(a, user1)
	cb := s.register(b, user2)
	deadline := time.Now().Add(100 * time.Millisecond)
	s.leases["comet-b"].LeaseDeadline = timestamppb.New(deadline)
	sa.hook = func(ctx context.Context, r *pb.DeliverRequest) (*pb.DeliverResponse, error) {
		timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return sa.CometServiceServer.Deliver(ctx, r)
	}
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{hint(ca.Identity()), hint(cb.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if len(sb.calls()) > 0 || got.Recipients[1].Outcome == Enqueued {
		t.Fatalf("used expired Comet lease: second_calls=%d outcome=%s err=%v", len(sb.calls()), got.Recipients[1].Outcome, err)
	}
}

func TestReviewRouteExpiresDuringResolution(t *testing.T) {
	s := newScenario(t)
	a, server := s.addComet("comet-a")
	s.register(a, user1)
	deadline := time.Now().Add(100 * time.Millisecond)
	s.routes[user1].LeaseDeadline = timestamppb.New(deadline)
	resolve := s.logic.resolve
	s.logic.resolve = func(ctx context.Context, r *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error) {
		timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return resolve(ctx, r)
	}
	task := taskFixture()
	task.Recipients = []string{user1}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if len(server.calls()) > 0 || got.Recipients[0].Outcome == Enqueued {
		t.Fatalf("used expired route lease: calls=%d outcome=%s err=%v", len(server.calls()), got.Recipients[0].Outcome, err)
	}
}

func TestReviewLeaseExpiresDuringDial(t *testing.T) {
	for _, kind := range []string{"route", "comet"} {
		t.Run(kind, func(t *testing.T) {
			s := newScenario(t)
			h, server := s.addComet("comet-a")
			s.register(h, user1)
			deadline := time.Now().Add(100 * time.Millisecond)
			if kind == "route" {
				s.routes[user1].LeaseDeadline = timestamppb.New(deadline)
			} else {
				s.leases["comet-a"].LeaseDeadline = timestamppb.New(deadline)
			}
			p, err := NewProcessor(s.client, func(ctx context.Context, address string) (pb.CometServiceClient, error) {
				timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-timer.C:
				}
				return s.clients[address], nil
			}, jobToken, fastOptions())
			if err != nil {
				t.Fatal(err)
			}
			task := taskFixture()
			task.Recipients = []string{user1}
			got, err := p.Process(context.Background(), taskBytes(t, task))
			if len(server.calls()) != 0 || !errors.Is(err, ErrUnresolved) || got.Recipients[0].Outcome != Unresolved {
				t.Fatalf("delivered after %s lease expired during dial: calls=%d pending=%v err=%v", kind, len(server.calls()), got.PendingRecipients(), err)
			}
		})
	}
}
func TestReviewConfirmedEnqueueSurvivesInFlightLeaseExpiry(t *testing.T) {
	s := newScenario(t)
	h, server := s.addComet("comet-a")
	s.register(h, user1)
	deadline := time.Now().Add(100 * time.Millisecond)
	s.routes[user1].LeaseDeadline = timestamppb.New(deadline)
	server.hook = func(ctx context.Context, r *pb.DeliverRequest) (*pb.DeliverResponse, error) {
		timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return server.CometServiceServer.Deliver(ctx, r)
	}
	task := taskFixture()
	task.Recipients = []string{user1}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || got.Recipients[0].Outcome != Enqueued {
		t.Fatalf("discarded confirmed success: %+v %v", got.Recipients, err)
	}
}
func TestReviewUnicodeEscapesPreserveValidContent(t *testing.T) {
	for _, tc := range []struct{ wire, want string }{
		{`\ud800`, ""}, {`\udc00`, ""}, {`\ud800x`, ""}, {`\ud800\ud800`, ""}, {`\udc00\ud800`, ""},
		{`\ud83d\ude00`, "😀"}, {`\uFFFD`, "�"}, {`�`, "�"}, {`\\ud800`, `\ud800`},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			raw := strings.Replace(string(taskBytes(t, taskFixture())), `"content":"hello"`, `"content":"`+tc.wire+`"`, 1)
			got, err := DecodeTask([]byte(raw))
			if tc.want == "" {
				if !errors.Is(err, ErrInvalidTask) {
					t.Fatal("accepted malformed Unicode")
				}
			} else if err != nil || got.Message.Content != tc.want {
				t.Fatalf("valid Unicode changed: got=%q err=%v", got.Message.Content, err)
			}
		})
	}
}
