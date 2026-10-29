package comet

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "mychat/api/gen/fable/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testComet() *pb.CometIdentity {
	return &pb.CometIdentity{InstanceId: "comet-one", BootId: "50000000-0000-4000-8000-000000000001"}
}

func testTarget(n string) *pb.ConnectionIdentity {
	return &pb.ConnectionIdentity{UserId: "U10000000-0000-4000-8000-00000000000" + n,
		InstanceId: "comet-one", ConnectionId: "20000000-0000-4000-8000-00000000000" + n,
		RouteToken: "MDEyMzQ1Njc4OWFiY2RlZg"}
}

func testMessage() *pb.MessageView {
	return &pb.MessageView{MessageId: "M30000000-0000-4000-8000-000000000001",
		ConversationId: "direct:S40000000-0000-4000-8000-000000000001", SessionId: "S40000000-0000-4000-8000-000000000001",
		SenderId:    "U10000000-0000-4000-8000-000000000009",
		Destination: &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_USER, ReceiveId: testTarget("1").UserId},
		MessageType: pb.MessageType_MESSAGE_TYPE_TEXT, Content: "hello", Origin: pb.MessageOrigin_MESSAGE_ORIGIN_USER,
		CreatedAt: timestamppb.New(time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)), Position: 9007199254740993}
}

func taskID() string { return "push:v1:M30000000-0000-4000-8000-000000000001:0" }

func testHub(t *testing.T, size int) *Hub {
	t.Helper()
	h, err := NewHub(testComet(), size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

func register(t *testing.T, h *Hub, id *pb.ConnectionIdentity) *Connection {
	t.Helper()
	c, err := h.Register(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func deliver(t *testing.T, h *Hub, ids ...*pb.ConnectionIdentity) []*pb.DeliveryResult {
	t.Helper()
	r, err := h.Deliver(context.Background(), taskID(), testMessage(), ids)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func waitClosed(t *testing.T, c *Connection) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("connection did not close")
	}
}

func TestReplacementFencesOldUnregister(t *testing.T) {
	h := testHub(t, 2)
	oldID := testTarget("1")
	old := register(t, h, oldID)
	newID := proto.Clone(oldID).(*pb.ConnectionIdentity)
	newID.ConnectionId = "20000000-0000-4000-8000-000000000099"
	current := register(t, h, newID)
	waitClosed(t, old)
	got, err := h.Unregister(context.Background(), oldID)
	if err != nil || got != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
		t.Fatalf("old unregister = %v, %v", got, err)
	}
	results := deliver(t, h, oldID, newID)
	if results[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_STALE || results[1].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatalf("results = %v", results)
	}
	select {
	case m := <-current.Messages():
		if m.Content != "hello" {
			t.Fatal(m)
		}
	default:
		t.Fatal("new connection lost")
	}
}

func TestRepeatedRegistrationKeepsSameActiveConnection(t *testing.T) {
	h := testHub(t, 2)
	original := register(t, h, testTarget("1"))
	repeated := register(t, h, testTarget("1"))
	if repeated != original {
		t.Fatal("same registration replaced a live connection")
	}
	select {
	case <-original.Done():
		t.Fatal("same registration closed original socket")
	default:
	}
}

func TestUnknownFieldsAreNotPartOfConnectionIdentity(t *testing.T) {
	h := testHub(t, 2)
	original := register(t, h, testTarget("1"))
	extended := testTarget("1")
	extended.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
	repeated := register(t, h, extended)
	if repeated != original {
		t.Fatal("unknown extension replaced the same connection")
	}
	if deliver(t, h, extended)[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("unknown extension fenced a valid target")
	}
	result, err := h.Unregister(context.Background(), extended)
	if err != nil || result != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED {
		t.Fatalf("extended unregister = %v, %v", result, err)
	}
}

func TestRegisterRejectsNoncanonicalRouteToken(t *testing.T) {
	for _, token := range []string{"MDEyMzQ1Njc4OWFiY2RlZg\n", "MDEyMzQ1Njc4OWFiY2RlZh"} {
		h := testHub(t, 1)
		id := testTarget("1")
		id.RouteToken = token
		if _, err := h.Register(context.Background(), id); !errors.Is(err, ErrInvalid) {
			t.Fatalf("noncanonical token accepted: %v", err)
		}
	}
}

func TestDeliveryReportsEachTargetAndClosesOnlySlowConnection(t *testing.T) {
	h := testHub(t, 1)
	slow := register(t, h, testTarget("1"))
	fast := register(t, h, testTarget("2"))
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("initial enqueue failed")
	}
	r := deliver(t, h, testTarget("1"), testTarget("3"), testTarget("2"))
	want := []pb.DeliveryOutcome{pb.DeliveryOutcome_DELIVERY_OUTCOME_QUEUE_FULL, pb.DeliveryOutcome_DELIVERY_OUTCOME_NOT_FOUND, pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED}
	wantTargets := []*pb.ConnectionIdentity{testTarget("1"), testTarget("3"), testTarget("2")}
	for i := range want {
		if r[i].Outcome != want[i] || !proto.Equal(r[i].Target, wantTargets[i]) {
			t.Fatalf("result %d = %v", i, r[i])
		}
	}
	waitClosed(t, slow)
	select {
	case <-fast.Done():
		t.Fatal("fast connection closed")
	default:
	}
}

func TestDeliveryOwnsInputSnapshots(t *testing.T) {
	h := testHub(t, 2)
	id := testTarget("1")
	c := register(t, h, id)
	id.RouteToken = "mutated"
	m := testMessage()
	r, err := h.Deliver(context.Background(), taskID(), m, []*pb.ConnectionIdentity{testTarget("1")})
	if err != nil {
		t.Fatal(err)
	}
	m.Content = "mutated"
	r[0].Target.RouteToken = "mutated"
	if m := <-c.Messages(); m.Content != "hello" {
		t.Fatalf("queued input mutated: %v", m)
	}
	copyID := c.Identity()
	copyID.RouteToken = "mutated"
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("identity aliased")
	}
}

func TestCancelledDeliveryHasNoQueueSideEffect(t *testing.T) {
	h := testHub(t, 1)
	c := register(t, h, testTarget("1"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.Deliver(ctx, taskID(), testMessage(), []*pb.ConnectionIdentity{testTarget("1")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("cancelled request filled the queue")
	}
	select {
	case <-c.Done():
		t.Fatal("cancelled request closed connection")
	default:
	}
}

func TestInvalidBatchHasNoPartialSideEffects(t *testing.T) {
	h := testHub(t, 1)
	register(t, h, testTarget("1"))
	bad := testTarget("2")
	bad.RouteToken = "short"
	_, err := h.Deliver(context.Background(), taskID(), testMessage(), []*pb.ConnectionIdentity{testTarget("1"), bad})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid batch = %v", err)
	}
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("invalid batch partially enqueued")
	}
}

func TestDrainCannotExtendDeadlineAndClosesConnections(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	deadline := time.Now().Add(100 * time.Millisecond)
	s, err := h.Drain(context.Background(), deadline)
	if err != nil || s.AlreadyDraining {
		t.Fatalf("drain = %v, %v", s, err)
	}
	s, err = h.Drain(context.Background(), time.Now().Add(20*time.Second))
	if err != nil || !s.AlreadyDraining || !s.Deadline.Equal(deadline) {
		t.Fatalf("repeat drain = %v, %v", s, err)
	}
	if _, err := h.Register(context.Background(), testTarget("2")); !errors.Is(err, ErrDraining) {
		t.Fatalf("register while draining = %v", err)
	}
	waitClosed(t, c)
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_DRAINING {
		t.Fatal("delivered past drain deadline")
	}
}

func TestConcurrentCloseAndCommandsFinish(t *testing.T) {
	h := testHub(t, 4)
	c := register(t, h, testTarget("1"))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = h.Deliver(context.Background(), taskID(), testMessage(), []*pb.ConnectionIdentity{testTarget("1")})
			}
		}()
	}
	h.Close()
	wg.Wait()
	h.Close()
	waitClosed(t, c)
	if _, err := h.Register(context.Background(), testTarget("2")); !errors.Is(err, ErrClosed) {
		t.Fatalf("register after close = %v", err)
	}
}
