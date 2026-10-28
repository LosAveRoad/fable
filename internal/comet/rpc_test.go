package comet

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pb "mychat/api/gen/fable/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testCredentials() RPCCredentials {
	return RPCCredentials{JobToken: strings.Repeat("j", 32), LogicToken: strings.Repeat("l", 32), AdminToken: strings.Repeat("a", 32)}
}

func rpcClient(t *testing.T, h *Hub) pb.CometServiceClient {
	t.Helper()
	service, err := NewRPCService(h, testCredentials())
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterCometServiceServer(server, service)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() { server.Stop(); _ = lis.Close() })
	conn, err := grpc.NewClient("passthrough:///comet-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewCometServiceClient(conn)
}

func rpcContext(t *testing.T, token string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func deliveryRequest() *pb.DeliverRequest {
	return &pb.DeliverRequest{Comet: testComet(), TaskId: taskID(), Message: testMessage(), Targets: []*pb.ConnectionIdentity{testTarget("1")}, RequestId: "rpc-test"}
}

func TestRPCRoleChecksCannotTouchConnection(t *testing.T) {
	h := testHub(t, 1)
	c := register(t, h, testTarget("1"))
	client := rpcClient(t, h)
	creds := testCredentials()
	for _, tc := range []struct {
		token string
		want  codes.Code
	}{{"", codes.Unauthenticated}, {"unknown-token", codes.Unauthenticated}, {creds.LogicToken, codes.PermissionDenied}, {creds.AdminToken, codes.PermissionDenied}} {
		if _, err := client.Deliver(rpcContext(t, tc.token), deliveryRequest()); status.Code(err) != tc.want {
			t.Fatalf("Deliver role = %v, want %v", err, tc.want)
		}
	}
	if _, err := client.Kick(rpcContext(t, creds.JobToken), &pb.KickRequest{Comet: testComet(), Target: testTarget("1"), Reason: pb.KickReason_KICK_REASON_REPLACED}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if _, err := client.Drain(rpcContext(t, creds.LogicToken), &pb.DrainRequest{Comet: testComet(), Deadline: timestamppb.New(time.Now().Add(time.Second))}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	r, err := client.Deliver(rpcContext(t, creds.JobToken), deliveryRequest())
	if err != nil || r.Results[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatalf("unauthorized call had side effect: %v %v", r, err)
	}
	select {
	case <-c.Done():
		t.Fatal("unauthorized call closed connection")
	default:
	}
}

func TestRPCBootFenceReturnsDetailsBeforeAnyEnqueue(t *testing.T) {
	h := testHub(t, 1)
	register(t, h, testTarget("1"))
	client := rpcClient(t, h)
	req := deliveryRequest()
	req.Comet.BootId = "50000000-0000-4000-8000-000000000099"
	_, err := client.Deliver(rpcContext(t, testCredentials().JobToken), req)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	var detail *pb.ErrorDetail
	for _, d := range status.Convert(err).Details() {
		if v, ok := d.(*pb.ErrorDetail); ok {
			detail = v
		}
	}
	if detail == nil || detail.Reason != pb.ErrorReason_ERROR_REASON_INSTANCE_NOT_REGISTERED || detail.Retry != pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE {
		t.Fatalf("wrong status detail: %v", detail)
	}
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("old boot mutated queue")
	}
}

func TestRPCReportsOrderedPartialResultsAndKickIsExact(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	client := rpcClient(t, h)
	creds := testCredentials()
	req := deliveryRequest()
	stale := testTarget("1")
	stale.ConnectionId = "20000000-0000-4000-8000-000000000099"
	req.Targets = []*pb.ConnectionIdentity{testTarget("2"), stale, testTarget("1")}
	r, err := client.Deliver(rpcContext(t, creds.JobToken), req)
	if err != nil {
		t.Fatal(err)
	}
	if r.TaskId != taskID() || r.MessageId != testMessage().MessageId || len(r.Results) != 3 {
		t.Fatal(r)
	}
	want := []pb.DeliveryOutcome{pb.DeliveryOutcome_DELIVERY_OUTCOME_NOT_FOUND, pb.DeliveryOutcome_DELIVERY_OUTCOME_STALE, pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED}
	for i, w := range want {
		if r.Results[i].Outcome != w || !proto.Equal(r.Results[i].Target, req.Targets[i]) {
			t.Fatalf("result %d: %v", i, r.Results[i])
		}
	}
	kick, err := client.Kick(rpcContext(t, creds.LogicToken), &pb.KickRequest{Comet: testComet(), Target: stale, Reason: pb.KickReason_KICK_REASON_REPLACED})
	if err != nil || kick.Outcome != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
		t.Fatalf("stale kick=%v %v", kick, err)
	}
	kick, err = client.Kick(rpcContext(t, creds.LogicToken), &pb.KickRequest{Comet: testComet(), Target: testTarget("1"), Reason: pb.KickReason_KICK_REASON_REPLACED})
	if err != nil || kick.Outcome != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED {
		t.Fatalf("kick=%v %v", kick, err)
	}
	waitClosed(t, c)
}

func TestRPCInvalidRequestsDoNotPartiallyDeliver(t *testing.T) {
	cases := []struct {
		name   string
		change func(*pb.DeliverRequest)
	}{
		{"invalid target", func(r *pb.DeliverRequest) {
			bad := testTarget("2")
			bad.RouteToken = "short"
			r.Targets = append(r.Targets, bad)
		}},
		{"duplicate target", func(r *pb.DeliverRequest) { r.Targets = append(r.Targets, testTarget("1")) }},
		{"unknown enum", func(r *pb.DeliverRequest) { r.Message.Origin = 99 }},
		{"group session mismatch", func(r *pb.DeliverRequest) { r.Message.Destination.ReceiveType = pb.ReceiveType_RECEIVE_TYPE_GROUP }},
		{"zero position", func(r *pb.DeliverRequest) { r.Message.Position = 0 }},
		{"invalid timestamp", func(r *pb.DeliverRequest) { r.Message.CreatedAt = nil }},
		{"oversized content", func(r *pb.DeliverRequest) { r.Message.Content = strings.Repeat("x", 16*1024+1) }},
		{"invalid request id", func(r *pb.DeliverRequest) { r.RequestId = "line\nbreak" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := testHub(t, 1)
			register(t, h, testTarget("1"))
			client := rpcClient(t, h)
			req := deliveryRequest()
			tc.change(req)
			_, err := client.Deliver(rpcContext(t, testCredentials().JobToken), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
			if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
				t.Fatal("invalid request changed queue")
			}
		})
	}
}

func TestRPCDrainClosesConnectionsAndRejectsNewOnes(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	client := rpcClient(t, h)
	deadline := time.Now().Add(150 * time.Millisecond)
	r, err := client.Drain(rpcContext(t, testCredentials().AdminToken), &pb.DrainRequest{Comet: testComet(), Deadline: timestamppb.New(deadline)})
	if err != nil || r.AlreadyDraining || r.ActiveConnections != 1 {
		t.Fatalf("drain=%v %v", r, err)
	}
	r, err = client.Drain(rpcContext(t, testCredentials().AdminToken), &pb.DrainRequest{Comet: testComet(), Deadline: timestamppb.New(time.Now().Add(20 * time.Second))})
	if err != nil || !r.AlreadyDraining || !r.Deadline.AsTime().Equal(deadline) {
		t.Fatalf("repeat drain=%v %v", r, err)
	}
	waitClosed(t, c)
}

func TestRPCRequiresIndependentCredentials(t *testing.T) {
	h := testHub(t, 1)
	if _, err := NewRPCService(h, RPCCredentials{}); err == nil {
		t.Fatal("accepted empty credentials")
	}
	creds := testCredentials()
	creds.LogicToken = creds.JobToken
	if _, err := NewRPCService(h, creds); err == nil {
		t.Fatal("accepted role-equivalent credentials")
	}
}
