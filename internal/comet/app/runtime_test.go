package app

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "mychat/api/gen/fable/v1"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type runtimeLogic struct {
	expectedAddress string
	pb.UnimplementedLogicServiceServer
	t            *testing.T
	kickToken    string
	registered   chan struct{}
	unregistered chan struct{}
}

func (s *runtimeLogic) RegisterComet(ctx context.Context, r *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
	if r.RpcAddress != s.expectedAddress {
		return nil, fmt.Errorf("registered wrong grant address")
	}
	// Registration may immediately call back into this Comet. This fails if
	// Run registers the instance before starting its private RPC listener.
	pool, e := grpc.NewClient(r.RpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return nil, e
	}
	defer pool.Close()
	call, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.kickToken), time.Second)
	defer cancel()
	_, e = pb.NewCometServiceClient(pool).Kick(call, &pb.KickRequest{Comet: r.Identity, Target: &pb.ConnectionIdentity{UserId: "U11111111-1111-4111-8111-111111111111", InstanceId: r.Identity.InstanceId, ConnectionId: "22222222-2222-4222-8222-222222222222", RouteToken: "MDEyMzQ1Njc4OWFiY2RlZg"}, Reason: pb.KickReason_KICK_REASON_REPLACED})
	if e != nil {
		return nil, e
	}
	close(s.registered)
	return &pb.RegisterCometResponse{Lease: &pb.CometLease{Identity: r.Identity, RpcAddress: r.RpcAddress, LeaseDeadline: timestamppb.New(time.Now().Add(time.Minute))}}, nil
}
func (s *runtimeLogic) UnregisterComet(context.Context, *pb.UnregisterCometRequest) (*pb.UnregisterCometResponse, error) {
	close(s.unregistered)
	return &pb.UnregisterCometResponse{Outcome: pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED}, nil
}
func unusedAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func TestRunStartsRPCBeforeRegistrationAndClosesOnCancellation(t *testing.T) {
	c := validConfig()
	c.HTTPAddress = unusedAddress(t)
	c.RPCAddress = "localhost:" + strings.Split(unusedAddress(t), ":")[1]
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	c.LogicAddress = listener.Addr().String()
	s := &runtimeLogic{t: t, kickToken: c.KickToken, expectedAddress: c.RPCAddress, registered: make(chan struct{}), unregistered: make(chan struct{})}
	g := grpc.NewServer()
	pb.RegisterLogicServiceServer(g, s)
	go g.Serve(listener)
	defer g.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, c) }()
	select {
	case <-s.registered:
	case e := <-done:
		t.Fatal("registration failed", e)
	case <-time.After(3 * time.Second):
		t.Fatal("registration timeout")
	}
	deadline := time.Now().Add(time.Second)
	for {
		response, e := http.Get("http://" + c.HTTPAddress + "/readyz")
		if e == nil {
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal(response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(time.Millisecond)
	}
	response, e := http.Get("http://" + c.HTTPAddress + "/metrics")
	if e != nil {
		t.Fatal(e)
	}
	response.Body.Close()
	cancel()
	select {
	case e := <-done:
		if e != nil && !strings.Contains(e.Error(), "lease stopped") {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not terminate")
	}
	select {
	case <-s.unregistered:
	default:
		t.Fatal("missing instance cleanup")
	}
	if l, e := net.Listen("tcp", c.RPCAddress); e != nil {
		t.Fatal("rpc listener leaked", e)
	} else {
		l.Close()
	}
}
