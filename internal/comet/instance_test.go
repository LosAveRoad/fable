package comet

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "mychat/api/gen/fable/v1"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

const cometToken = "comet-012345678901234567890123456789"

type instanceLogic struct {
	pb.UnimplementedLogicServiceServer
	register func(context.Context, *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error)
	renew    func(context.Context, *pb.RenewCometRequest) (*pb.RenewCometResponse, error)
	mu       sync.Mutex
	deleted  []*pb.UnregisterCometRequest
}

func checkComet(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("authorization"); len(v) != 1 || v[0] != "Bearer "+cometToken {
		return status.Error(codes.Unauthenticated, "private")
	}
	return nil
}
func (s *instanceLogic) RegisterComet(c context.Context, r *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
	if e := checkComet(c); e != nil {
		return nil, e
	}
	return s.register(c, r)
}
func (s *instanceLogic) RenewComet(c context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
	if e := checkComet(c); e != nil {
		return nil, e
	}
	return s.renew(c, r)
}
func (s *instanceLogic) UnregisterComet(c context.Context, r *pb.UnregisterCometRequest) (*pb.UnregisterCometResponse, error) {
	if e := checkComet(c); e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, proto.Clone(r).(*pb.UnregisterCometRequest))
	return &pb.UnregisterCometResponse{Outcome: pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED}, nil
}
func lease(draining bool, ttl time.Duration) *pb.CometLease {
	return &pb.CometLease{Identity: testComet(), RpcAddress: "127.0.0.1:9091", Draining: draining, LeaseDeadline: timestamppb.New(time.Now().Add(ttl))}
}
func logicPair(t *testing.T, s *instanceLogic) pb.LogicServiceClient {
	t.Helper()
	l := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer()
	pb.RegisterLogicServiceServer(g, s)
	go func() { _ = g.Serve(l) }()
	t.Cleanup(func() { g.Stop(); _ = l.Close() })
	c, e := grpc.NewClient("passthrough:///logic", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return pb.NewLogicServiceClient(c)
}
func newInstanceLogic() *instanceLogic {
	return &instanceLogic{
		register: func(context.Context, *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
			return &pb.RegisterCometResponse{Lease: lease(false, time.Minute)}, nil
		},
		renew: func(_ context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
			return &pb.RenewCometResponse{Lease: lease(r.Draining, time.Minute)}, nil
		},
	}
}
func waitInstance(t *testing.T, i *Instance) {
	t.Helper()
	select {
	case <-i.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("instance did not stop")
	}
}
func TestInstanceRegistersRenewsAndConditionallyUnregisters(t *testing.T) {
	s := newInstanceLogic()
	renewed := make(chan bool, 10)
	s.renew = func(_ context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		renewed <- r.Draining
		return &pb.RenewCometResponse{Lease: lease(r.Draining, time.Minute)}, nil
	}
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RenewInterval: 10 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	if !i.Ready() {
		t.Fatal("valid registration not ready")
	}
	select {
	case <-renewed:
	case <-time.After(time.Second):
		t.Fatal("no renewal")
	}
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); i.Close() }()
	}
	wg.Wait()
	waitClosed(t, c)
	if i.Ready() || !errors.Is(i.Err(), ErrInstanceStopped) {
		t.Fatalf("ready=%v err=%v", i.Ready(), i.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.deleted) != 1 || !sameComet(s.deleted[0].Identity, testComet()) {
		t.Fatal("incorrect conditional unregister")
	}
}
func TestInstanceLeaseCutoffInterruptsBlockedRenewal(t *testing.T) {
	s := newInstanceLogic()
	s.register = func(context.Context, *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
		return &pb.RegisterCometResponse{Lease: lease(false, 5200*time.Millisecond)}, nil
	}
	s.renew = func(ctx context.Context, _ *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	h := testHub(t, 1)
	c := register(t, h, testTarget("1"))
	started := time.Now()
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RenewInterval: 10 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	waitInstance(t, i)
	waitClosed(t, c)
	if time.Since(started) > time.Second || i.Ready() || !errors.Is(i.Err(), ErrInstanceLeaseLost) {
		t.Fatalf("served beyond safe cutoff: %v", i.Err())
	}
}
func TestInstanceRejectsMalformedRegistrationAndCleansUncertainRoute(t *testing.T) {
	for _, kind := range []string{"boot", "address", "expiry", "draining", "nil", "error"} {
		t.Run(kind, func(t *testing.T) {
			s := newInstanceLogic()
			s.register = func(context.Context, *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
				v := lease(false, time.Minute)
				switch kind {
				case "boot":
					v.Identity.BootId = "50000000-0000-4000-8000-000000000099"
				case "address":
					v.RpcAddress = "other:1"
				case "expiry":
					v.LeaseDeadline = timestamppb.New(time.Now().Add(4 * time.Second))
				case "draining":
					v.Draining = true
				case "nil":
					v = nil
				case "error":
					return nil, status.Error(codes.Unavailable, "sensitive body")
				}
				return &pb.RegisterCometResponse{Lease: v}, nil
			}
			h := testHub(t, 1)
			c := register(t, h, testTarget("1"))
			_, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{})
			if e == nil || strings.Contains(e.Error(), "sensitive") {
				t.Fatal("invalid registration accepted/leaked", e)
			}
			waitClosed(t, c)
			s.mu.Lock()
			defer s.mu.Unlock()
			if len(s.deleted) != 1 {
				t.Fatal("uncertain registration not cleaned")
			}
		})
	}
}
func TestInstanceDrainIsMonotonicAndPropagated(t *testing.T) {
	s := newInstanceLogic()
	drained := make(chan struct{}, 1)
	s.renew = func(_ context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		if r.Draining {
			select {
			case drained <- struct{}{}:
			default:
			}
		}
		return &pb.RenewCometResponse{Lease: lease(r.Draining, time.Minute)}, nil
	}
	h := testHub(t, 1)
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	if _, e := h.Drain(context.Background(), time.Now().Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if i.Ready() {
		t.Fatal("draining instance ready")
	}
	select {
	case <-drained:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain not propagated promptly")
	}
}
func TestPermanentRenewalFailureClosesHub(t *testing.T) {
	s := newInstanceLogic()
	s.renew = func(context.Context, *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "private")
	}
	h := testHub(t, 1)
	c := register(t, h, testTarget("1"))
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RenewInterval: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	waitInstance(t, i)
	waitClosed(t, c)
	if !errors.Is(i.Err(), ErrInstanceLeaseLost) {
		t.Fatal(i.Err())
	}
}

func TestTransientRenewalFailureRecoversWithinLease(t *testing.T) {
	s := newInstanceLogic()
	failed := make(chan struct{})
	allow := make(chan struct{})
	var once sync.Once
	s.renew = func(ctx context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		first := false
		once.Do(func() { first = true; close(failed) })
		if first {
			return nil, status.Error(codes.Unavailable, "private")
		}
		select {
		case <-allow:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return &pb.RenewCometResponse{Lease: lease(r.Draining, time.Minute)}, nil
	}
	h := testHub(t, 1)
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RenewInterval: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	<-failed
	deadline := time.Now().Add(time.Second)
	for i.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if i.Ready() {
		t.Fatal("dependency failure stayed ready")
	}
	close(allow)
	deadline = time.Now().Add(time.Second)
	for !i.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !i.Ready() {
		t.Fatal("valid renewal did not recover")
	}
}
func TestDrainDuringInflightRenewalDoesNotRestoreActive(t *testing.T) {
	s := newInstanceLogic()
	entered := make(chan struct{})
	release := make(chan struct{})
	drained := make(chan struct{}, 1)
	var once sync.Once
	s.renew = func(ctx context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		first := false
		once.Do(func() { first = true; close(entered) })
		if first {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		}
		if r.Draining {
			select {
			case drained <- struct{}{}:
			default:
			}
		}
		return &pb.RenewCometResponse{Lease: lease(r.Draining, time.Minute)}, nil
	}
	h := testHub(t, 1)
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RenewInterval: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	<-entered
	_, e = h.Drain(context.Background(), time.Now().Add(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	close(release)
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("in-flight active renewal lost draining notification")
	}
	if i.Ready() {
		t.Fatal("drain reverted")
	}
}
func TestRegistrationTimeoutClosesHubAndCleans(t *testing.T) {
	s := newInstanceLogic()
	s.register = func(ctx context.Context, _ *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	h := testHub(t, 1)
	c := register(t, h, testTarget("1"))
	_, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RPCTimeout: 20 * time.Millisecond})
	if !errors.Is(e, ErrInstanceLeaseLost) {
		t.Fatal(e)
	}
	waitClosed(t, c)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.deleted) != 1 {
		t.Fatal("timeout cleanup omitted")
	}
}
func TestInvalidRenewalClosesWithoutReregistration(t *testing.T) {
	s := newInstanceLogic()
	s.renew = func(context.Context, *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
		v := lease(false, time.Minute)
		v.Identity.BootId = "50000000-0000-4000-8000-000000000099"
		return &pb.RenewCometResponse{Lease: v}, nil
	}
	h := testHub(t, 1)
	i, e := StartInstance(context.Background(), h, logicPair(t, s), "127.0.0.1:9091", cometToken, InstanceOptions{RenewInterval: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	waitInstance(t, i)
	if !errors.Is(i.Err(), ErrInstanceLeaseLost) {
		t.Fatal(i.Err())
	}
}
