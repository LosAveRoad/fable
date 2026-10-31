package job

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/comet"
	"mychat/internal/dto/im"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const jobToken = "job-01234567890123456789012345678901"

type logicServer struct {
	pb.UnimplementedLogicServiceServer
	lookup  func(context.Context, *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error)
	resolve func(context.Context, *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error)
}

func (s *logicServer) LookupRoutes(ctx context.Context, r *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
	if err := checkJob(ctx); err != nil {
		return nil, err
	}
	return s.lookup(ctx, r)
}
func (s *logicServer) ResolveComets(ctx context.Context, r *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error) {
	if err := checkJob(ctx); err != nil {
		return nil, err
	}
	return s.resolve(ctx, r)
}
func checkJob(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+jobToken {
		return status.Error(codes.Unauthenticated, "wrong role")
	}
	return nil
}
func grpcPair(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	l := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	register(s)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() { s.Stop(); _ = l.Close() })
	c, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type cometServer struct {
	pb.CometServiceServer
	mu       sync.Mutex
	requests []*pb.DeliverRequest
	hook     func(context.Context, *pb.DeliverRequest) (*pb.DeliverResponse, error)
}

func (s *cometServer) Deliver(ctx context.Context, r *pb.DeliverRequest) (*pb.DeliverResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, proto.Clone(r).(*pb.DeliverRequest))
	s.mu.Unlock()
	if s.hook != nil {
		return s.hook(ctx, r)
	}
	return s.CometServiceServer.Deliver(ctx, r)
}
func (s *cometServer) calls() []*pb.DeliverRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.DeliverRequest(nil), s.requests...)
}

type scenario struct {
	t       *testing.T
	logic   *logicServer
	client  pb.LogicServiceClient
	routes  map[string]*pb.RouteLease
	leases  map[string]*pb.CometLease
	clients map[string]pb.CometServiceClient
	mu      sync.Mutex
	lookups [][]string
}

func newScenario(t *testing.T) *scenario {
	s := &scenario{t: t, routes: map[string]*pb.RouteLease{}, leases: map[string]*pb.CometLease{}, clients: map[string]pb.CometServiceClient{}}
	s.logic = &logicServer{}
	s.logic.lookup = func(_ context.Context, r *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.lookups = append(s.lookups, append([]string(nil), r.UserIds...))
		out := &pb.LookupRoutesResponse{}
		for _, id := range r.UserIds {
			v := &pb.RouteLookupResult{UserId: id}
			if route := s.routes[id]; route != nil {
				v.State = &pb.RouteLookupResult_Route{Route: route}
			} else {
				v.State = &pb.RouteLookupResult_Offline{Offline: pb.OfflineReason_OFFLINE_REASON_NO_ROUTE}
			}
			out.Results = append(out.Results, v)
		}
		return out, nil
	}
	s.logic.resolve = func(_ context.Context, r *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error) {
		out := &pb.ResolveCometsResponse{}
		for _, id := range r.InstanceIds {
			v := &pb.CometResolution{InstanceId: id}
			if lease := s.leases[id]; lease != nil {
				v.State = &pb.CometResolution_Lease{Lease: lease}
			} else {
				v.State = &pb.CometResolution_Missing{Missing: pb.MissingCometReason_MISSING_COMET_REASON_NOT_REGISTERED}
			}
			out.Results = append(out.Results, v)
		}
		return out, nil
	}
	s.client = pb.NewLogicServiceClient(grpcPair(t, func(g *grpc.Server) { pb.RegisterLogicServiceServer(g, s.logic) }))
	return s
}
func (s *scenario) addComet(name string) (*comet.Hub, *cometServer) {
	h, err := comet.NewHub(&pb.CometIdentity{InstanceId: name, BootId: "50000000-0000-4000-8000-000000000001"}, 4)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(h.Close)
	rpc, err := comet.NewRPCService(h, comet.RPCCredentials{JobToken: jobToken, LogicToken: "logic-012345678901234567890123456789", AdminToken: "admin-012345678901234567890123456789"})
	if err != nil {
		s.t.Fatal(err)
	}
	server := &cometServer{CometServiceServer: rpc}
	address := name + ":9091"
	s.clients[address] = pb.NewCometServiceClient(grpcPair(s.t, func(g *grpc.Server) { pb.RegisterCometServiceServer(g, server) }))
	s.leases[name] = &pb.CometLease{Identity: h.Identity(), RpcAddress: address, LeaseDeadline: timestamppb.New(time.Now().Add(time.Minute))}
	return h, server
}
func route(id, instance string) *pb.ConnectionIdentity {
	return &pb.ConnectionIdentity{UserId: id, InstanceId: instance, ConnectionId: "20000000-0000-4000-8000-000000000001", RouteToken: "MDEyMzQ1Njc4OWFiY2RlZg"}
}
func (s *scenario) register(h *comet.Hub, id string) *comet.Connection {
	r := route(id, h.Identity().InstanceId)
	c, err := h.Register(context.Background(), r)
	if err != nil {
		s.t.Fatal(err)
	}
	s.routes[id] = &pb.RouteLease{Identity: r, LeaseDeadline: timestamppb.New(time.Now().Add(time.Minute)), AuthExpiresAt: timestamppb.New(time.Now().Add(2 * time.Minute))}
	return c
}
func hint(id *pb.ConnectionIdentity) im.ConnectionIdentity {
	return im.ConnectionIdentity{UserID: id.UserId, InstanceID: id.InstanceId, ConnectionID: id.ConnectionId, RouteToken: id.RouteToken}
}
func (s *scenario) processor(options Options) *Processor {
	p, err := NewProcessor(s.client, func(_ context.Context, address string) (pb.CometServiceClient, error) {
		if c := s.clients[address]; c != nil {
			return c, nil
		}
		return nil, errors.New("unresolved address")
	}, jobToken, options)
	if err != nil {
		s.t.Fatal(err)
	}
	return p
}
func fastOptions() Options {
	return Options{RetryDelays: []time.Duration{time.Millisecond, time.Millisecond}}
}
func receive(t *testing.T, c *comet.Connection) {
	t.Helper()
	select {
	case m := <-c.Messages():
		if m.Content != "hello" || m.Position != 9007199254740993 {
			t.Fatal("bad mapping", m)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered")
	}
}
func assertEmpty(t *testing.T, c *comet.Connection) {
	t.Helper()
	select {
	case m := <-c.Messages():
		t.Fatal("unexpected duplicate", m)
	default:
	}
}

func TestProcessDeliversToTwoCometsUsingResolvedHints(t *testing.T) {
	s := newScenario(t)
	a, sa := s.addComet("comet-a")
	b, sb := s.addComet("comet-b")
	ca := s.register(a, user1)
	cb := s.register(b, user2)
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{hint(ca.Identity()), hint(cb.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || len(got.PendingRecipients()) != 0 || got.Attempts != 1 {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	if got.Recipients[0].Outcome != Enqueued || got.Recipients[1].Outcome != Enqueued {
		t.Fatal(got)
	}
	receive(t, ca)
	receive(t, cb)
	if len(sa.calls()) != 1 || len(sb.calls()) != 1 || len(s.lookups) != 0 {
		t.Fatal("did not use resolved hints")
	}
}
func TestReplayLooksUpOnlyPendingRecipients(t *testing.T) {
	s := newScenario(t)
	a, _ := s.addComet("comet-a")
	ca := s.register(a, user1)
	cb := s.register(a, user2)
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{hint(route(user2, "stale-instance"))}
	task.PendingRecipients = []string{user2}
	task.ReplayRunID = "run-1"
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || len(got.Recipients) != 1 || got.Recipients[0].UserID != user2 || len(got.Task.Recipients) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	receive(t, cb)
	assertEmpty(t, ca)
	if len(s.lookups) != 1 || len(s.lookups[0]) != 1 || s.lookups[0][0] != user2 {
		t.Fatal("wrong replay scope", s.lookups)
	}
}

func TestRetriesOnlyUnresolvedRecipientAfterRouteRepair(t *testing.T) {
	s := newScenario(t)
	a, sa := s.addComet("comet-a")
	b, sb := s.addComet("comet-b")
	ca := s.register(a, user1)
	cb := s.register(b, user2)
	stale := cb.Identity()
	stale.ConnectionId = "20000000-0000-4000-8000-000000000099"
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{hint(ca.Identity()), hint(stale)}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || got.Attempts != 2 {
		t.Fatalf("pending=%v attempts=%d err=%v", got.PendingRecipients(), got.Attempts, err)
	}
	receive(t, ca)
	receive(t, cb)
	assertEmpty(t, ca)
	assertEmpty(t, cb)
	if len(sa.calls()) != 1 || len(sb.calls()) != 2 || len(s.lookups) != 1 || len(s.lookups[0]) != 1 || s.lookups[0][0] != user2 {
		t.Fatal("terminal recipient retried or wrong repair scope")
	}
}
func TestOnlyOneRepairEvenIfRefreshedRouteIsStillStale(t *testing.T) {
	s := newScenario(t)
	a, server := s.addComet("comet-a")
	c := s.register(a, user1)
	stale := c.Identity()
	stale.ConnectionId = "20000000-0000-4000-8000-000000000099"
	s.routes[user1].Identity = stale
	task := taskFixture()
	task.Recipients = []string{user1}
	task.Targets = []im.ConnectionIdentity{hint(stale)}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if !errors.Is(err, ErrUnresolved) || len(got.PendingRecipients()) != 1 || len(server.calls()) != 2 || len(s.lookups) != 1 {
		t.Fatalf("calls=%d lookups=%d pending=%v err=%v", len(server.calls()), len(s.lookups), got.PendingRecipients(), err)
	}
	assertEmpty(t, c)
}
func TestDependencyFailureIsNotOfflineAndRetryBudgetIsBounded(t *testing.T) {
	s := newScenario(t)
	s.logic.lookup = func(context.Context, *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
		return nil, status.Error(codes.Unavailable, "private dependency failure")
	}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, taskFixture()))
	if !errors.Is(err, ErrUnresolved) || got.Attempts != 3 || len(got.PendingRecipients()) != 2 || got.Recipients[0].Reason != DependencyFailure {
		t.Fatalf("%+v %v", got.Recipients, err)
	}
}
func TestMissingInstanceRepairsRouteInsteadOfInventingOffline(t *testing.T) {
	s := newScenario(t)
	h, _ := s.addComet("comet-a")
	c := s.register(h, user1)
	task := taskFixture()
	task.Recipients = []string{user1}
	task.Targets = []im.ConnectionIdentity{hint(route(user1, "expired-instance"))}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || got.Recipients[0].Outcome != Enqueued {
		t.Fatalf("%+v %v", got.Recipients, err)
	}
	receive(t, c)
}
func TestOldBootErrorRefreshesRouteThroughLogic(t *testing.T) {
	s := newScenario(t)
	h, server := s.addComet("comet-a")
	c := s.register(h, user1)
	resolve := s.logic.resolve
	calls := 0
	s.logic.resolve = func(ctx context.Context, r *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error) {
		out, err := resolve(ctx, r)
		calls++
		if calls == 1 {
			out = proto.Clone(out).(*pb.ResolveCometsResponse)
			out.Results[0].GetLease().Identity.BootId = "50000000-0000-4000-8000-000000000099"
		}
		return out, err
	}
	task := taskFixture()
	task.Recipients = []string{user1}
	task.Targets = []im.ConnectionIdentity{hint(c.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || len(server.calls()) != 2 || len(s.lookups) != 1 {
		t.Fatalf("calls=%d lookup=%d pending=%v err=%v", len(server.calls()), len(s.lookups), got.PendingRecipients(), err)
	}
	receive(t, c)
	assertEmpty(t, c)
}
func TestInvalidLookupRepliesNeverBecomeOffline(t *testing.T) {
	for _, kind := range []string{"missing", "wrong-user", "unspecified", "nil-route", "expired-route", "auth-bound"} {
		t.Run(kind, func(t *testing.T) {
			s := newScenario(t)
			h, server := s.addComet("comet-a")
			s.register(h, user1)
			original := s.logic.lookup
			s.logic.lookup = func(ctx context.Context, r *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
				out, _ := original(ctx, r)
				out = proto.Clone(out).(*pb.LookupRoutesResponse)
				switch kind {
				case "missing":
					out.Results = nil
				case "wrong-user":
					out.Results[0].UserId = user3
				case "unspecified":
					out.Results[0].State = &pb.RouteLookupResult_Offline{}
				case "nil-route":
					out.Results[0].State = &pb.RouteLookupResult_Route{}
				case "expired-route":
					out.Results[0].GetRoute().LeaseDeadline = timestamppb.New(time.Now().Add(-time.Second))
				case "auth-bound":
					out.Results[0].GetRoute().AuthExpiresAt = timestamppb.New(time.Now().Add(time.Second))
				}
				return out, nil
			}
			got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, taskFixture()))
			if !errors.Is(err, ErrUnresolved) || len(got.PendingRecipients()) != 2 || len(server.calls()) != 0 {
				t.Fatalf("accepted invalid reply: %+v %v", got.Recipients, err)
			}
		})
	}
}
func TestInvalidResolutionCannotDialOrDeliver(t *testing.T) {
	for _, kind := range []string{"address", "expired", "wrong-instance", "missing-enum", "no-state"} {
		t.Run(kind, func(t *testing.T) {
			s := newScenario(t)
			h, server := s.addComet("comet-a")
			c := s.register(h, user1)
			original := s.logic.resolve
			s.logic.resolve = func(ctx context.Context, r *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error) {
				out, _ := original(ctx, r)
				out = proto.Clone(out).(*pb.ResolveCometsResponse)
				switch kind {
				case "address":
					out.Results[0].GetLease().RpcAddress = "https://evil:9091"
				case "expired":
					out.Results[0].GetLease().LeaseDeadline = timestamppb.New(time.Now().Add(-time.Second))
				case "wrong-instance":
					out.Results[0].GetLease().Identity.InstanceId = "other"
				case "missing-enum":
					out.Results[0].State = &pb.CometResolution_Missing{}
				case "no-state":
					out.Results[0].State = nil
				}
				return out, nil
			}
			task := taskFixture()
			task.Recipients = []string{user1}
			task.Targets = []im.ConnectionIdentity{hint(c.Identity())}
			got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
			if !errors.Is(err, ErrUnresolved) || len(got.PendingRecipients()) != 1 || len(server.calls()) != 0 {
				t.Fatalf("accepted invalid resolution: %+v %v", got.Recipients, err)
			}
		})
	}
}
func TestMalformedDeliveryReplyIsUncertain(t *testing.T) {
	for _, kind := range []string{"missing", "task-id", "target", "enum", "nil-item"} {
		t.Run(kind, func(t *testing.T) {
			s := newScenario(t)
			h, server := s.addComet("comet-a")
			c := s.register(h, user1)
			server.hook = func(_ context.Context, r *pb.DeliverRequest) (*pb.DeliverResponse, error) {
				out := &pb.DeliverResponse{TaskId: r.TaskId, MessageId: r.Message.MessageId, Results: []*pb.DeliveryResult{{Target: r.Targets[0], Outcome: pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED}}}
				switch kind {
				case "missing":
					out.Results = nil
				case "task-id":
					out.TaskId = "wrong"
				case "target":
					out.Results[0].Target = route(user2, "comet-a")
				case "enum":
					out.Results[0].Outcome = 99
				case "nil-item":
					out.Results[0] = nil
				}
				return out, nil
			}
			task := taskFixture()
			task.Recipients = []string{user1}
			task.Targets = []im.ConnectionIdentity{hint(c.Identity())}
			got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
			if !errors.Is(err, ErrUnresolved) || len(got.PendingRecipients()) != 1 || got.Recipients[0].Reason != InvalidResponse {
				t.Fatalf("trusted malformed reply: %+v %v", got.Recipients, err)
			}
		})
	}
}
func TestCancellationPreservesKnownSuccessAndInterruptsRPC(t *testing.T) {
	s := newScenario(t)
	a, _ := s.addComet("comet-a")
	b, blocked := s.addComet("comet-b")
	ca := s.register(a, user1)
	cb := s.register(b, user2)
	entered := make(chan struct{})
	blocked.hook = func(ctx context.Context, _ *pb.DeliverRequest) (*pb.DeliverResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{hint(ca.Identity()), hint(cb.Identity())}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-entered; cancel() }()
	started := time.Now()
	got, err := s.processor(fastOptions()).Process(ctx, taskBytes(t, task))
	if !errors.Is(err, context.Canceled) || len(got.PendingRecipients()) != 1 || got.PendingRecipients()[0] != user2 || got.Recipients[0].Outcome != Enqueued || time.Since(started) > time.Second {
		t.Fatalf("lost partial progress: %+v %v", got.Recipients, err)
	}
	receive(t, ca)
	assertEmpty(t, cb)
}
func TestDeadlineBoundsSlowLookup(t *testing.T) {
	s := newScenario(t)
	s.logic.lookup = func(ctx context.Context, _ *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	started := time.Now()
	got, err := s.processor(Options{Timeout: 40 * time.Millisecond, RPCTimeout: 10 * time.Millisecond, RetryDelays: []time.Duration{time.Millisecond, time.Millisecond}}).Process(context.Background(), taskBytes(t, taskFixture()))
	if !errors.Is(err, ErrUnresolved) || got.Attempts != 3 || len(got.PendingRecipients()) != 2 || time.Since(started) > time.Second {
		t.Fatalf("unbounded RPC or wrong pending: attempts=%d %v", got.Attempts, err)
	}
}
func TestPermissionFailureIsNotRetried(t *testing.T) {
	s := newScenario(t)
	h, server := s.addComet("comet-a")
	c := s.register(h, user1)
	server.hook = func(context.Context, *pb.DeliverRequest) (*pb.DeliverResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "secret")
	}
	task := taskFixture()
	task.Recipients = []string{user1}
	task.Targets = []im.ConnectionIdentity{hint(c.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if !errors.Is(err, ErrUnresolved) || got.Recipients[0].Reason != DeliveryRejected || len(server.calls()) != 1 {
		t.Fatalf("permission failure retried: %+v %v", got.Recipients, err)
	}
}
func TestConcurrentTasksHaveIndependentState(t *testing.T) {
	s := newScenario(t)
	p := s.processor(fastOptions())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := p.Process(context.Background(), taskBytes(t, taskFixture()))
			if err != nil || len(got.Recipients) != 2 || got.Recipients[0].Outcome != Offline {
				t.Errorf("shared state: %+v %v", got.Recipients, err)
			}
		}()
	}
	wg.Wait()
}
func TestInvalidTaskCannotMakeRPC(t *testing.T) {
	s := newScenario(t)
	s.logic.lookup = func(context.Context, *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
		t.Error("invalid task made RPC")
		return nil, nil
	}
	if _, err := s.processor(fastOptions()).Process(context.Background(), []byte(`{"schema_version":99}`)); !errors.Is(err, ErrInvalidTask) {
		t.Fatal(err)
	}
}

func TestOneCometPartialResultsRetryOnlyFailedTarget(t *testing.T) {
	s := newScenario(t)
	h, server := s.addComet("comet-a")
	ca := s.register(h, user1)
	cb := s.register(h, user2)
	attempt := 0
	server.hook = func(ctx context.Context, r *pb.DeliverRequest) (*pb.DeliverResponse, error) {
		attempt++
		if attempt == 1 {
			first := proto.Clone(r).(*pb.DeliverRequest)
			first.Targets = first.Targets[:1]
			out, err := server.CometServiceServer.Deliver(ctx, first)
			if err != nil {
				return nil, err
			}
			out.Results = append(out.Results, &pb.DeliveryResult{Target: r.Targets[1], Outcome: pb.DeliveryOutcome_DELIVERY_OUTCOME_QUEUE_FULL})
			return out, nil
		}
		return server.CometServiceServer.Deliver(ctx, r)
	}
	task := taskFixture()
	task.Targets = []im.ConnectionIdentity{hint(ca.Identity()), hint(cb.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || len(got.PendingRecipients()) != 0 {
		t.Fatalf("%+v %v", got.Recipients, err)
	}
	requests := server.calls()
	if len(requests) != 2 || len(requests[1].Targets) != 1 || requests[1].Targets[0].UserId != user2 {
		t.Fatal("retried known success")
	}
	receive(t, ca)
	receive(t, cb)
	assertEmpty(t, ca)
}
func TestDuplicateReplyAndUnknownTargetExtensionAreTerminal(t *testing.T) {
	s := newScenario(t)
	h, server := s.addComet("comet-a")
	c := s.register(h, user1)
	server.hook = func(_ context.Context, r *pb.DeliverRequest) (*pb.DeliverResponse, error) {
		target := proto.Clone(r.Targets[0]).(*pb.ConnectionIdentity)
		target.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
		return &pb.DeliverResponse{TaskId: r.TaskId, MessageId: r.Message.MessageId, Results: []*pb.DeliveryResult{{Target: target, Outcome: pb.DeliveryOutcome_DELIVERY_OUTCOME_DUPLICATE}}}, nil
	}
	task := taskFixture()
	task.Recipients = []string{user1}
	task.Targets = []im.ConnectionIdentity{hint(c.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if err != nil || got.Recipients[0].Outcome != Duplicate || len(server.calls()) != 1 {
		t.Fatalf("%+v %v", got.Recipients, err)
	}
}
func TestTaskDeadlineIncludesBackoff(t *testing.T) {
	s := newScenario(t)
	s.logic.lookup = func(context.Context, *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
		return nil, status.Error(codes.Unavailable, "private")
	}
	started := time.Now()
	got, err := s.processor(Options{Timeout: 30 * time.Millisecond}).Process(context.Background(), taskBytes(t, taskFixture()))
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrUnresolved) || got.Attempts != 1 || len(got.PendingRecipients()) != 2 || time.Since(started) > time.Second {
		t.Fatalf("backoff ignored deadline: attempts=%d %v", got.Attempts, err)
	}
}
func TestCancellationInterruptsBackoff(t *testing.T) {
	s := newScenario(t)
	entered := make(chan struct{})
	s.logic.lookup = func(context.Context, *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
		close(entered)
		return nil, status.Error(codes.Unavailable, "private")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-entered; cancel() }()
	got, err := s.processor(Options{}).Process(ctx, taskBytes(t, taskFixture()))
	if !errors.Is(err, context.Canceled) || got.Attempts != 1 || len(got.PendingRecipients()) != 2 {
		t.Fatalf("cancel ignored: attempts=%d %v", got.Attempts, err)
	}
}
func TestSameRequestDirectiveUsesAllThreeAttemptsWithoutRepair(t *testing.T) {
	s := newScenario(t)
	h, server := s.addComet("comet-a")
	c := s.register(h, user1)
	st, _ := status.New(codes.Unavailable, "private").WithDetails(&pb.ErrorDetail{Reason: pb.ErrorReason_ERROR_REASON_RESOURCE_EXHAUSTED, Retry: pb.RetryDirective_RETRY_DIRECTIVE_SAME_REQUEST})
	server.hook = func(context.Context, *pb.DeliverRequest) (*pb.DeliverResponse, error) { return nil, st.Err() }
	task := taskFixture()
	task.Recipients = []string{user1}
	task.Targets = []im.ConnectionIdentity{hint(c.Identity())}
	got, err := s.processor(fastOptions()).Process(context.Background(), taskBytes(t, task))
	if !errors.Is(err, ErrUnresolved) || got.Attempts != 3 || len(server.calls()) != 3 || len(s.lookups) != 0 {
		t.Fatalf("incorrect retry directive: attempts=%d calls=%d lookups=%d err=%v", got.Attempts, len(server.calls()), len(s.lookups), err)
	}
}
func TestProcessorRejectsUnsafeConfiguration(t *testing.T) {
	s := newScenario(t)
	dial := func(context.Context, string) (pb.CometServiceClient, error) { return nil, nil }
	for _, tc := range []struct {
		token   string
		options Options
	}{
		{"short", Options{}}, {"a credential with spaces long enough here", Options{}}, {jobToken, Options{Timeout: 11 * time.Second}},
		{jobToken, Options{RPCTimeout: 3 * time.Second}}, {jobToken, Options{RetryDelays: []time.Duration{time.Second}}},
	} {
		if _, err := NewProcessor(s.client, dial, tc.token, tc.options); err == nil {
			t.Fatal("accepted unsafe configuration")
		}
	}
}
