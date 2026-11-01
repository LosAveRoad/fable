package comet

import (
	"context"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type socketLogic struct {
	pb.UnimplementedLogicServiceServer
	mu                             sync.Mutex
	route                          *pb.ConnectionIdentity
	connects, disconnects, submits int
	hold                           chan struct{}
	entered                        chan struct{}
	uncertain                      bool
	heartbeatErr                   bool
	heartbeatUnavailable           bool
	submitErr                      error
	leaseDuration                  time.Duration
	last                           *pb.SubmitMessageRequest
	requests                       []*pb.SubmitMessageRequest
}

func (s *socketLogic) Connect(ctx context.Context, r *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	s.mu.Lock()
	s.connects++
	s.route = &pb.ConnectionIdentity{UserId: testTarget("1").UserId, InstanceId: r.Comet.InstanceId, ConnectionId: r.ConnectionId, RouteToken: r.RouteToken}
	id := proto.Clone(s.route).(*pb.ConnectionIdentity)
	s.mu.Unlock()
	if s.entered != nil {
		close(s.entered)
	}
	if s.hold != nil {
		select {
		case <-s.hold:
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "lost")
		}
	}
	if s.uncertain {
		return nil, status.Error(codes.Unavailable, "lost reply")
	}
	d := s.leaseDuration
	if d == 0 {
		d = time.Minute
	}
	return &pb.ConnectResponse{Route: &pb.RouteLease{Identity: id, AuthExpiresAt: timestamppb.New(time.Now().Add(time.Hour)), LeaseDeadline: timestamppb.New(time.Now().Add(d))}}, nil
}
func (s *socketLogic) Disconnect(_ context.Context, r *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnects++
	if !sameConnection(s.route, r.Identity) {
		return nil, status.Error(codes.InvalidArgument, "wrong tuple")
	}
	return &pb.DisconnectResponse{Outcome: pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED}, nil
}
func (s *socketLogic) Heartbeat(_ context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if s.heartbeatUnavailable {
		return nil, status.Error(codes.Unavailable, "dependency")
	}
	if s.heartbeatErr {
		return nil, status.Error(codes.FailedPrecondition, "replaced")
	}
	return &pb.HeartbeatResponse{Route: &pb.RouteLease{Identity: r.Identity, AuthExpiresAt: timestamppb.New(time.Now().Add(time.Hour)), LeaseDeadline: timestamppb.New(time.Now().Add(time.Minute))}}, nil
}
func (s *socketLogic) SubmitMessage(_ context.Context, r *pb.SubmitMessageRequest) (*pb.SubmitMessageResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.submits++
	s.requests = append(s.requests, proto.Clone(r).(*pb.SubmitMessageRequest))
	if s.submitErr != nil {
		return nil, s.submitErr
	}
	s.last = proto.Clone(r).(*pb.SubmitMessageRequest)
	m := testMessage()
	m.SenderId = r.Identity.UserId
	m.Destination = r.Destination
	m.Content = r.Content
	return &pb.SubmitMessageResponse{Result: &pb.SendResult{ClientMessageId: r.ClientMessageId, Stored: true, Message: m, Dispatch: pb.DispatchState_DISPATCH_STATE_QUEUED}}, nil
}

var socketKey = []byte("socket-jwt-secret-01234567890123456789")

func socketJWT(t *testing.T, d time.Duration) string {
	t.Helper()
	s, e := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": testTarget("1").UserId, "exp": time.Now().Add(d).Unix()}).SignedString(socketKey)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func handlerFixture(t *testing.T, s *socketLogic) (*Hub, string) {
	return handlerFixtureOptions(t, s, nil)
}
func handlerFixtureOptions(t *testing.T, s *socketLogic, configure func(*HandlerConfig)) (*Hub, string) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	g := grpc.NewServer()
	pb.RegisterLogicServiceServer(g, s)
	go g.Serve(l)
	t.Cleanup(g.Stop)
	cc, e := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { cc.Close() })
	h := testHub(t, 256)
	cfg := HandlerConfig{JWTSecret: socketKey, LogicToken: strings.Repeat("c", 32), AllowedOrigins: []string{"http://localhost:5173"}, HeartbeatInterval: 20 * time.Millisecond, RPCTimeout: 100 * time.Millisecond}
	if configure != nil {
		configure(&cfg)
	}
	handler, e := NewHandler(h, pb.NewLogicServiceClient(cc), cfg)
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return h, "ws" + strings.TrimPrefix(server.URL, "http") + "/wss"
}
func dialSocket(t *testing.T, url, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	return websocket.DefaultDialer.Dial(url+"&token="+token, http.Header{"Origin": []string{"http://localhost:5173"}})
}
func TestHandlerRejectsBeforeUpgrade(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixture(t, s)
	for _, tc := range []struct {
		query, token, origin string
		want                 int
	}{{"?protocol=v3", socketJWT(t, time.Hour), "http://localhost:5173", 400}, {"?protocol=v2", "bad", "http://localhost:5173", 401}, {"?protocol=v2", socketJWT(t, time.Hour), "http://evil.test", 403}} {
		_, r, e := websocket.DefaultDialer.Dial(url+tc.query+"&token="+tc.token, http.Header{"Origin": []string{tc.origin}})
		if e == nil || r == nil || r.StatusCode != tc.want {
			t.Fatalf("reject = %v %#v", e, r)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connects != 0 {
		t.Fatal("rejection called Logic")
	}
}
func TestHandlerV2ResultAndDeliveryUseOneWriter(t *testing.T) {
	s := &socketLogic{}
	h, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	id := "70000000-0000-4000-8000-000000000001"
	pongs := 0
	c.SetPongHandler(func(string) error { pongs++; return nil })
	for i := 0; i < 30; i++ {
		if e = c.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if e = c.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: id, ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: "hello"}); e != nil {
			t.Fatal(e)
		}
	}
	s.mu.Lock()
	target := proto.Clone(s.route).(*pb.ConnectionIdentity)
	s.mu.Unlock()
	go func() {
		for i := 0; i < 30; i++ {
			_, _ = h.Deliver(context.Background(), taskID(), testMessage(), []*pb.ConnectionIdentity{target})
		}
	}()
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	results, messages := 0, 0
	for i := 0; i < 60; i++ {
		var f struct {
			Event           string         `json:"event"`
			ClientMessageID string         `json:"client_message_id"`
			Result          *im.SendResult `json:"result"`
		}
		if e = c.ReadJSON(&f); e != nil {
			t.Fatal(e)
		}
		switch f.Event {
		case "send_result":
			results++
			if f.ClientMessageID != id || f.Result == nil || !f.Result.Stored || f.Result.Message.SendID != testTarget("1").UserId {
				t.Fatalf("bad result %+v", f)
			}
		case "message":
			messages++
		default:
			t.Fatal(f.Event)
		}
	}
	if pongs != 30 {
		t.Fatalf("control frames lost: %d pongs", pongs)
	}
	if results != 30 || messages != 30 {
		t.Fatal(results, messages)
	}
}
func TestHandlerUncertainConnectCleansExactPendingWithoutRetry(t *testing.T) {
	s := &socketLogic{uncertain: true}
	h, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e == nil {
		c.Close()
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n, connects := s.disconnects, s.connects
		s.mu.Unlock()
		if n == 1 {
			if connects != 1 {
				t.Fatal("retried uncertain Connect")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	h.Close()
	t.Fatal("missing conditional cleanup")
}
func TestHandlerPendingNotDeliverable(t *testing.T) {
	s := &socketLogic{hold: make(chan struct{}), entered: make(chan struct{})}
	h, url := handlerFixture(t, s)
	done := make(chan struct{})
	go func() {
		c, _, _ := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
		if c != nil {
			c.Close()
		}
		close(done)
	}()
	<-s.entered
	s.mu.Lock()
	id := proto.Clone(s.route).(*pb.ConnectionIdentity)
	s.mu.Unlock()
	r, e := h.Deliver(context.Background(), taskID(), testMessage(), []*pb.ConnectionIdentity{id})
	if e != nil || r[0].Outcome == pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("pending accepted delivery", r, e)
	}
	close(s.hold)
	<-done
}
func TestHandlerHeartbeatReplacementClosesSocket(t *testing.T) {
	s := &socketLogic{heartbeatErr: true}
	_, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, e = c.ReadMessage(); e == nil {
		t.Fatal("replaced socket remains active")
	}
}
func TestHandlerLegacyRejectsSpoofAndSendsNoControl(t *testing.T) {
	s := &socketLogic{}
	h, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=legacy", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.WriteJSON(map[string]any{"send_id": testTarget("2").UserId, "receive_id": testTarget("2").UserId, "content": "spoof"})
	c.WriteJSON(map[string]any{"send_id": testTarget("1").UserId, "receive_id": testTarget("2").UserId, "content": "valid"})
	time.Sleep(30 * time.Millisecond)
	s.mu.Lock()
	n := s.submits
	id := proto.Clone(s.route).(*pb.ConnectionIdentity)
	s.mu.Unlock()
	if n != 1 {
		t.Fatalf("spoof reached Logic: %d", n)
	}
	h.Deliver(context.Background(), taskID(), testMessage(), []*pb.ConnectionIdentity{id})
	c.SetReadDeadline(time.Now().Add(time.Second))
	var frame map[string]any
	if e = c.ReadJSON(&frame); e != nil {
		t.Fatal(e)
	}
	if frame["event"] != nil || frame["content"] != "hello" {
		t.Fatal(frame)
	}
}

// Mutations caught: ignoring the JWT bound, ignoring the lease safety margin,
// and allowing stale reads after local drain closes the connection.
func TestHandlerStopsAtLeaseSafetyDeadline(t *testing.T) {
	s := &socketLogic{leaseDuration: 5100 * time.Millisecond}
	// Prevent renewal from extending the lease: an unavailable heartbeat may
	// preserve the socket only until the original lease's safety deadline.
	s.heartbeatUnavailable = true
	_, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, e = c.ReadMessage(); e == nil {
		t.Fatal("socket outlived lease safety deadline")
	} else if n, ok := e.(net.Error); ok && n.Timeout() {
		t.Fatal("closure was only a test read timeout")
	}
}
func TestHandlerRejectsJWTWithinSafetyMargin(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixture(t, s)
	c, r, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, 4*time.Second))
	if c != nil {
		c.Close()
	}
	if e == nil || r.StatusCode != 401 {
		t.Fatal("accepted unsafe JWT", e, r)
	}
}
func TestHandlerDrainClosesSocketAndRejectsNewConnections(t *testing.T) {
	s := &socketLogic{}
	h, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = h.Drain(context.Background(), time.Now().Add(40*time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	next, r, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if next != nil {
		next.Close()
	}
	if e == nil || r.StatusCode != 503 {
		t.Fatal("drain accepted socket")
	}
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, e = c.ReadMessage(); e == nil {
		t.Fatal("drain left socket open")
	}
}
func TestHandlerV2RejectsSenderAndPreservesUnknownResult(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	id := "70000000-0000-4000-8000-000000000001"
	c.WriteJSON(map[string]any{"event": "send_message", "client_message_id": id, "send_id": testTarget("2").UserId, "receive_id": testTarget("2").UserId, "receive_type": "user", "content": "spoof"})
	c.SetReadDeadline(time.Now().Add(time.Second))
	var frame im.SendResultFrame
	if e = c.ReadJSON(&frame); e != nil {
		t.Fatal(e)
	}
	if frame.Error == nil || frame.Error.Code != im.ErrorInvalidArgument || frame.Result != nil {
		t.Fatal(frame)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.submits != 0 {
		t.Fatal("spoof submitted")
	}
}

func TestHandlerTransportSubmitErrorIsUncertainSameID(t *testing.T) {
	s := &socketLogic{submitErr: status.Error(codes.Unavailable, "sensitive upstream details")}
	_, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	id := "70000000-0000-4000-8000-000000000001"
	c.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: id, ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: "hello"})
	c.SetReadDeadline(time.Now().Add(time.Second))
	var frame im.SendResultFrame
	if e = c.ReadJSON(&frame); e != nil {
		t.Fatal(e)
	}
	if frame.ClientMessageID != id || frame.Result != nil || frame.Error == nil || frame.Error.Code != im.ErrorResultUnknown || frame.Error.Retry != im.RetrySameRequest {
		t.Fatal(frame)
	}
}

func TestHandlerRejectsPlainHTTPBeforeReplacingRoute(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixture(t, s)
	req, e := http.NewRequest(http.MethodGet, "http"+strings.TrimPrefix(url, "ws")+"?protocol=v2&token="+socketJWT(t, time.Hour), nil)
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Origin", "http://localhost:5173")
	response, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatal(response.StatusCode)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connects != 0 {
		t.Fatal("plain HTTP replaced authenticated route")
	}
}

func TestHandlerJWTExpirationCannotBeExtendedByHeartbeat(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, 7*time.Second))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, e = c.ReadMessage()
	if e == nil {
		t.Fatal("expired JWT remained active")
	}
	if n, ok := e.(net.Error); ok && n.Timeout() {
		t.Fatal("JWT expiry did not close the socket")
	}
}

func TestHandlerClosesSilentSocketWithoutRenewingForever(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixtureOptions(t, s, func(c *HandlerConfig) { c.ReadTimeout = 60 * time.Millisecond })
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	time.Sleep(120 * time.Millisecond)
	c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	_, _, e = c.ReadMessage()
	if e == nil {
		t.Fatal("silent socket survived")
	}
	if n, ok := e.(net.Error); ok && n.Timeout() {
		t.Fatal("silent socket remained open past configured read deadline")
	}
}

func TestHandlerAcceptsSixteenKiBContentWithJSONEscapes(t *testing.T) {
	s := &socketLogic{}
	_, url := handlerFixture(t, s)
	c, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: "70000000-0000-4000-8000-000000000001", ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: strings.Repeat("\n", 16*1024)})
	c.SetReadDeadline(time.Now().Add(time.Second))
	var frame im.SendResultFrame
	if e = c.ReadJSON(&frame); e != nil {
		t.Fatal("legal escaped content rejected", e)
	}
	if frame.Result == nil || len(frame.Result.Message.Content) != 16*1024 {
		t.Fatal("legal escaped content rejected", frame.Error)
	}
}

func TestHandlerOldCloseCannotEraseReplacement(t *testing.T) {
	s := &socketLogic{}
	h, url := handlerFixture(t, s)
	first, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	second, _, e := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	first.SetReadDeadline(time.Now().Add(time.Second))
	_, _, e = first.ReadMessage()
	if e == nil {
		t.Fatal("old socket was not closed")
	}
	if n, ok := e.(net.Error); ok && n.Timeout() {
		t.Fatal("old socket was not closed promptly")
	}
	s.mu.Lock()
	target := proto.Clone(s.route).(*pb.ConnectionIdentity)
	s.mu.Unlock()
	result, e := h.Deliver(context.Background(), taskID(), testMessage(), []*pb.ConnectionIdentity{target})
	if e != nil || result[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("old cleanup erased replacement", result, e)
	}
	second.SetReadDeadline(time.Now().Add(time.Second))
	var f im.MessageFrame
	if e = second.ReadJSON(&f); e != nil {
		t.Fatal(e)
	}
	if f.Event != im.EventMessage {
		t.Fatal(f)
	}
}
func TestHandlerHubCloseCancelsPendingConnectPromptly(t *testing.T) {
	s := &socketLogic{hold: make(chan struct{}), entered: make(chan struct{})}
	h, url := handlerFixtureOptions(t, s, func(c *HandlerConfig) { c.RPCTimeout = 2 * time.Second })
	done := make(chan struct{})
	token := socketJWT(t, time.Hour)
	go func() {
		c, _, _ := dialSocket(t, url+"?protocol=v2", token)
		if c != nil {
			c.Close()
		}
		close(done)
	}()
	<-s.entered
	h.Close()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		close(s.hold)
		t.Fatal("pending Connect ignored Hub close")
	}
}

func TestHandlerSubmitTerminalErrorsCloseExactSocket(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolLegacy, ProtocolV2} {
		for _, reason := range []pb.ErrorReason{pb.ErrorReason_ERROR_REASON_SUPERSEDED, pb.ErrorReason_ERROR_REASON_LEASE_EXPIRED, pb.ErrorReason_ERROR_REASON_INSTANCE_NOT_REGISTERED, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED} {
			t.Run(string(protocol)+"/"+reason.String(), func(t *testing.T) {
				failure, err := status.New(codes.FailedPrecondition, "terminal").WithDetails(&pb.ErrorDetail{Reason: reason, Retry: pb.RetryDirective_RETRY_DIRECTIVE_RECONNECT})
				if err != nil {
					t.Fatal(err)
				}
				s := &socketLogic{submitErr: failure.Err()}
				_, url := handlerFixtureOptions(t, s, func(c *HandlerConfig) { c.HeartbeatInterval = time.Second })
				socket, _, err := dialSocket(t, url+"?protocol="+string(protocol), socketJWT(t, time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				var frame any = im.SendFrame{Event: im.EventSendMessage, ClientMessageID: "70000000-0000-4000-8000-000000000001", ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: "first"}
				if protocol == ProtocolLegacy {
					frame = map[string]any{"send_id": testTarget("1").UserId, "receive_id": testTarget("2").UserId, "content": "first"}
				}
				if err = socket.WriteJSON(frame); err != nil {
					t.Fatal(err)
				}
				_ = socket.WriteJSON(frame)
				socket.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
				if protocol == ProtocolV2 {
					var result im.SendResultFrame
					if err = socket.ReadJSON(&result); err != nil {
						t.Fatal("terminal result was not written", err)
					}
					if result.Error == nil {
						t.Fatal(result)
					}
				}
				_, _, err = socket.ReadMessage()
				if err == nil {
					t.Fatal("terminal socket processed another frame")
				}
				if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
					t.Fatal("terminal socket stayed open until heartbeat")
				}
				s.mu.Lock()
				defer s.mu.Unlock()
				if s.submits != 1 {
					t.Fatalf("terminal connection submitted %d messages", s.submits)
				}
			})
		}
	}
}

func TestHandlerBusinessPermissionDeniedKeepsSocketUsable(t *testing.T) {
	failure, err := status.New(codes.PermissionDenied, "membership").WithDetails(&pb.ErrorDetail{Reason: pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, Retry: pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY})
	if err != nil {
		t.Fatal(err)
	}
	s := &socketLogic{submitErr: failure.Err()}
	_, url := handlerFixtureOptions(t, s, func(c *HandlerConfig) { c.HeartbeatInterval = time.Second })
	socket, _, err := dialSocket(t, url+"?protocol=v2", socketJWT(t, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	socket.SetReadDeadline(time.Now().Add(time.Second))
	for i := 0; i < 2; i++ {
		if err = socket.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: "70000000-0000-4000-8000-000000000001", ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: "membership"}); err != nil {
			t.Fatal(err)
		}
		var result im.SendResultFrame
		if err = socket.ReadJSON(&result); err != nil {
			t.Fatal("business denial closed socket", err)
		}
		if result.Error == nil || result.Error.Code != im.ErrorPermissionDenied {
			t.Fatal(result)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.submits != 2 {
		t.Fatal(s.submits)
	}
}

func TestHandlerRejectsInvalidRawUTF8BeforeDecoding(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolLegacy, ProtocolV2} {
		t.Run(string(protocol), func(t *testing.T) {
			s := &socketLogic{}
			_, url := handlerFixture(t, s)
			socket, _, err := dialSocket(t, url+"?protocol="+string(protocol), socketJWT(t, time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			prefix := `{"event":"send_message","client_message_id":"70000000-0000-4000-8000-000000000001","receive_id":"` + testTarget("2").UserId + `","receive_type":"user","content":"`
			if protocol == ProtocolLegacy {
				prefix = `{"send_id":"` + testTarget("1").UserId + `","receive_id":"` + testTarget("2").UserId + `","content":"`
			}
			raw := append([]byte(prefix), 0xff)
			raw = append(raw, []byte(`"}`)...)
			if err = socket.WriteMessage(websocket.TextMessage, raw); err != nil {
				t.Fatal(err)
			}
			socket.SetReadDeadline(time.Now().Add(time.Second))
			if protocol == ProtocolV2 {
				var result im.SendResultFrame
				if err = socket.ReadJSON(&result); err != nil {
					t.Fatal(err)
				}
				if result.Result != nil || result.Error == nil || result.Error.Code != im.ErrorInvalidArgument {
					t.Fatal("raw UTF8 was changed and submitted", result)
				}
			}
			var valid any = im.SendFrame{Event: im.EventSendMessage, ClientMessageID: "70000000-0000-4000-8000-000000000002", ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: "valid"}
			if protocol == ProtocolLegacy {
				valid = map[string]any{"send_id": testTarget("1").UserId, "receive_id": testTarget("2").UserId, "content": "valid"}
			}
			if err = socket.WriteJSON(valid); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for {
				s.mu.Lock()
				n, last := s.submits, s.last
				s.mu.Unlock()
				if last != nil && last.Content == "valid" {
					if n != 1 {
						t.Fatalf("invalid raw UTF8 reached Logic: %d submits", n)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("valid follow-up frame never submitted")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestSocketSubmitHasFreshRequestIDForEachAttempt(t *testing.T) {
	for _, protocol := range []string{"v2", "legacy"} {
		t.Run(protocol, func(t *testing.T) {
			s := &socketLogic{}
			_, url := handlerFixture(t, s)
			c, _, err := dialSocket(t, url+"?protocol="+protocol, socketJWT(t, time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			id := "70000000-0000-4000-8000-000000000001"
			for attempt := 0; attempt < 2; attempt++ {
				if protocol == "v2" {
					err = c.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: id, ReceiveID: testTarget("2").UserId, ReceiveType: im.ReceiveTypeUser, Content: "private text"})
				} else {
					err = c.WriteJSON(map[string]any{"send_id": testTarget("1").UserId, "receive_id": testTarget("2").UserId, "receive_type": 0, "content": "private text"})
				}
				if err != nil {
					t.Fatal(err)
				}
				if protocol == "v2" {
					c.SetReadDeadline(time.Now().Add(time.Second))
					var frame im.SendResultFrame
					if err = c.ReadJSON(&frame); err != nil {
						t.Fatal(err)
					}
					if frame.ClientMessageID != id || (attempt == 0 && frame.Result == nil) || (attempt == 1 && frame.Error == nil) {
						t.Fatalf("bad result: %+v", frame)
					}
				}
				until := time.Now().Add(time.Second)
				for {
					s.mu.Lock()
					n := len(s.requests)
					s.mu.Unlock()
					if n > attempt {
						break
					}
					if time.Now().After(until) {
						t.Fatal("missing RPC attempt")
					}
					time.Sleep(time.Millisecond)
				}
				s.mu.Lock()
				s.submitErr = status.Error(codes.PermissionDenied, "destination rejected")
				s.mu.Unlock()
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			a, b := s.requests[0], s.requests[1]
			if !validUUID(a.RequestId) || !validUUID(b.RequestId) || a.RequestId == b.RequestId {
				t.Fatalf("missing fresh canonical request IDs: %q %q", a.RequestId, b.RequestId)
			}
			if protocol == "v2" && (a.ClientMessageId != id || b.ClientMessageId != id) {
				t.Fatal("retry identity changed")
			}
		})
	}
}

func TestHubCongestionMetricsObserveActualTransitions(t *testing.T) {
	h := testHub(t, 1)
	handler, err := NewHandler(h, pb.NewLogicServiceClient(nil), HandlerConfig{JWTSecret: socketKey, LogicToken: strings.Repeat("c", 32), AllowedOrigins: []string{"http://localhost:5173"}})
	if err != nil {
		t.Fatal(err)
	}
	metrics := func() string {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		return r.Body.String()
	}
	require := func(want string) {
		t.Helper()
		if got := metrics(); !strings.Contains(got, want+"\n") {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
	c := register(t, h, testTarget("1"))
	deliver(t, h, c.Identity())
	require("fable_comet_queue_depth 1")
	stale := c.Identity()
	stale.ConnectionId = "20000000-0000-4000-8000-000000000099"
	deliver(t, h, stale)
	require("fable_comet_stale_targets_total 1")
	deliver(t, h, c.Identity())
	waitClosed(t, c)
	require("fable_comet_queue_full_total 1")
	require("fable_comet_slow_closes_total 1")
	require("fable_comet_queue_depth 0")
	other := register(t, h, testTarget("2"))
	if err := queueControl(context.Background(), other, socketControl{kind: websocket.PingMessage}); err != nil {
		t.Fatal(err)
	}
	require("fable_comet_queue_depth 1")
	if err := queueControl(context.Background(), other, socketControl{kind: websocket.PingMessage}); err == nil {
		t.Fatal("full control queue accepted")
	}
	require("fable_comet_queue_full_total 2")
	require("fable_comet_slow_closes_total 2")
	if _, err := h.Drain(context.Background(), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	require("fable_comet_draining 1")
	if _, err := h.Register(context.Background(), testTarget("3")); err != ErrDraining {
		t.Fatal("drain admitted connection", err)
	}
}

func TestClosedHubMetricsDoNotInventQueueDepth(t *testing.T) {
	h := testHub(t, 1)
	h.Close()
	var output strings.Builder
	h.writeMetrics(context.Background(), &output)
	if strings.Contains(output.String(), "fable_comet_queue_depth") {
		t.Fatal("unavailable depth reported as measured", output.String())
	}
	if !strings.Contains(output.String(), "fable_comet_queue_full_total 0\n") {
		t.Fatal("counters unavailable after close")
	}
}
