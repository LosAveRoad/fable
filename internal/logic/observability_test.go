package logic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/metadata"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"mychat/internal/logic/messagestore"
	"mychat/internal/logic/routing"
)

type observedStore struct {
	saved messagestore.Saved
	err   error
}

func (s *observedStore) Save(context.Context, messagestore.Input) (messagestore.Saved, error) {
	return s.saved, s.err
}
func (s *observedStore) SaveAI(context.Context, string, string, string, string) (messagestore.Saved, error) {
	return s.saved, s.err
}
func (s *observedStore) Sync(context.Context, string, string, string, int) (im.SyncPage, error) {
	return im.SyncPage{}, s.err
}

func observedService(t *testing.T, loggers ...*slog.Logger) (*Service, *observedStore, *capturePublisher, context.Context, *pb.SubmitMessageRequest) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	routes, err := routing.New(rc, "observe:", time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	comet := &pb.CometIdentity{InstanceId: "comet-1", BootId: "50000000-0000-4000-8000-000000000001"}
	if _, err = routes.Register(context.Background(), comet, "127.0.0.1:9010"); err != nil {
		t.Fatal(err)
	}
	saved := sampleSaved()
	route, _, err := routes.Connect(context.Background(), comet, saved.Message.SenderId, "60000000-0000-4000-8000-000000000001", strings.Repeat("r", 32), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	store := &observedStore{saved: saved}
	publisher := &capturePublisher{}
	token := "observability-comet-token-0123456789"
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	service, err := NewService(context.Background(), Config{Logger: logger, Routes: routes, Messages: store, Publisher: publisher, JWTSecret: []byte(strings.Repeat("j", 32)), JobToken: strings.Repeat("j", 32), KickToken: strings.Repeat("k", 32), Comets: []CometGrant{{Identity: comet, RPCAddress: "127.0.0.1:9010", Token: token}}, UserExists: func(context.Context, string) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	req := &pb.SubmitMessageRequest{RequestId: "request-observe-1", Identity: route.Identity, ClientMessageId: "20000000-0000-4000-8000-000000000001", Destination: saved.Message.Destination, Content: "private body should never be logged"}
	return service, store, publisher, ctx, req
}

func scrapeLogic(t *testing.T, s *Service) string {
	t.Helper()
	response := httptest.NewRecorder()
	NewHandler(s, nil, nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics endpoint status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatal("metrics must expose Prometheus text")
	}
	return response.Body.String()
}

// Removing instrumentation around the actual save or publish would lose these
// externally scraped outcomes, including the DB-first uncertain-publish window.
func TestLogicMetricsObserveServiceSaveReplayConflictAndPublish(t *testing.T) {
	s, store, publisher, ctx, req := observedService(t)
	for _, tc := range []struct {
		replayed            bool
		saveErr, publishErr error
		dispatch            pb.DispatchState
	}{
		{dispatch: pb.DispatchState_DISPATCH_STATE_QUEUED},
		{replayed: true, dispatch: pb.DispatchState_DISPATCH_STATE_QUEUED},
		{saveErr: messagestore.ErrConflict},
		{saveErr: messagestore.ErrForbidden},
		{saveErr: errors.New("private database password")},
		{publishErr: ErrNotPublished, dispatch: pb.DispatchState_DISPATCH_STATE_NOT_QUEUED},
		{publishErr: errors.New("private upstream payload"), dispatch: pb.DispatchState_DISPATCH_STATE_UNKNOWN},
	} {
		store.saved.Replayed = tc.replayed
		store.err = tc.saveErr
		publisher.failAt = len(publisher.payloads) + 1
		publisher.failure = tc.publishErr
		result, err := s.SubmitMessage(ctx, req)
		if tc.saveErr != nil {
			if err == nil {
				t.Fatal("save failure disappeared")
			}
			continue
		}
		if err != nil || !result.Result.Stored || result.Result.Dispatch != tc.dispatch {
			t.Fatalf("save/publish behavior changed: %+v %v", result, err)
		}
	}
	metrics := scrapeLogic(t, s)
	for _, want := range []string{
		`fable_logic_save_total{outcome="saved"} 3`,
		`fable_logic_save_total{outcome="replayed"} 1`,
		`fable_logic_save_total{outcome="conflict"} 1`,
		`fable_logic_save_total{outcome="rejected"} 1`,
		`fable_logic_save_total{outcome="error"} 1`,
		`fable_logic_db_duration_seconds_count{operation="save"} 7`,
		`fable_logic_publish_total{outcome="success"} 2`,
		`fable_logic_publish_total{outcome="not_published"} 1`,
		`fable_logic_publish_total{outcome="uncertain"} 1`,
		`fable_logic_dispatch_total{outcome="queued"} 2`,
		`fable_logic_dispatch_total{outcome="not_queued"} 1`,
		`fable_logic_dispatch_total{outcome="unknown"} 1`,
	} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("missing %s\n%s", want, metrics)
		}
	}
	for _, private := range []string{req.RequestId, req.Content, req.Identity.UserId, store.saved.Message.MessageId, "private database", "private upstream", "request_id", "message_id"} {
		if strings.Contains(metrics, private) {
			t.Errorf("unbounded/private metric value %q", private)
		}
	}
}

// Lost or unsafe correlation fields would prevent tracing a known commit through
// an uncertain publish, or leak private dependency details into process logs.
func TestLogicStructuredLogsCorrelateSaveAndPublishWithoutSecrets(t *testing.T) {
	var output bytes.Buffer
	s, store, publisher, ctx, req := observedService(t, slog.New(slog.NewJSONHandler(&output, nil)))
	publisher.failAt = 1
	publisher.failure = errors.New("raw error contains private-upstream-token")
	_, err := s.SubmitMessage(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	logs := output.String()
	for _, secret := range []string{req.Content, "private-upstream-token", req.Identity.UserId, req.Identity.RouteToken, "observability-comet-token", "127.0.0.1"} {
		if strings.Contains(logs, secret) {
			t.Errorf("unsafe logged data %q", secret)
		}
	}
	events := map[string]map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		name, _ := event["msg"].(string)
		events[name] = event
	}
	for _, name := range []string{"logic.save", "logic.publish", "logic.dispatch"} {
		event := events[name]
		if event["request_id"] != req.RequestId || event["message_id"] != store.saved.Message.MessageId {
			t.Errorf("missing %s correlation: %v", name, event)
		}
	}
	if event := events["logic.publish"]; event["task_id"] != "push:v1:M30000000-0000-4000-8000-000000000001:0" || event["outcome"] != "uncertain" {
		t.Errorf("publish correlation/outcome lost: %v", event)
	}
}

// Transport-level failures must remain visible without accepting arbitrary
// method names, request IDs, content, or credentials as metric labels/log data.
func TestLogicRPCMetricsCountDurationErrorsAndSafeCorrelation(t *testing.T) {
	var output bytes.Buffer
	s, _, _, ctx, req := observedService(t, slog.New(slog.NewJSONHandler(&output, nil)))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(s.UnaryInterceptor()))
	pb.RegisterLogicServiceServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewLogicServiceClient(conn)
	md, _ := metadata.FromIncomingContext(ctx)
	outbound, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), time.Second*3)
	defer cancel()
	if _, err = client.SubmitMessage(outbound, req); err != nil {
		t.Fatal(err)
	}
	invalidID := "unsafe-request-body-" + strings.Repeat("x", 129)
	req.RequestId = invalidID
	if _, err = client.SubmitMessage(outbound, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid request code=%v", err)
	}
	if _, err = client.RenewComet(outbound, &pb.RenewCometRequest{RequestId: "request-renew", Identity: &pb.CometIdentity{InstanceId: "comet-1", BootId: "50000000-0000-4000-8000-000000000002"}}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong boot code=%v", err)
	}
	metrics := scrapeLogic(t, s)
	for _, want := range []string{
		`fable_logic_rpc_total{method="SubmitMessage",code="OK"} 1`,
		`fable_logic_rpc_total{method="SubmitMessage",code="InvalidArgument"} 1`,
		`fable_logic_rpc_duration_seconds_count{method="SubmitMessage"} 2`,
		`fable_logic_route_failures_total{method="RenewComet"} 1`,
	} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	logs := output.String()
	for _, secret := range []string{invalidID, req.Content, req.Identity.RouteToken, md.Get("authorization")[0], req.Identity.UserId} {
		if strings.Contains(logs, secret) || strings.Contains(metrics, secret) {
			t.Errorf("unsafe RPC observability %q", secret)
		}
	}
	if !strings.Contains(logs, `"msg":"logic.rpc"`) || !strings.Contains(logs, `"method":"SubmitMessage"`) || !strings.Contains(logs, `"request_id":"request-observe-1"`) {
		t.Fatalf("missing RPC correlation: %s", logs)
	}
}

// AI sends have no caller request ID; a generated safe ID must correlate their
// DB commit and immutable task, while sync contributes its own DB duration.
func TestLogicAIAndSyncMetricsPreserveSafeCorrelation(t *testing.T) {
	var output bytes.Buffer
	s, _, publisher, _, req := observedService(t, slog.New(slog.NewJSONHandler(&output, nil)))
	if _, err := s.SendAI(context.Background(), req.Identity.UserId, "S40000000-0000-4000-8000-000000000001", req.ClientMessageId, req.Content); err != nil {
		t.Fatal(err)
	}
	var task im.PushTask
	if err := json.Unmarshal(publisher.payloads[0], &task); err != nil {
		t.Fatal(err)
	}
	if !validUUID(task.RequestID) {
		t.Errorf("missing generated AI request correlation %q", task.RequestID)
	}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event["request_id"] != task.RequestID {
			t.Errorf("AI correlation changed between stages: %v", event)
		}
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": req.Identity.UserId, "exp": time.Now().Add(time.Hour).Unix()}).SignedString(s.jwtKey)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v2/messages/sync?conversation_id=direct:test", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	NewHandler(s, nil, nil).ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("sync status=%d", response.Code)
	}
	metrics := scrapeLogic(t, s)
	for _, want := range []string{`fable_logic_db_duration_seconds_count{operation="save_ai"} 1`, `fable_logic_db_duration_seconds_count{operation="sync"} 1`} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
}
