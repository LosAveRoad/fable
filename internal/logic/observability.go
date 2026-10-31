package logic

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/logic/messagestore"
)

// Every label has a process-static vocabulary; correlation IDs belong in logs.
type observability struct {
	logger        *slog.Logger
	saves         [5]atomic.Uint64
	db            [3]durationCounter
	publishes     [3]atomic.Uint64
	dispatches    [3]atomic.Uint64
	rpc           [9][17]atomic.Uint64
	rpcDuration   [9]durationCounter
	routeFailures [6]atomic.Uint64
}

func newObservability(logger *slog.Logger) *observability {
	if logger == nil {
		logger = slog.Default()
	}
	return &observability{logger: logger}
}

type durationCounter struct {
	count       atomic.Uint64
	nanoseconds atomic.Uint64
}

func (d *durationCounter) observe(elapsed time.Duration) {
	d.nanoseconds.Add(uint64(max(elapsed, 0)))
	d.count.Add(1)
}

const (
	dbSave = iota
	dbSaveAI
	dbSync
)

var saveOutcomes = [...]string{"saved", "replayed", "conflict", "rejected", "error"}
var dbOperations = [...]string{"save", "save_ai", "sync"}
var publishOutcomes = [...]string{"success", "not_published", "uncertain"}
var dispatchOutcomes = [...]string{"queued", "not_queued", "unknown"}

func (s *Service) observeSave(ctx context.Context, operation int, requestID string, started time.Time, saved messagestore.Saved, err error) {
	if s.observation == nil {
		return
	}
	outcome := 0
	switch {
	case errors.Is(err, messagestore.ErrConflict):
		outcome = 2
	case errors.Is(err, messagestore.ErrInvalid), errors.Is(err, messagestore.ErrForbidden):
		outcome = 3
	case err != nil:
		outcome = 4
	case saved.Replayed:
		outcome = 1
	}
	s.observation.saves[outcome].Add(1)
	elapsed := time.Since(started)
	s.observation.db[operation].observe(elapsed)
	s.logEvent(ctx, "logic.save", requestID, saved.Message.GetMessageId(), "", "operation", dbOperations[operation], "outcome", saveOutcomes[outcome], "duration_seconds", elapsed.Seconds())
}
func (s *Service) observePublish(ctx context.Context, requestID, messageID, taskID string, err error) {
	if s.observation == nil {
		return
	}
	outcome := 0
	if errors.Is(err, ErrNotPublished) {
		outcome = 1
	} else if err != nil {
		outcome = 2
	}
	s.observation.publishes[outcome].Add(1)
	s.logEvent(ctx, "logic.publish", requestID, messageID, taskID, "outcome", publishOutcomes[outcome])
}
func (s *Service) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	observation := s.observation
	if observation == nil {
		observation = &observability{}
	}
	fmt.Fprintln(w, "# TYPE fable_logic_save_total counter")
	for index, outcome := range saveOutcomes {
		fmt.Fprintf(w, "fable_logic_save_total{outcome=%q} %d\n", outcome, observation.saves[index].Load())
	}
	fmt.Fprintln(w, "# TYPE fable_logic_db_duration_seconds summary")
	for index, operation := range dbOperations {
		writeDuration(w, "fable_logic_db_duration_seconds", "operation", operation, &observation.db[index])
	}
	fmt.Fprintln(w, "# TYPE fable_logic_publish_total counter")
	for index, outcome := range publishOutcomes {
		fmt.Fprintf(w, "fable_logic_publish_total{outcome=%q} %d\n", outcome, observation.publishes[index].Load())
	}
	fmt.Fprintln(w, "# TYPE fable_logic_dispatch_total counter")
	for index, outcome := range dispatchOutcomes {
		fmt.Fprintf(w, "fable_logic_dispatch_total{outcome=%q} %d\n", outcome, observation.dispatches[index].Load())
	}
	fmt.Fprintln(w, "# TYPE fable_logic_rpc_total counter")
	fmt.Fprintln(w, "# TYPE fable_logic_rpc_duration_seconds summary")
	fmt.Fprintln(w, "# TYPE fable_logic_route_failures_total counter")
	for index, method := range rpcMethods {
		for code := codes.OK; code <= codes.Unauthenticated; code++ {
			fmt.Fprintf(w, "fable_logic_rpc_total{method=%q,code=%q} %d\n", method, code.String(), observation.rpc[index][code].Load())
		}
		writeDuration(w, "fable_logic_rpc_duration_seconds", "method", method, &observation.rpcDuration[index])
		if index < 6 {
			fmt.Fprintf(w, "fable_logic_route_failures_total{method=%q} %d\n", method, observation.routeFailures[index].Load())
		}
	}
}
func writeDuration(w http.ResponseWriter, name, label, value string, d *durationCounter) {
	fmt.Fprintf(w, "%s_sum{%s=%q} %g\n%s_count{%s=%q} %d\n", name, label, value, float64(d.nanoseconds.Load())/float64(time.Second), name, label, value, d.count.Load())
}

// Log a strict allow-list of fields. Never pass requests, messages, credentials,
// routes, recipients, dependency errors, or network addresses to this boundary.
func (s *Service) logEvent(ctx context.Context, event, requestID, messageID, taskID string, fields ...any) {
	if s.observation == nil || s.observation.logger == nil {
		return
	}
	if requestID != "" && validRequestID(requestID) {
		fields = append(fields, "request_id", requestID)
	}
	if validID(messageID, 'M') {
		fields = append(fields, "message_id", messageID)
		if strings.HasPrefix(taskID, "push:v1:"+messageID+":") {
			index := strings.TrimPrefix(taskID, "push:v1:"+messageID+":")
			if n, err := strconv.ParseUint(index, 10, 32); err == nil && strconv.FormatUint(n, 10) == index {
				fields = append(fields, "task_id", taskID)
			}
		}
	}
	s.observation.logger.InfoContext(ctx, event, fields...)
}

var rpcMethods = [...]string{"RegisterComet", "RenewComet", "UnregisterComet", "Connect", "Heartbeat", "Disconnect", "SubmitMessage", "LookupRoutes", "ResolveComets"}
var rpcFullMethods = [...]string{pb.LogicService_RegisterComet_FullMethodName, pb.LogicService_RenewComet_FullMethodName, pb.LogicService_UnregisterComet_FullMethodName, pb.LogicService_Connect_FullMethodName, pb.LogicService_Heartbeat_FullMethodName, pb.LogicService_Disconnect_FullMethodName, pb.LogicService_SubmitMessage_FullMethodName, pb.LogicService_LookupRoutes_FullMethodName, pb.LogicService_ResolveComets_FullMethodName}

// UnaryInterceptor observes only the nine declared Logic methods. It preserves
// the original response/error and never examines request bodies or credentials.
func (s *Service) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		method := -1
		for index, name := range rpcFullMethods {
			if name == info.FullMethod {
				method = index
				break
			}
		}
		if method < 0 || s.observation == nil {
			return handler(ctx, request)
		}
		started := time.Now()
		response, err := handler(ctx, request)
		elapsed := time.Since(started)
		code := status.Code(err)
		if code > codes.Unauthenticated {
			code = codes.Unknown
		}
		s.observation.rpc[method][code].Add(1)
		s.observation.rpcDuration[method].observe(elapsed)
		if method < 6 && err != nil {
			s.observation.routeFailures[method].Add(1)
		}
		requestID := ""
		if correlation, ok := request.(interface{ GetRequestId() string }); ok {
			requestID = correlation.GetRequestId()
		}
		messageID := ""
		if submit, ok := response.(*pb.SubmitMessageResponse); ok {
			messageID = submit.GetResult().GetMessage().GetMessageId()
		}
		s.logEvent(ctx, "logic.rpc", requestID, messageID, "", "method", rpcMethods[method], "code", code.String(), "duration_seconds", elapsed.Seconds())
		return response, err
	}
}

func (s *Service) observeDispatch(ctx context.Context, requestID, messageID string, state pb.DispatchState) {
	outcome := 2
	switch state {
	case pb.DispatchState_DISPATCH_STATE_QUEUED:
		outcome = 0
	case pb.DispatchState_DISPATCH_STATE_NOT_QUEUED:
		outcome = 1
	}
	if s.observation != nil {
		s.observation.dispatches[outcome].Add(1)
	}
	s.logEvent(ctx, "logic.dispatch", requestID, messageID, "", "outcome", dispatchOutcomes[outcome])
}
