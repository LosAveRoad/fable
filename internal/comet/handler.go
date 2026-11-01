package comet

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/auth"
	"mychat/internal/dto/im"
	"mychat/internal/dto/wschat"
)

type HandlerConfig struct {
	JWTSecret                     []byte
	LogicToken                    string
	AllowedOrigins                []string
	Ready                         func() bool
	HeartbeatInterval, RPCTimeout time.Duration
	ReadTimeout                   time.Duration
}
type publicHandler struct {
	hub                                                      *Hub
	logic                                                    pb.LogicServiceClient
	config                                                   HandlerConfig
	origins                                                  map[string]bool
	accepted, rejected, active, uncertain, heartbeatFailures atomic.Uint64
}

// NewHandler owns no shared clients. The caller closes the Hub before closing
// Logic's client pool. Application data and Ping/Pong/control frames use one
// writer. Gorilla may issue concurrency-safe automatic error Close controls
// inside ReadMessage when rejecting oversized or invalid protocol frames.
func NewHandler(hub *Hub, logic pb.LogicServiceClient, c HandlerConfig) (http.Handler, error) {
	if hub == nil || logic == nil || len(c.JWTSecret) < 32 || !instanceToken(c.LogicToken) || len(c.AllowedOrigins) == 0 {
		return nil, invalid("handler configuration")
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 20 * time.Second
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 2 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 60 * time.Second
	}
	if c.ReadTimeout <= c.HeartbeatInterval || c.ReadTimeout > 60*time.Second {
		return nil, invalid("socket read timeout")
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval > 20*time.Second || c.RPCTimeout <= 0 || c.RPCTimeout > 2*time.Second {
		return nil, invalid("handler timeouts")
	}
	h := &publicHandler{hub: hub, logic: logic, config: c, origins: map[string]bool{}}
	h.config.JWTSecret = append([]byte(nil), c.JWTSecret...)
	for _, origin := range c.AllowedOrigins {
		u, e := url.Parse(origin)
		if e != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, invalid("origin allowlist")
		}
		h.origins[origin] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/wss", h.socket)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hub.done:
			http.Error(w, "stopped", 503)
		default:
			w.Write([]byte("ok\n"))
		}
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !h.ready() {
			http.Error(w, "not ready", 503)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "fable_comet_connections_active %d\nfable_comet_connections_accepted_total %d\nfable_comet_connections_rejected_total %d\nfable_comet_connect_uncertain_total %d\nfable_comet_heartbeat_failures_total %d\n", h.active.Load(), h.accepted.Load(), h.rejected.Load(), h.uncertain.Load(), h.heartbeatFailures.Load())
		hub.writeMetrics(r.Context(), w)
	})
	return mux, nil
}
func (h *publicHandler) ready() bool {
	if h.hub.draining.Load() {
		return false
	}
	select {
	case <-h.hub.done:
		return false
	default:
	}
	return h.config.Ready == nil || h.config.Ready()
}
func (h *publicHandler) rpc(ctx context.Context, safe time.Time) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(h.config.RPCTimeout)
	if !safe.IsZero() && safe.Before(deadline) {
		deadline = safe
	}
	return context.WithDeadline(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+h.config.LogicToken), deadline)
}
func (h *publicHandler) socket(w http.ResponseWriter, r *http.Request) {
	reject := func(code int) { h.rejected.Add(1); http.Error(w, http.StatusText(code), code) }
	if r.Method != http.MethodGet {
		reject(405)
		return
	}
	key, keyErr := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	if !websocket.IsWebSocketUpgrade(r) || r.Header.Get("Sec-WebSocket-Version") != "13" || keyErr != nil || len(key) != 16 {
		reject(400)
		return
	}
	protocol := Protocol(r.URL.Query().Get("protocol"))
	if protocol == "" {
		protocol = ProtocolLegacy
	}
	if protocol != ProtocolLegacy && protocol != ProtocolV2 {
		reject(400)
		return
	}
	if !h.origins[r.Header.Get("Origin")] {
		reject(403)
		return
	}
	raw := r.URL.Query().Get("token")
	if raw == "" {
		raw = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	claims, e := auth.ParseJWT(raw, h.config.JWTSecret)
	if e != nil || !validID(claims.UserUUID, 'U') || !claims.ExpiresAt.Time.After(time.Now().Add(instanceSafetyMargin)) {
		reject(401)
		return
	}
	if !h.ready() {
		reject(503)
		return
	}
	tokenBytes := make([]byte, 32)
	if _, e = rand.Read(tokenBytes); e != nil {
		reject(503)
		return
	}
	id := &pb.ConnectionIdentity{UserId: claims.UserUUID, InstanceId: h.hub.identity.InstanceId, ConnectionId: uuid.NewString(), RouteToken: base64.RawURLEncoding.EncodeToString(tokenBytes)}
	// Cleanup always uses the originally generated full tuple, including when
	// Connect's response was lost. Never retry an uncertain Connect.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = h.hub.Unregister(ctx, id)
		cancel()
		ctx, cancel = h.rpc(context.Background(), time.Time{})
		defer cancel()
		_, _ = h.logic.Disconnect(ctx, &pb.DisconnectRequest{Identity: id})
	}()
	c, e := h.hub.register(r.Context(), id, true)
	if e != nil {
		reject(503)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// A replacement, instance loss or drain closes a pending Connect promptly.
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-c.done:
			cancel()
		case <-watchDone:
		case <-ctx.Done():
		}
	}()
	rpcCtx, stop := h.rpc(ctx, claims.ExpiresAt.Time.Add(-instanceSafetyMargin))
	reply, e := h.logic.Connect(rpcCtx, &pb.ConnectRequest{Comet: h.hub.Identity(), ConnectionId: id.ConnectionId, RouteToken: id.RouteToken, UserJwt: raw})
	stop()
	if e != nil || reply == nil {
		h.uncertain.Add(1)
		reject(503)
		return
	}
	safe, e := routeDeadline(reply.Route, id, claims.ExpiresAt.Time)
	if e != nil || ctx.Err() != nil {
		reject(401)
		return
	}
	if e = h.hub.activate(ctx, c); e != nil {
		reject(503)
		return
	}
	socket, e := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return h.origins[r.Header.Get("Origin")] }, HandshakeTimeout: 2 * time.Second}).Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer socket.Close()
	h.accepted.Add(1)
	h.active.Add(1)
	defer h.active.Add(^uint64(0))
	// JSON escapes may use six wire bytes for one content byte. Bound the
	// wire frame separately from the 16 KiB decoded content contract.
	socket.SetReadLimit(128 * 1024)
	_ = socket.SetReadDeadline(time.Now().Add(h.config.ReadTimeout))
	socket.SetPongHandler(func(string) error { return socket.SetReadDeadline(time.Now().Add(h.config.ReadTimeout)) })
	// Gorilla's default handlers write control frames from the reader. Override
	// application-authored control handler so the writer goroutine remains the
	// sole application writer. ReadMessage may itself emit Gorilla's fatal
	// protocol/read-limit Close via its concurrency-safe WriteControl path.
	socket.SetPingHandler(func(data string) error {
		return queueControl(ctx, c, socketControl{kind: websocket.PongMessage, data: []byte(data)})
	})
	socket.SetCloseHandler(func(int, string) error { return errors.New("peer closed") })
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); _ = c.WriteTo(ctx, socket, protocol); cancel() }()
	heartbeatDone := make(chan struct{})
	go func() { defer close(heartbeatDone); h.heartbeat(ctx, c, claims.ExpiresAt.Time, safe); cancel() }()
	h.read(ctx, c, socket, protocol)
	cancel()
	socket.Close()
	<-writerDone
	<-heartbeatDone
}
func routeDeadline(route *pb.RouteLease, id *pb.ConnectionIdentity, authDeadline time.Time) (time.Time, error) {
	if route == nil || !sameConnection(route.Identity, id) || route.AuthExpiresAt == nil || route.AuthExpiresAt.CheckValid() != nil || route.LeaseDeadline == nil || route.LeaseDeadline.CheckValid() != nil {
		return time.Time{}, ErrInvalid
	}
	safe := route.LeaseDeadline.AsTime()
	if route.AuthExpiresAt.AsTime().Before(safe) {
		safe = route.AuthExpiresAt.AsTime()
	}
	if authDeadline.Before(safe) {
		safe = authDeadline
	}
	safe = safe.Add(-instanceSafetyMargin)
	if !safe.After(time.Now()) {
		return time.Time{}, ErrClosed
	}
	return safe, nil
}
func (h *publicHandler) heartbeat(ctx context.Context, c *Connection, authDeadline, safe time.Time) {
	for {
		remaining := time.Until(safe)
		if remaining <= 0 {
			return
		}
		timer := time.NewTimer(min(h.config.HeartbeatInterval, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-c.done:
			timer.Stop()
			return
		case <-timer.C:
		}
		if !time.Now().Before(safe) {
			return
		}
		if queueControl(ctx, c, socketControl{kind: websocket.PingMessage}) != nil {
			return
		}
		rpcCtx, cancel := h.rpc(ctx, safe)
		reply, e := h.logic.Heartbeat(rpcCtx, &pb.HeartbeatRequest{Identity: c.Identity()})
		cancel()
		if e != nil {
			h.heartbeatFailures.Add(1)
			switch status.Code(e) {
			case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.FailedPrecondition, codes.Unimplemented, codes.Unauthenticated:
				return
			}
			continue
		}
		if reply == nil {
			return
		}
		next, e := routeDeadline(reply.Route, c.identity, authDeadline)
		if e != nil {
			return
		}
		safe = next
	}
}
func queueControl(ctx context.Context, c *Connection, control socketControl) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	case c.controls <- control:
		return nil
	default:
		c.hub.queueFull.Add(1)
		c.markSlow()
		_, _ = c.hub.Unregister(context.Background(), c.identity)
		return ErrClosed
	}
}
func (h *publicHandler) read(ctx context.Context, c *Connection, socket *websocket.Conn, protocol Protocol) {
	for {
		kind, raw, e := socket.ReadMessage()
		if e != nil {
			return
		}
		_ = socket.SetReadDeadline(time.Now().Add(h.config.ReadTimeout))
		if kind != websocket.TextMessage {
			return
		}
		var req *pb.SubmitMessageRequest
		if protocol == ProtocolV2 {
			var f im.SendFrame
			e = strictJSON(raw, &f)
			req = &pb.SubmitMessageRequest{RequestId: uuid.NewString(), Identity: c.Identity(), ClientMessageId: f.ClientMessageID, Destination: &pb.Destination{ReceiveId: f.ReceiveID}, Content: f.Content}
			switch f.ReceiveType {
			case im.ReceiveTypeUser:
				req.Destination.ReceiveType = pb.ReceiveType_RECEIVE_TYPE_USER
			case im.ReceiveTypeGroup:
				req.Destination.ReceiveType = pb.ReceiveType_RECEIVE_TYPE_GROUP
			default:
				e = ErrInvalid
			}
			if f.Event != im.EventSendMessage || !validUUID(f.ClientMessageID) {
				e = ErrInvalid
			}
		} else {
			var f wschat.Message
			e = strictJSON(raw, &f)
			if f.SendID != c.identity.UserId || f.Origin != 0 {
				e = ErrInvalid
			}
			req = &pb.SubmitMessageRequest{RequestId: uuid.NewString(), Identity: c.Identity(), ClientMessageId: uuid.NewString(), Destination: &pb.Destination{ReceiveId: f.ReceiveID}, Content: f.Content}
			switch f.ReceiveType {
			case 0:
				req.Destination.ReceiveType = pb.ReceiveType_RECEIVE_TYPE_USER
			case 1:
				req.Destination.ReceiveType = pb.ReceiveType_RECEIVE_TYPE_GROUP
			default:
				e = ErrInvalid
			}
		}
		if e == nil && (len(req.Content) == 0 || len(req.Content) > 16*1024) {
			e = ErrInvalid
		}
		if e != nil {
			if protocol == ProtocolV2 {
				_ = queueControl(ctx, c, socketControl{frame: im.SendResultFrame{Event: im.EventSendResult, ClientMessageID: req.ClientMessageId, Error: &im.SendError{Code: im.ErrorInvalidArgument, Retry: im.RetryDoNotRetry}}})
			}
			continue
		}
		rpcCtx, cancel := h.rpc(ctx, time.Time{})
		reply, e := h.logic.SubmitMessage(rpcCtx, req)
		cancel()
		if protocol == ProtocolLegacy {
			if terminalSubmitError(e) {
				return
			}
			continue
		}
		frame := im.SendResultFrame{Event: im.EventSendResult, ClientMessageID: req.ClientMessageId}
		if e == nil && reply != nil && reply.Result != nil && reply.Result.Stored && reply.Result.ClientMessageId == req.ClientMessageId && validateMessage(reply.Result.Message) == nil {
			r := reply.Result
			dispatch := map[pb.DispatchState]im.DispatchState{pb.DispatchState_DISPATCH_STATE_QUEUED: im.DispatchQueued, pb.DispatchState_DISPATCH_STATE_NOT_QUEUED: im.DispatchNotQueued, pb.DispatchState_DISPATCH_STATE_UNKNOWN: im.DispatchUnknown}[r.Dispatch]
			if dispatch != "" {
				frame.Result = &im.SendResult{ClientMessageID: r.ClientMessageId, Stored: true, Message: publicMessage(r.Message), Dispatch: dispatch, Replayed: r.Replayed}
			}
		}
		if frame.Result == nil {
			frame.Error = sendError(e)
		}
		terminal := terminalSubmitError(e)
		if queueControl(ctx, c, socketControl{frame: frame, closeAfter: terminal}) != nil {
			return
		}
		if terminal {
			// Stop reading business input immediately. Give the sole writer a short,
			// bounded chance to flush the terminal result and close this exact socket.
			select {
			case <-ctx.Done():
			case <-c.done:
			case <-time.After(100 * time.Millisecond):
			}
			return
		}
	}
}
func strictJSON(raw []byte, value any) error {
	// encoding/json otherwise silently replaces malformed UTF-8 with U+FFFD,
	// changing a user's text before Logic's decoded-string validation sees it.
	if !utf8.Valid(raw) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(value); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}
func sendError(err error) *im.SendError {
	result := &im.SendError{Code: im.ErrorResultUnknown, Retry: im.RetrySameRequest}
	if err == nil {
		return result
	}
	for _, d := range status.Convert(err).Details() {
		detail, ok := d.(*pb.ErrorDetail)
		if !ok {
			continue
		}
		codes := []im.ErrorCode{"", im.ErrorInvalidArgument, im.ErrorIdempotencyConflict, im.ErrorUnauthenticated, im.ErrorPermissionDenied, im.ErrorSuperseded, im.ErrorLeaseExpired, im.ErrorInstanceNotRegistered, im.ErrorDependencyUnavailable, im.ErrorResourceExhausted, im.ErrorDraining}
		retries := []im.RetryDirective{"", im.RetryDoNotRetry, im.RetrySameRequest, im.RetryReconnect, im.RetryReauthenticate, im.RetryRefreshRoute}
		if int(detail.Reason) > 0 && int(detail.Reason) < len(codes) && int(detail.Retry) > 0 && int(detail.Retry) < len(retries) {
			result.Code = codes[detail.Reason]
			result.Retry = retries[detail.Retry]
			if validRequestID(detail.RequestId) {
				result.RequestID = detail.RequestId
			}
		}
	}
	return result
}

// PermissionDenied can mean ordinary destination membership denial and is not
// terminal. Close only on authoritative authentication/route lease failures.
func terminalSubmitError(err error) bool {
	if status.Code(err) == codes.Unauthenticated {
		return true
	}
	for _, raw := range status.Convert(err).Details() {
		detail, ok := raw.(*pb.ErrorDetail)
		if !ok {
			continue
		}
		switch detail.Reason {
		case pb.ErrorReason_ERROR_REASON_SUPERSEDED, pb.ErrorReason_ERROR_REASON_LEASE_EXPIRED, pb.ErrorReason_ERROR_REASON_INSTANCE_NOT_REGISTERED, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED:
			return true
		}
	}
	return false
}
