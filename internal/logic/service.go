// Package logic owns business orchestration and authenticated internal RPC.
package logic

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "mychat/api/gen/fable/v1"
	appauth "mychat/internal/auth"
	"mychat/internal/dto/im"
	"mychat/internal/logic/kafkapush"
	"mychat/internal/logic/messagestore"
	"mychat/internal/logic/routing"
)

type MessageStore interface {
	Save(context.Context, messagestore.Input) (messagestore.Saved, error)
	SaveAI(context.Context, string, string, string, string) (messagestore.Saved, error)
	Sync(context.Context, string, string, string, int) (im.SyncPage, error)
}
type Publisher interface {
	Publish(context.Context, string, []byte) error
}

var ErrNotPublished = kafkapush.ErrNotPublished

type CometGrant struct {
	Identity   *pb.CometIdentity `json:"identity"`
	RPCAddress string            `json:"rpc_address"`
	Token      string            `json:"token"`
}
type Config struct {
	Routes              *routing.Store
	Messages            MessageStore
	Publisher           Publisher
	JWTSecret           []byte
	JobToken, KickToken string
	Comets              []CometGrant
	UserExists          func(context.Context, string) (bool, error)
	DialComet           func(context.Context, string) (pb.CometServiceClient, error)
}
type kickTask struct{ target *pb.ConnectionIdentity }
type Service struct {
	pb.UnimplementedLogicServiceServer
	routes              *routing.Store
	messages            MessageStore
	publisher           Publisher
	jwtKey              []byte
	jobToken, kickToken string
	grants              []CometGrant
	userExists          func(context.Context, string) (bool, error)
	dial                func(context.Context, string) (pb.CometServiceClient, error)
	pool                *cometPool
	kicks               chan kickTask
	cancel              context.CancelFunc
	workers             sync.WaitGroup
	closeOnce           sync.Once
}

func NewService(ctx context.Context, c Config) (*Service, error) {
	if c.Routes == nil || c.Messages == nil || c.Publisher == nil || len(c.JWTSecret) < 32 || !validToken(c.JobToken) || !validToken(c.KickToken) || c.JobToken == c.KickToken || len(c.Comets) == 0 || c.UserExists == nil {
		return nil, errors.New("invalid Logic configuration")
	}
	grants := make([]CometGrant, 0, len(c.Comets))
	tokens := map[string]bool{c.JobToken: true, c.KickToken: true}
	ids := map[string]bool{}
	for _, g := range c.Comets {
		if !validComet(g.Identity) || !validToken(g.Token) || tokens[g.Token] || ids[g.Identity.InstanceId] || !localAddress(g.RPCAddress) {
			return nil, errors.New("invalid Comet authorization grant")
		}
		tokens[g.Token] = true
		ids[g.Identity.InstanceId] = true
		g.Identity = &pb.CometIdentity{InstanceId: g.Identity.InstanceId, BootId: g.Identity.BootId}
		grants = append(grants, g)
	}
	lifetime, cancel := context.WithCancel(ctx)
	s := &Service{routes: c.Routes, messages: c.Messages, publisher: c.Publisher, jwtKey: append([]byte(nil), c.JWTSecret...), jobToken: c.JobToken, kickToken: c.KickToken, grants: grants, userExists: c.UserExists, kicks: make(chan kickTask, 128), cancel: cancel, dial: c.DialComet}
	if s.dial == nil {
		s.pool = newCometPool()
		s.dial = s.pool.client
	}
	for n := 0; n < 4; n++ {
		s.workers.Add(1)
		go s.kickWorker(lifetime)
	}
	return s, nil
}
func (s *Service) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.workers.Wait()
		if s.pool != nil {
			s.pool.close()
		}
	})
}
func validToken(s string) bool {
	if len(s) < 32 || len(s) > 256 {
		return false
	}
	for _, r := range s {
		if r <= 32 || r > 126 {
			return false
		}
	}
	return true
}
func validUUID(s string) bool {
	return len(s) == 36 && s != uuid.Nil.String() && uuid.Validate(s) == nil
}
func validID(s string, p byte) bool { return len(s) == 37 && s[0] == p && validUUID(s[1:]) }
func validInstance(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}
func validComet(i *pb.CometIdentity) bool {
	return i != nil && validInstance(i.InstanceId) && validUUID(i.BootId)
}
func validRequestID(s string) bool {
	if len(s) > 128 {
		return false
	}
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}
func localAddress(address string) bool {
	host, port, e := net.SplitHostPort(address)
	n, err := strconv.Atoi(port)
	ip := net.ParseIP(host)
	return e == nil && err == nil && n > 0 && n <= 65535 && (host == "localhost" || ip != nil && ip.IsLoopback())
}
func bearer(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")
	if len(v) != 1 {
		return ""
	}
	token, ok := strings.CutPrefix(v[0], "Bearer ")
	if !ok {
		return ""
	}
	return token
}
func eq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
func (s *Service) cometAuth(ctx context.Context, id *pb.CometIdentity, instance string) (CometGrant, error) {
	token := bearer(ctx)
	for _, g := range s.grants {
		if eq(token, g.Token) {
			if id != nil {
				if id.InstanceId != g.Identity.InstanceId || id.BootId != g.Identity.BootId {
					return CometGrant{}, rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
				}
			} else if instance != g.Identity.InstanceId {
				return CometGrant{}, rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
			}
			return g, nil
		}
	}
	if eq(token, s.jobToken) || eq(token, s.kickToken) {
		return CometGrant{}, rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
	}
	return CometGrant{}, rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, "")
}
func (s *Service) jobAuth(ctx context.Context) error {
	token := bearer(ctx)
	if eq(token, s.jobToken) {
		return nil
	}
	for _, g := range s.grants {
		if eq(token, g.Token) {
			return rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
		}
	}
	if eq(token, s.kickToken) {
		return rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
	}
	return rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, "")
}
func requestContext(ctx context.Context, send bool) (context.Context, context.CancelFunc) {
	d := 2 * time.Second
	if send {
		d = 10 * time.Second
	}
	return context.WithTimeout(ctx, d)
}
func rpcError(c codes.Code, r pb.ErrorReason, retry pb.RetryDirective, id string) error {
	st := status.New(c, c.String())
	d, e := st.WithDetails(&pb.ErrorDetail{Reason: r, Retry: retry, RequestId: id})
	if e != nil {
		return st.Err()
	}
	return d.Err()
}
func mapError(err error, id string) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, routing.ErrInvalid), errors.Is(err, messagestore.ErrInvalid):
		return rpcError(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, id)
	case errors.Is(err, messagestore.ErrForbidden):
		return rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, id)
	case errors.Is(err, messagestore.ErrConflict), errors.Is(err, routing.ErrConflict):
		return rpcError(codes.AlreadyExists, pb.ErrorReason_ERROR_REASON_IDEMPOTENCY_CONFLICT, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, id)
	case errors.Is(err, routing.ErrNotRegistered):
		return rpcError(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_INSTANCE_NOT_REGISTERED, pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE, id)
	case errors.Is(err, routing.ErrSuperseded):
		return rpcError(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_SUPERSEDED, pb.RetryDirective_RETRY_DIRECTIVE_RECONNECT, id)
	case errors.Is(err, routing.ErrLeaseExpired):
		return rpcError(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_LEASE_EXPIRED, pb.RetryDirective_RETRY_DIRECTIVE_RECONNECT, id)
	case errors.Is(err, routing.ErrDraining):
		return rpcError(codes.Unavailable, pb.ErrorReason_ERROR_REASON_DRAINING, pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE, id)
	default:
		return rpcError(codes.Unavailable, pb.ErrorReason_ERROR_REASON_DEPENDENCY_UNAVAILABLE, pb.RetryDirective_RETRY_DIRECTIVE_SAME_REQUEST, id)
	}
}
func invalidRequest(id string) error { return mapError(routing.ErrInvalid, id) }
func (s *Service) RegisterComet(ctx context.Context, r *pb.RegisterCometRequest) (*pb.RegisterCometResponse, error) {
	if r == nil {
		return nil, invalidRequest("")
	}
	grant, e := s.cometAuth(ctx, r.Identity, "")
	if e != nil {
		return nil, e
	}
	if !validRequestID(r.RequestId) || r.RpcAddress != grant.RPCAddress {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	lease, e := s.routes.Register(ctx, r.Identity, r.RpcAddress)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.RegisterCometResponse{Lease: lease}, nil
}
func (s *Service) RenewComet(ctx context.Context, r *pb.RenewCometRequest) (*pb.RenewCometResponse, error) {
	if r == nil {
		return nil, invalidRequest("")
	}
	if _, e := s.cometAuth(ctx, r.Identity, ""); e != nil {
		return nil, e
	}
	if !validRequestID(r.RequestId) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	lease, e := s.routes.Renew(ctx, r.Identity, r.Draining)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.RenewCometResponse{Lease: lease}, nil
}
func (s *Service) UnregisterComet(ctx context.Context, r *pb.UnregisterCometRequest) (*pb.UnregisterCometResponse, error) {
	if r == nil {
		return nil, invalidRequest("")
	}
	if _, e := s.cometAuth(ctx, r.Identity, ""); e != nil {
		return nil, e
	}
	if !validRequestID(r.RequestId) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	out, e := s.routes.Unregister(ctx, r.Identity)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.UnregisterCometResponse{Outcome: out}, nil
}
func (s *Service) Connect(ctx context.Context, r *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	if r == nil {
		return nil, invalidRequest("")
	}
	if _, e := s.cometAuth(ctx, r.Comet, ""); e != nil {
		return nil, e
	}
	if !validRequestID(r.RequestId) || len(r.UserJwt) > 8192 {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	claims, e := appauth.ParseJWT(r.UserJwt, s.jwtKey)
	if e != nil || !validID(claims.UserUUID, 'U') {
		return nil, rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, r.RequestId)
	}
	exists, e := s.userExists(ctx, claims.UserUUID)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	if !exists {
		return nil, rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, r.RequestId)
	}
	route, old, e := s.routes.Connect(ctx, r.Comet, claims.UserUUID, r.ConnectionId, r.RouteToken, claims.ExpiresAt.Time)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	if old != nil && !sameConnection(old.Identity, route.Identity) {
		select {
		case s.kicks <- kickTask{target: old.Identity}:
		default:
		}
	}
	return &pb.ConnectResponse{Route: route, ReplacedRoute: old}, nil
}
func (s *Service) connectionAuth(ctx context.Context, id *pb.ConnectionIdentity) (CometGrant, error) {
	if id == nil {
		return CometGrant{}, invalidRequest("")
	}
	g, e := s.cometAuth(ctx, nil, id.InstanceId)
	if e != nil {
		return g, e
	}
	leases, e := s.routes.Resolve(ctx, []string{id.InstanceId})
	if e != nil {
		return g, mapError(e, "")
	}
	if len(leases) != 1 || leases[0].GetLease() == nil || leases[0].GetLease().Identity.BootId != g.Identity.BootId {
		return g, mapError(routing.ErrNotRegistered, "")
	}
	return g, nil
}
func (s *Service) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if r == nil || !validRequestID(r.RequestId) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	if _, e := s.connectionAuth(ctx, r.Identity); e != nil {
		return nil, e
	}
	lease, e := s.routes.Heartbeat(ctx, r.Identity)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.HeartbeatResponse{Route: lease}, nil
}
func (s *Service) Disconnect(ctx context.Context, r *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	if r == nil || !validRequestID(r.RequestId) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	if r.Identity == nil {
		return nil, invalidRequest("")
	}
	if _, e := s.cometAuth(ctx, nil, r.Identity.InstanceId); e != nil {
		return nil, e
	}
	out, e := s.routes.Disconnect(ctx, r.Identity)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.DisconnectResponse{Outcome: out}, nil
}
func (s *Service) SubmitMessage(ctx context.Context, r *pb.SubmitMessageRequest) (*pb.SubmitMessageResponse, error) {
	if r == nil || !validRequestID(r.RequestId) || !validUUID(r.ClientMessageId) || r.Content == "" || len(r.Content) > 16384 || !utf8.ValidString(r.Content) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, true)
	defer cancel()
	if _, e := s.connectionAuth(ctx, r.Identity); e != nil {
		return nil, e
	}
	if _, e := s.routes.Check(ctx, r.Identity); e != nil {
		return nil, mapError(e, r.RequestId)
	}
	saved, e := s.messages.Save(ctx, messagestore.Input{SenderID: r.Identity.UserId, ClientMessageID: r.ClientMessageId, Destination: r.Destination, Content: r.Content})
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.SubmitMessageResponse{Result: s.dispatch(ctx, r.ClientMessageId, saved, r.RequestId)}, nil
}
func (s *Service) LookupRoutes(ctx context.Context, r *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
	if e := s.jobAuth(ctx); e != nil {
		return nil, e
	}
	if r == nil || !validRequestID(r.RequestId) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	out, e := s.routes.Lookup(ctx, r.UserIds)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.LookupRoutesResponse{Results: out}, nil
}
func (s *Service) ResolveComets(ctx context.Context, r *pb.ResolveCometsRequest) (*pb.ResolveCometsResponse, error) {
	if e := s.jobAuth(ctx); e != nil {
		return nil, e
	}
	if r == nil || !validRequestID(r.RequestId) {
		return nil, invalidRequest("")
	}
	ctx, cancel := requestContext(ctx, false)
	defer cancel()
	out, e := s.routes.Resolve(ctx, r.InstanceIds)
	if e != nil {
		return nil, mapError(e, r.RequestId)
	}
	return &pb.ResolveCometsResponse{Results: out}, nil
}
func (s *Service) SendAI(ctx context.Context, user, session, clientID, content string) (*pb.SendResult, error) {
	ctx, cancel := requestContext(ctx, true)
	defer cancel()
	if e := s.routes.Ping(ctx); e != nil {
		return nil, e
	}
	if clientID == "" {
		clientID = uuid.NewString()
	}
	saved, e := s.messages.SaveAI(ctx, user, session, clientID, content)
	if e != nil {
		return nil, e
	}
	return s.dispatch(ctx, clientID, saved, ""), nil
}

func sameConnection(a, b *pb.ConnectionIdentity) bool {
	return a != nil && b != nil && a.UserId == b.UserId && a.InstanceId == b.InstanceId && a.ConnectionId == b.ConnectionId && eq(a.RouteToken, b.RouteToken)
}
