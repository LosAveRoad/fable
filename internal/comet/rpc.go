package comet

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"

	pb "mychat/api/gen/fable/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RPCCredentials must be provisioned per environment and never logged. These
// credentials authorize callers; the Hub separately fences its own boot ID.
type RPCCredentials struct{ JobToken, LogicToken, AdminToken string }

type RPCService struct {
	pb.UnimplementedCometServiceServer
	hub         *Hub
	credentials RPCCredentials
}

func NewRPCService(hub *Hub, credentials RPCCredentials) (*RPCService, error) {
	if hub == nil {
		return nil, invalid("hub")
	}
	tokens := []string{credentials.JobToken, credentials.LogicToken, credentials.AdminToken}
	for i, token := range tokens {
		if len(token) < 32 || len(token) > 256 {
			return nil, invalid("service credentials")
		}
		for _, r := range token {
			if r <= 32 || r > 126 {
				return nil, invalid("service credentials")
			}
		}
		for j := 0; j < i; j++ {
			if token == tokens[j] {
				return nil, invalid("role credentials must differ")
			}
		}
	}
	return &RPCService{hub: hub, credentials: credentials}, nil
}

// Authorization is inside each handler, so registration without an interceptor
// cannot accidentally expose an anonymous service.
func (s *RPCService) authorize(ctx context.Context, expected string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, "")
	}
	token, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok {
		return rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, "")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1 {
		return nil
	}
	known := subtle.ConstantTimeCompare([]byte(token), []byte(s.credentials.JobToken)) |
		subtle.ConstantTimeCompare([]byte(token), []byte(s.credentials.LogicToken)) |
		subtle.ConstantTimeCompare([]byte(token), []byte(s.credentials.AdminToken))
	if known == 1 {
		return rpcError(codes.PermissionDenied, pb.ErrorReason_ERROR_REASON_PERMISSION_DENIED, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
	}
	return rpcError(codes.Unauthenticated, pb.ErrorReason_ERROR_REASON_UNAUTHENTICATED, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE, "")
}

func (s *RPCService) checkIdentity(id *pb.CometIdentity, requestID string) error {
	if !validRequestID(requestID) || validateComet(id) != nil {
		return rpcError(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, "")
	}
	if !proto.Equal(id, s.hub.identity) {
		return rpcError(codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_INSTANCE_NOT_REGISTERED, pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE, requestID)
	}
	return nil
}

func (s *RPCService) Deliver(ctx context.Context, req *pb.DeliverRequest) (*pb.DeliverResponse, error) {
	if err := s.authorize(ctx, s.credentials.JobToken); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, mapError(ErrInvalid, "")
	}
	if err := s.checkIdentity(req.Comet, req.RequestId); err != nil {
		return nil, err
	}
	results, err := s.hub.Deliver(ctx, req.TaskId, req.Message, req.Targets)
	if err != nil {
		return nil, mapError(err, req.RequestId)
	}
	return &pb.DeliverResponse{TaskId: req.TaskId, MessageId: req.Message.MessageId, Results: results}, nil
}

func (s *RPCService) Kick(ctx context.Context, req *pb.KickRequest) (*pb.KickResponse, error) {
	if err := s.authorize(ctx, s.credentials.LogicToken); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, mapError(ErrInvalid, "")
	}
	if err := s.checkIdentity(req.Comet, req.RequestId); err != nil {
		return nil, err
	}
	if req.Reason != pb.KickReason_KICK_REASON_REPLACED && req.Reason != pb.KickReason_KICK_REASON_AUTH_EXPIRED && req.Reason != pb.KickReason_KICK_REASON_SHUTDOWN {
		return nil, mapError(ErrInvalid, req.RequestId)
	}
	outcome, err := s.hub.Unregister(ctx, req.Target)
	if err != nil {
		return nil, mapError(err, req.RequestId)
	}
	return &pb.KickResponse{Outcome: outcome}, nil
}

func (s *RPCService) Drain(ctx context.Context, req *pb.DrainRequest) (*pb.DrainResponse, error) {
	if err := s.authorize(ctx, s.credentials.AdminToken); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, mapError(ErrInvalid, "")
	}
	if err := s.checkIdentity(req.Comet, req.RequestId); err != nil {
		return nil, err
	}
	if req.Deadline == nil || req.Deadline.CheckValid() != nil {
		return nil, mapError(ErrInvalid, req.RequestId)
	}
	state, err := s.hub.Drain(ctx, req.Deadline.AsTime())
	if err != nil {
		return nil, mapError(err, req.RequestId)
	}
	return &pb.DrainResponse{Deadline: timestamppb.New(state.Deadline), AlreadyDraining: state.AlreadyDraining, ActiveConnections: state.ActiveConnections}, nil
}

func rpcError(code codes.Code, reason pb.ErrorReason, retry pb.RetryDirective, requestID string) error {
	s := status.New(code, code.String())
	withDetail, err := s.WithDetails(&pb.ErrorDetail{Reason: reason, Retry: retry, RequestId: requestID})
	if err != nil {
		return s.Err()
	}
	return withDetail.Err()
}

func mapError(err error, requestID string) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, ErrInvalid):
		return rpcError(codes.InvalidArgument, pb.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, requestID)
	case errors.Is(err, ErrDraining):
		return rpcError(codes.Unavailable, pb.ErrorReason_ERROR_REASON_DRAINING, pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE, requestID)
	default:
		return rpcError(codes.Unavailable, pb.ErrorReason_ERROR_REASON_DEPENDENCY_UNAVAILABLE, pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE, requestID)
	}
}
