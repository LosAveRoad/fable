package im

type EventType string

const (
	EventSendMessage EventType = "send_message"
	EventMessage     EventType = "message"
	EventSendResult  EventType = "send_result"
)

// SendFrame is v2 only. Sender comes from authenticated connection state.
// Legacy send_id/receive_type integers remain in dto/wschat, not this type.
type SendFrame struct {
	Event           EventType   `json:"event"`
	ClientMessageID string      `json:"client_message_id"`
	ReceiveID       string      `json:"receive_id"`
	ReceiveType     ReceiveType `json:"receive_type"`
	Content         string      `json:"content"`
}

type MessageFrame struct {
	Event   EventType `json:"event"`
	Message Message   `json:"message"`
}

// Exactly one of Result or Error must be present. Declarations alone do not
// validate this invariant; future boundary adapters must enforce the spec.
type SendResultFrame struct {
	Event           EventType   `json:"event"`
	ClientMessageID string      `json:"client_message_id"`
	Result          *SendResult `json:"result,omitempty"`
	Error           *SendError  `json:"error,omitempty"`
}

// Code/Retry use the documented string mapping, never raw gRPC integer codes.
type SendError struct {
	Code      ErrorCode      `json:"code"`
	Retry     RetryDirective `json:"retry"`
	RequestID string         `json:"request_id,omitempty"`
}

type ErrorCode string

const (
	ErrorInvalidArgument       ErrorCode = "invalid_argument"
	ErrorIdempotencyConflict   ErrorCode = "idempotency_conflict"
	ErrorUnauthenticated       ErrorCode = "unauthenticated"
	ErrorPermissionDenied      ErrorCode = "permission_denied"
	ErrorSuperseded            ErrorCode = "superseded"
	ErrorLeaseExpired          ErrorCode = "lease_expired"
	ErrorInstanceNotRegistered ErrorCode = "instance_not_registered"
	ErrorDependencyUnavailable ErrorCode = "dependency_unavailable"
	ErrorResourceExhausted     ErrorCode = "resource_exhausted"
	ErrorDraining              ErrorCode = "draining"
	// Transport errors can have no ErrorDetail and an uncertain persistence result.
	ErrorResultUnknown ErrorCode = "result_unknown"
)

type RetryDirective string

const (
	RetryDoNotRetry     RetryDirective = "do_not_retry"
	RetrySameRequest    RetryDirective = "same_request"
	RetryReconnect      RetryDirective = "reconnect"
	RetryReauthenticate RetryDirective = "reauthenticate"
	RetryRefreshRoute   RetryDirective = "refresh_route"
)
