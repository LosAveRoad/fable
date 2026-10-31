package job

import (
	"context"
	"errors"
	"time"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RouteClient is the Job-authorized subset of LogicServiceClient.
type RouteClient interface {
	LookupRoutes(context.Context, *pb.LookupRoutesRequest, ...grpc.CallOption) (*pb.LookupRoutesResponse, error)
	ResolveComets(context.Context, *pb.ResolveCometsRequest, ...grpc.CallOption) (*pb.ResolveCometsResponse, error)
}

// CometDialer selects a client for an address resolved by Logic, never a task URL.
// It must honor ctx. The caller owns transport security, connection pooling and
// closing the returned clients' connections. Process does not spawn dialers.
type CometDialer func(context.Context, string) (pb.CometServiceClient, error)

// Options may shorten the contract defaults. There are always at most 3 rounds;
// each round shares the task deadline and delays never reset that budget.
type Options struct {
	Timeout     time.Duration
	RPCTimeout  time.Duration
	RetryDelays []time.Duration
}

type Processor struct {
	logic               RouteClient
	dial                CometDialer
	token               string
	timeout, rpcTimeout time.Duration
	delays              [2]time.Duration
}

type Outcome string

const (
	Unresolved Outcome = "unresolved"
	Enqueued   Outcome = "enqueued"
	Duplicate  Outcome = "duplicate"
	Offline    Outcome = "offline"
)

// Reason is a safe diagnostic category, never an upstream error description,
// message body, route token or internal address.
type Reason string

const (
	DependencyFailure Reason = "dependency_failure"
	InvalidResponse   Reason = "invalid_response"
	RouteChanged      Reason = "route_changed"
	DeliveryRejected  Reason = "delivery_rejected"
	BudgetExhausted   Reason = "budget_exhausted"
)

type RecipientResult struct {
	UserID  string
	Outcome Outcome
	Reason  Reason
}
type Result struct {
	// Task retains the original stable identity/full recipients for future DLQ.
	Task im.PushTask
	// Recipients follows the original (or replay subset) order. Attempts counts
	// task rounds, including failed lookup/resolution before a Deliver.
	Recipients []RecipientResult
	Attempts   int
}

func (r Result) PendingRecipients() []string {
	ids := make([]string, 0)
	for _, v := range r.Recipients {
		if v.Outcome == Unresolved {
			ids = append(ids, v.UserID)
		}
	}
	return ids
}

var ErrUnresolved = errors.New("push task has unresolved recipients")

func NewProcessor(logic RouteClient, dial CometDialer, jobToken string, options Options) (*Processor, error) {
	if logic == nil || dial == nil || !printable(jobToken, 256) || len(jobToken) < 32 {
		return nil, errors.New("invalid Job RPC dependencies or credential")
	}
	for _, c := range jobToken {
		if c == ' ' {
			return nil, errors.New("invalid Job RPC credential")
		}
	}
	if options.Timeout == 0 {
		options.Timeout = 10 * time.Second
	}
	if options.RPCTimeout == 0 {
		options.RPCTimeout = 2 * time.Second
	}
	if options.Timeout < 0 || options.Timeout > 10*time.Second || options.RPCTimeout < 0 || options.RPCTimeout > 2*time.Second {
		return nil, errors.New("invalid Job timeout")
	}
	delays := [2]time.Duration{200 * time.Millisecond, time.Second}
	if options.RetryDelays != nil {
		if len(options.RetryDelays) != 2 {
			return nil, errors.New("invalid Job retry delays")
		}
		for i, d := range options.RetryDelays {
			if d < 0 || d > delays[i] {
				return nil, errors.New("invalid Job retry delay")
			}
			delays[i] = d
		}
	}
	return &Processor{logic: logic, dial: dial, token: jobToken, timeout: options.Timeout, rpcTimeout: options.RPCTimeout, delays: delays}, nil
}

type recipient struct {
	RecipientResult
	route *pb.ConnectionIdentity
	// Zero for hints (which carry no deadlines); otherwise the known Logic
	// route deadline, already validated not to exceed authentication expiry.
	routeDeadline time.Time
	repair        bool
	stopped       bool
}

// Process never commits an offset or writes DLQ. Even a nil error only means
// terminal online enqueue/offline results, not a client delivery acknowledgement.
// ErrUnresolved means a future consumer must persist pending users to DLQ before
// committing. Cancellation returns partial progress; it does not undo delivery.
func (p *Processor) Process(ctx context.Context, raw []byte) (Result, error) {
	task, err := DecodeTask(raw)
	if err != nil {
		return Result{}, err
	}
	result := Result{Task: task}
	ids := task.Recipients
	if task.PendingRecipients != nil {
		ids = task.PendingRecipients
	}
	states := make([]*recipient, 0, len(ids))
	hints := map[string]*pb.ConnectionIdentity{}
	if task.PendingRecipients == nil {
		for _, h := range task.Targets {
			hints[h.UserID] = connectionView(h)
		}
	}
	for _, id := range ids {
		states = append(states, &recipient{RecipientResult: RecipientResult{UserID: id, Outcome: Unresolved}, route: hints[id]})
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	repaired := false
	for attempt := 0; attempt < 3 && ctx.Err() == nil; attempt++ {
		runnable := false
		for _, s := range states {
			if s.Outcome == Unresolved && !s.stopped && !(s.repair && repaired) {
				runnable = true
			}
		}
		if !runnable {
			break
		}
		if attempt > 0 {
			timer := time.NewTimer(p.delays[attempt-1])
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
			if ctx.Err() != nil {
				break
			}
		}
		result.Attempts++
		p.round(ctx, task, states, &repaired)
	}
	for _, s := range states {
		if s.Outcome == Unresolved && s.Reason == "" {
			s.Reason = BudgetExhausted
		}
		result.Recipients = append(result.Recipients, s.RecipientResult)
	}
	if len(result.PendingRecipients()) > 0 {
		return result, errors.Join(ErrUnresolved, ctx.Err())
	}
	return result, nil
}

func (p *Processor) rpcContext(ctx context.Context) (context.Context, context.CancelFunc) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+p.token)
	return context.WithTimeout(metadata.NewOutgoingContext(ctx, md), p.rpcTimeout)
}
