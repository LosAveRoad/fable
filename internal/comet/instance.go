package comet

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "mychat/api/gen/fable/v1"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const instanceSafetyMargin = 5 * time.Second

var (
	ErrInstanceStopped   = errors.New("comet instance stopped")
	ErrInstanceLeaseLost = errors.New("comet instance lease lost")
)

// InstanceLogicClient is the Comet instance-authorized subset of Logic RPC.
// Transport security, connection pooling and closing remain caller-owned.
type InstanceLogicClient interface {
	RegisterComet(context.Context, *pb.RegisterCometRequest, ...grpc.CallOption) (*pb.RegisterCometResponse, error)
	RenewComet(context.Context, *pb.RenewCometRequest, ...grpc.CallOption) (*pb.RenewCometResponse, error)
	UnregisterComet(context.Context, *pb.UnregisterCometRequest, ...grpc.CallOption) (*pb.UnregisterCometResponse, error)
}
type InstanceOptions struct{ RenewInterval, RPCTimeout time.Duration }

// Instance owns the Hub after a valid StartInstance attempt. Lease loss closes
// it permanently; recovery requires a new Hub and boot identity, not re-register.
type Instance struct {
	hub                  *Hub
	logic                InstanceLogicClient
	address, token       string
	interval, rpcTimeout time.Duration
	cancel               context.CancelFunc
	done                 chan struct{}
	mu                   sync.RWMutex
	safeDeadline         time.Time
	ready                bool
	err                  error
}

// StartInstance returns only after authenticated registration. Once configuration
// is accepted it takes Hub ownership even if registration fails. Uncertain
// registration is conditionally cleaned with the original instance/boot.
// Caller ctx controls lifetime, not only startup. No listener is opened here.
func StartInstance(ctx context.Context, hub *Hub, logic InstanceLogicClient, address, token string, options InstanceOptions) (*Instance, error) {
	if hub == nil || logic == nil || !instanceAddress(address) || !instanceToken(token) {
		return nil, invalid("instance configuration")
	}
	if options.RenewInterval == 0 {
		options.RenewInterval = 10 * time.Second
	}
	if options.RPCTimeout == 0 {
		options.RPCTimeout = 2 * time.Second
	}
	if options.RenewInterval <= 0 || options.RenewInterval > 10*time.Second || options.RPCTimeout <= 0 || options.RPCTimeout > 2*time.Second {
		return nil, invalid("instance timeouts")
	}
	if !hub.instanceOwned.CompareAndSwap(false, true) {
		return nil, invalid("instance already owned")
	}
	lifetime, cancel := context.WithCancel(ctx)
	i := &Instance{hub: hub, logic: logic, address: address, token: token, interval: options.RenewInterval, rpcTimeout: options.RPCTimeout, cancel: cancel, done: make(chan struct{})}
	rpcCtx, stop := i.rpcContext(lifetime, time.Time{})
	reply, err := logic.RegisterComet(rpcCtx, &pb.RegisterCometRequest{Identity: hub.Identity(), RpcAddress: address})
	stop()
	var safe time.Time
	if err == nil && reply != nil {
		safe, err = i.validateLease(reply.Lease, false)
	} else {
		err = ErrInstanceLeaseLost
	}
	if err != nil || lifetime.Err() != nil || hub.draining.Load() {
		cancel()
		hub.Close()
		i.unregister()
		return nil, ErrInstanceLeaseLost
	}
	select {
	case <-hub.done:
		cancel()
		i.unregister()
		return nil, ErrInstanceLeaseLost
	default:
	}
	i.safeDeadline = safe
	i.ready = true
	go i.run(lifetime)
	return i, nil
}
func (i *Instance) Ready() bool {
	i.mu.RLock()
	ready := i.ready && time.Now().Before(i.safeDeadline)
	i.mu.RUnlock()
	if !ready || i.hub.draining.Load() {
		return false
	}
	select {
	case <-i.hub.done:
		return false
	case <-i.done:
		return false
	default:
		return true
	}
}
func (i *Instance) Done() <-chan struct{} { return i.done }
func (i *Instance) Err() error            { i.mu.RLock(); defer i.mu.RUnlock(); return i.err }
func (i *Instance) Close()                { i.cancel(); <-i.done }
func (i *Instance) run(ctx context.Context) {
	failure := ErrInstanceStopped
	defer func() {
		i.mu.Lock()
		i.ready = false
		i.err = failure
		i.mu.Unlock()
		i.hub.Close()
		i.unregister()
		close(i.done)
	}()
	for {
		i.mu.RLock()
		safe := i.safeDeadline
		i.mu.RUnlock()
		remaining := time.Until(safe)
		if remaining <= 0 {
			failure = ErrInstanceLeaseLost
			return
		}
		delay := min(i.interval, remaining/2)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-i.hub.done:
			timer.Stop()
			return
		case <-i.hub.drainSignal:
			timer.Stop()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		if !time.Now().Before(safe) {
			failure = ErrInstanceLeaseLost
			return
		}
		draining := i.hub.draining.Load()
		rpcCtx, cancel := i.rpcContext(ctx, safe)
		reply, err := i.logic.RenewComet(rpcCtx, &pb.RenewCometRequest{Identity: i.hub.Identity(), Draining: draining})
		cancel()
		if ctx.Err() != nil {
			return
		}
		if !time.Now().Before(safe) {
			failure = ErrInstanceLeaseLost
			return
		}
		if err != nil {
			i.mu.Lock()
			i.ready = false
			i.mu.Unlock()
			switch status.Code(err) {
			case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.Unimplemented:
				failure = ErrInstanceLeaseLost
				return
			}
			continue
		}
		if reply == nil {
			failure = ErrInstanceLeaseLost
			return
		}
		next, e := i.validateLease(reply.Lease, draining)
		if e != nil {
			failure = ErrInstanceLeaseLost
			return
		}
		i.mu.Lock()
		i.safeDeadline = next
		i.ready = !i.hub.draining.Load()
		i.mu.Unlock()
	}
}
func (i *Instance) validateLease(lease *pb.CometLease, draining bool) (time.Time, error) {
	if lease == nil || !sameComet(lease.Identity, i.hub.identity) || lease.RpcAddress != i.address || lease.Draining != draining || lease.LeaseDeadline == nil || lease.LeaseDeadline.CheckValid() != nil {
		return time.Time{}, ErrInstanceLeaseLost
	}
	safe := lease.LeaseDeadline.AsTime().Add(-instanceSafetyMargin)
	if !safe.After(time.Now()) {
		return time.Time{}, ErrInstanceLeaseLost
	}
	return safe, nil
}
func (i *Instance) rpcContext(ctx context.Context, safe time.Time) (context.Context, context.CancelFunc) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+i.token)
	deadline := time.Now().Add(i.rpcTimeout)
	if !safe.IsZero() && safe.Before(deadline) {
		deadline = safe
	}
	return context.WithDeadline(metadata.NewOutgoingContext(ctx, md), deadline)
}
func (i *Instance) unregister() {
	ctx, cancel := i.rpcContext(context.Background(), time.Time{})
	defer cancel()
	// Exact identity and bounded best effort; TTL remains the crash recovery path.
	_, _ = i.logic.UnregisterComet(ctx, &pb.UnregisterCometRequest{Identity: i.hub.Identity()})
}
func instanceToken(token string) bool {
	if len(token) < 32 || len(token) > 256 {
		return false
	}
	for _, r := range token {
		if r <= 32 || r > 126 {
			return false
		}
	}
	return true
}
func instanceAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	n, e := strconv.Atoi(port)
	return err == nil && e == nil && n > 0 && n <= 65535 && host != "" && !strings.ContainsAny(host, "/@?# \t\r\n\\")
}
