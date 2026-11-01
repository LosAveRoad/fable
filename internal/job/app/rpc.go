package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "mychat/api/gen/fable/v1"
)

// Local migration endpoints are loopback only. Logic is the sole authority
// supplying Comet addresses; tasks never supply dial targets. The bounded pool
// lives until generation workers drain, so clients cannot close mid-call.
type rpcPool struct {
	mu      sync.Mutex
	clients map[string]*grpc.ClientConn
	limit   int
	closed  bool
	metrics *rpcMetrics
}

func newRPCPool(limit int) *rpcPool {
	return &rpcPool{clients: map[string]*grpc.ClientConn{}, limit: limit, metrics: &rpcMetrics{}}
}

var errRPC = errors.New("Job RPC endpoint unavailable")

func (p *rpcPool) get(ctx context.Context, address string) (*grpc.ClientConn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !loopback(address, false) {
		return nil, errRPC
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errRPC
	}
	if c := p.clients[address]; c != nil {
		return c, nil
	}
	if len(p.clients) >= p.limit {
		return nil, errRPC
	}
	c, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(p.observeRPC), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256*1024), grpc.MaxCallSendMsgSize(256*1024)))
	if err != nil {
		return nil, errRPC
	}
	p.clients[address] = c
	c.Connect()
	return c, nil
}
func (p *rpcPool) comet(ctx context.Context, address string) (pb.CometServiceClient, error) {
	c, err := p.get(ctx, address)
	if err != nil {
		return nil, err
	}
	return pb.NewCometServiceClient(c), nil
}
func (p *rpcPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, c := range p.clients {
		_ = c.Close()
	}
	p.clients = nil
}

// Only the three RPC methods used by Job can become labels. Endpoints,
// identities, payloads and dependency errors never enter metrics.
type rpcMetrics struct{ count, nanos [3]atomic.Uint64 }

func (p *rpcPool) observeRPC(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	index := -1
	switch method {
	case pb.LogicService_LookupRoutes_FullMethodName:
		index = 0
	case pb.LogicService_ResolveComets_FullMethodName:
		index = 1
	case pb.CometService_Deliver_FullMethodName:
		index = 2
	}
	start := time.Now()
	err := invoke(ctx, method, req, reply, cc, opts...)
	if index >= 0 {
		p.metrics.nanos[index].Add(uint64(time.Since(start)))
		p.metrics.count[index].Add(1)
	}
	return err
}
func (m *rpcMetrics) writeMetrics(w io.Writer) {
	for index, method := range []string{"LookupRoutes", "ResolveComets", "Deliver"} {
		fmt.Fprintf(w, "fable_job_rpc_duration_seconds_sum{method=%q} %.9f\nfable_job_rpc_duration_seconds_count{method=%q} %d\n", method, float64(m.nanos[index].Load())/float64(time.Second), method, m.count[index].Load())
	}
}
