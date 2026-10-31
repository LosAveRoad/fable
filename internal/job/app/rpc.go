package app

import (
	"context"
	"errors"
	"sync"

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
}

func newRPCPool(limit int) *rpcPool {
	return &rpcPool{clients: map[string]*grpc.ClientConn{}, limit: limit}
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
	c, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256*1024), grpc.MaxCallSendMsgSize(256*1024)))
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
