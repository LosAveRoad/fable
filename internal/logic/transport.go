package logic

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	pb "mychat/api/gen/fable/v1"
	"sync"
	"time"
)

type cometPool struct {
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
}

func newCometPool() *cometPool { return &cometPool{connections: map[string]*grpc.ClientConn{}} }
func (p *cometPool) client(ctx context.Context, address string) (pb.CometServiceClient, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.connections[address]
	if c == nil {
		var e error
		c, e = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			return nil, e
		}
		p.connections[address] = c
	}
	return pb.NewCometServiceClient(c), nil
}
func (p *cometPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.connections {
		_ = c.Close()
	}
}
func (s *Service) kickWorker(ctx context.Context) {
	defer s.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.kicks:
			func() {
				rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				resolved, e := s.routes.Resolve(rpcCtx, []string{task.target.InstanceId})
				if e != nil || len(resolved) != 1 {
					return
				}
				lease := resolved[0].GetLease()
				if lease == nil {
					return
				}
				// Only provisioned instance/boot/address tuples may become dial targets.
				allowed := false
				for _, g := range s.grants {
					if g.Identity.InstanceId == lease.Identity.InstanceId && g.Identity.BootId == lease.Identity.BootId && g.RPCAddress == lease.RpcAddress {
						allowed = true
						break
					}
				}
				if !allowed {
					return
				}
				md := metadata.Pairs("authorization", "Bearer "+s.kickToken)
				rpcCtx = metadata.NewOutgoingContext(rpcCtx, md)
				client, e := s.dial(rpcCtx, lease.RpcAddress)
				if e != nil {
					return
				}
				_, _ = client.Kick(rpcCtx, &pb.KickRequest{Comet: lease.Identity, Target: task.target, Reason: pb.KickReason_KICK_REASON_REPLACED})
			}()
		}
	}
}
