package app

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// kafkaNetwork closes every owned socket on process cancellation. kafka-go's
// generation coordinator otherwise waits for a JoinGroup read deadline (which
// includes the rebalance timeout), even after ConsumerGroup.Close is called.
type kafkaNetwork struct {
	mu          sync.Mutex
	connections map[*ownedConn]struct{}
	closed      bool
	ctx         context.Context
	stop        func() bool
}
type ownedConn struct {
	net.Conn
	owner *kafkaNetwork
	once  sync.Once
}

func (c *ownedConn) Close() error {
	var err error
	c.once.Do(func() { err = c.Conn.Close(); c.owner.mu.Lock(); delete(c.owner.connections, c); c.owner.mu.Unlock() })
	return err
}
func newKafkaNetwork(ctx context.Context) *kafkaNetwork {
	n := &kafkaNetwork{ctx: ctx, connections: map[*ownedConn]struct{}{}}
	n.stop = context.AfterFunc(ctx, n.closeSockets)
	return n
}
func (n *kafkaNetwork) dial(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(n.ctx, cancel)
	defer stop()
	raw, err := (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	c := &ownedConn{Conn: raw, owner: n}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		_ = raw.Close()
		return nil, net.ErrClosed
	}
	n.connections[c] = struct{}{}
	n.mu.Unlock()
	return c, nil
}
func (n *kafkaNetwork) Dialer() *kafka.Dialer {
	return &kafka.Dialer{Timeout: 2 * time.Second, DialFunc: n.dial}
}
func (n *kafkaNetwork) closeSockets() {
	n.mu.Lock()
	n.closed = true
	connections := make([]*ownedConn, 0, len(n.connections))
	for c := range n.connections {
		connections = append(connections, c)
	}
	n.mu.Unlock()
	for _, c := range connections {
		_ = c.Close()
	}
}
func (n *kafkaNetwork) Close() { n.stop(); n.closeSockets() }
