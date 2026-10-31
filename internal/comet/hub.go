// Package comet owns local connection delivery only. Business persistence,
// authorization and cross-instance discovery belong to Logic and Job.
package comet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	pb "mychat/api/gen/fable/v1"

	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed   = errors.New("comet hub closed")
	ErrInvalid  = errors.New("invalid comet input")
	ErrDraining = errors.New("comet draining")
)

type Connection struct {
	identity *pb.ConnectionIdentity
	outbound chan *pb.MessageView
	done     chan struct{}
	hub      *Hub
	writer   atomic.Bool
}

func (c *Connection) Identity() *pb.ConnectionIdentity {
	return proto.Clone(c.identity).(*pb.ConnectionIdentity)
}
func (c *Connection) Messages() <-chan *pb.MessageView { return c.outbound }
func (c *Connection) Done() <-chan struct{}            { return c.done }

// Only the owner loop closes done. Outbound is never closed while it can send.
func (c *Connection) close() { close(c.done) }

type Hub struct {
	identity      *pb.CometIdentity
	queueSize     int
	commands      chan command
	done          chan struct{}
	stopped       chan struct{}
	once          sync.Once
	instanceOwned atomic.Bool
	draining      atomic.Bool
	drainSignal   chan struct{}
}

type command struct {
	ctx context.Context
	run func(*hubState)
}
type hubState struct {
	clients  map[string]*Connection
	draining bool
	deadline time.Time
	timer    *time.Timer
	timerC   <-chan time.Time
}

type DrainState struct {
	Deadline          time.Time
	AlreadyDraining   bool
	ActiveConnections uint64
}

func NewHub(identity *pb.CometIdentity, queueSize int) (*Hub, error) {
	if err := validateComet(identity); err != nil {
		return nil, err
	}
	if queueSize <= 0 {
		queueSize = 256
	}
	h := &Hub{identity: proto.Clone(identity).(*pb.CometIdentity), queueSize: queueSize, commands: make(chan command), done: make(chan struct{}), stopped: make(chan struct{}), drainSignal: make(chan struct{}, 1)}
	go h.run()
	return h, nil
}

func (h *Hub) Identity() *pb.CometIdentity { return proto.Clone(h.identity).(*pb.CometIdentity) }

func (h *Hub) run() {
	s := &hubState{clients: make(map[string]*Connection)}
	defer close(h.stopped)
	defer func() {
		if s.timer != nil {
			s.timer.Stop()
		}
		s.closeAll()
	}()
	for {
		select {
		case <-h.done:
			return
		case <-s.timerC:
			s.closeAll()
			s.timerC = nil
		case cmd := <-h.commands:
			if cmd.ctx.Err() != nil {
				continue
			}
			select {
			case <-h.done:
				return
			default:
			}
			cmd.run(s)
		}
	}
}

func (s *hubState) closeAll() {
	for key, c := range s.clients {
		c.close()
		delete(s.clients, key)
	}
}

// A caller cancelled after admission may not know the effect. A buffered result
// lets the owner finish without waiting for that caller to receive a response.
func call[T any](ctx context.Context, h *Hub, op func(*hubState) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	type result struct {
		value T
		err   error
	}
	reply := make(chan result, 1)
	cmd := command{ctx: ctx, run: func(s *hubState) { v, err := op(s); reply <- result{v, err} }}
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-h.done:
		return zero, ErrClosed
	case h.commands <- cmd:
	}
	select {
	case r := <-reply:
		return r.value, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-h.stopped:
		return zero, ErrClosed
	}
}

// Register accepts a connection already authenticated/registered by its owner.
// It is an in-process boundary, not an authentication or public upgrade API.
func (h *Hub) Register(ctx context.Context, id *pb.ConnectionIdentity) (*Connection, error) {
	if err := validateConnection(id, h.identity.InstanceId); err != nil {
		return nil, err
	}
	id = proto.Clone(id).(*pb.ConnectionIdentity)
	return call(ctx, h, func(s *hubState) (*Connection, error) {
		if s.draining {
			return nil, ErrDraining
		}
		if old := s.clients[id.UserId]; old != nil {
			if sameConnection(old.identity, id) {
				return old, nil
			}
			old.close()
		}
		c := &Connection{identity: id, outbound: make(chan *pb.MessageView, h.queueSize), done: make(chan struct{}), hub: h}
		s.clients[id.UserId] = c
		return c, nil
	})
}

func (h *Hub) Unregister(ctx context.Context, id *pb.ConnectionIdentity) (pb.ConditionalDeleteOutcome, error) {
	if err := validateConnection(id, h.identity.InstanceId); err != nil {
		return 0, err
	}
	id = proto.Clone(id).(*pb.ConnectionIdentity)
	return call(ctx, h, func(s *hubState) (pb.ConditionalDeleteOutcome, error) {
		c := s.clients[id.UserId]
		if c == nil {
			return pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_FOUND, nil
		}
		if !sameConnection(c.identity, id) {
			return pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT, nil
		}
		delete(s.clients, id.UserId)
		c.close()
		return pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED, nil
	})
}

func (h *Hub) Deliver(ctx context.Context, taskID string, message *pb.MessageView, targets []*pb.ConnectionIdentity) ([]*pb.DeliveryResult, error) {
	if err := validateDelivery(taskID, message, targets, h.identity.InstanceId); err != nil {
		return nil, err
	}
	message = proto.Clone(message).(*pb.MessageView)
	owned := make([]*pb.ConnectionIdentity, len(targets))
	for i, id := range targets {
		owned[i] = proto.Clone(id).(*pb.ConnectionIdentity)
	}
	return call(ctx, h, func(s *hubState) ([]*pb.DeliveryResult, error) {
		results := make([]*pb.DeliveryResult, 0, len(owned))
		for _, id := range owned {
			outcome := pb.DeliveryOutcome_DELIVERY_OUTCOME_NOT_FOUND
			c := s.clients[id.UserId]
			switch {
			case s.draining && !time.Now().Before(s.deadline):
				outcome = pb.DeliveryOutcome_DELIVERY_OUTCOME_DRAINING
			case c == nil:
			case !sameConnection(c.identity, id):
				outcome = pb.DeliveryOutcome_DELIVERY_OUTCOME_STALE
			default:
				select {
				case c.outbound <- proto.Clone(message).(*pb.MessageView):
					outcome = pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED
				default:
					outcome = pb.DeliveryOutcome_DELIVERY_OUTCOME_QUEUE_FULL
					delete(s.clients, id.UserId)
					c.close()
				}
			}
			results = append(results, &pb.DeliveryResult{Target: id, Outcome: outcome})
		}
		return results, nil
	})
}

func (h *Hub) Drain(ctx context.Context, deadline time.Time) (DrainState, error) {
	now := time.Now()
	if !deadline.After(now) || deadline.After(now.Add(30*time.Second)) {
		return DrainState{}, invalid("drain deadline")
	}
	return call(ctx, h, func(s *hubState) (DrainState, error) {
		already := s.draining
		if !already || deadline.Before(s.deadline) {
			s.deadline = deadline
			if s.timer == nil {
				s.timer = time.NewTimer(time.Until(deadline))
			} else {
				s.timer.Reset(time.Until(deadline))
			}
			s.timerC = s.timer.C
		}
		s.draining = true
		h.draining.Store(true)
		select {
		case h.drainSignal <- struct{}{}:
		default:
		}
		return DrainState{Deadline: s.deadline, AlreadyDraining: already, ActiveConnections: uint64(len(s.clients))}, nil
	})
}

func (h *Hub) Close() { h.once.Do(func() { close(h.done) }); <-h.stopped }
