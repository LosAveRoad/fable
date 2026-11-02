package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/connectivity"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/job"
)

type partitionProgress struct {
	Offset                   int64
	Lag                      int64
	ProcessedAt, CommittedAt int64
}
type runtimeState struct {
	alive, joined atomic.Bool
	mu            sync.Mutex
	partitions    map[int]partitionProgress
}

func (s *runtimeState) setLag(partition int, lag int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.partitions == nil {
		s.partitions = map[int]partitionProgress{}
	}
	p := s.partitions[partition]
	p.Lag = lag
	s.partitions[partition] = p
}
func (s *runtimeState) progress(partition int, offset int64, commit bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.partitions == nil {
		s.partitions = map[int]partitionProgress{}
	}
	p := s.partitions[partition]
	p.Offset = offset
	if commit {
		p.CommittedAt = time.Now().Unix()
	} else {
		p.ProcessedAt = time.Now().Unix()
	}
	s.partitions[partition] = p
}
func probeHandler(s *runtimeState, c *job.Consumer, dependencies func(context.Context) bool) http.Handler {
	mux := http.NewServeMux()
	probe := func(ready bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			ok := s.alive.Load()
			if ready {
				ok = ok && s.joined.Load() && !c.Blocked()
				if ok {
					ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
					defer cancel()
					ok = dependencies(ctx)
				}
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			if !ok {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("unavailable\n"))
				return
			}
			_, _ = w.Write([]byte("ok\n"))
		}
	}
	mux.HandleFunc("/healthz", probe(false))
	mux.HandleFunc("/readyz", probe(true))
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		v := c.Stats()
		values := []struct {
			name string
			n    uint64
		}{{"processed", v.Processed}, {"committed", v.Committed}, {"dlq", v.DLQ}, {"invalid", v.Invalid}, {"conflicts", v.Conflicts}, {"retries", v.Retries}, {"dlq_errors", v.DLQErrors}, {"commit_errors", v.CommitErrors}, {"offline", v.Offline}, {"enqueued", v.Enqueued}, {"duplicate", v.Duplicate}, {"unresolved", v.Unresolved}}
		for _, v := range values {
			_, _ = fmt.Fprintf(w, "fable_job_%s_total %d\n", v.name, v.n)
		}
		_, _ = fmt.Fprintf(w, "fable_job_recent_identities %d\nfable_job_alive %d\nfable_job_joined %d\n", v.RecentIdentities, boolInt(s.alive.Load()), boolInt(s.joined.Load()))
		s.mu.Lock()
		defer s.mu.Unlock()
		ids := make([]int, 0, len(s.partitions))
		for id := range s.partitions {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			p := s.partitions[id]
			_, _ = fmt.Fprintf(w, "fable_job_partition_lag{partition=\"%d\"} %d\nfable_job_partition_last_processed_seconds{partition=\"%d\"} %d\nfable_job_partition_last_committed_seconds{partition=\"%d\"} %d\n", id, p.Lag, id, p.ProcessedAt, id, p.CommittedAt)
		}
	})
	return mux
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// Run owns the consumer group, DLQ writer, bounded RPC pool and local probes.
// Shutdown cancels generation work and waits before closing any owned clients.
func Run(ctx context.Context, c Config) error {
	if err := validate(c); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pool := newRPCPool(64)
	defer pool.Close()
	logic, err := pool.get(ctx, c.LogicAddress)
	if err != nil {
		return err
	}
	processor, err := job.NewProcessor(pb.NewLogicServiceClient(logic), pool.comet, c.JobToken, job.Options{})
	if err != nil {
		return errConfig
	}
	sink := newSink(c, c.DLQTopic)
	defer sink.Close()
	consumer := job.NewConsumer(processor, sink, job.ConsumerOptions{Observe: correlationObserver(os.Stderr)})
	state := &runtimeState{}
	state.alive.Store(true)
	defer state.alive.Store(false)
	listener, err := net.Listen("tcp", c.HTTPAddress)
	if err != nil {
		return errors.New("Job HTTP listener unavailable")
	}
	defer listener.Close()
	dependencies := func(ctx context.Context) bool {
		return logic.GetState() == connectivity.Ready && sink.ready(ctx, c.Topic, c.DLQTopic)
	}
	server := &http.Server{Handler: probeHandler(state, consumer, dependencies), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	httpDone := make(chan error, 1)
	consumerDone := make(chan error, 1)
	go func() { httpDone <- server.Serve(listener) }()
	go func() {
		err := runConsumer(ctx, c, consumer, state)
		state.alive.Store(false)
		state.joined.Store(false)
		consumerDone <- err
	}()
	var result error
	consumerExited := false
	select {
	case <-ctx.Done():
	case err := <-consumerDone:
		consumerExited = true
		if ctx.Err() == nil {
			if err != nil {
				result = err
			} else {
				result = errors.New("Job consumer stopped unexpectedly")
			}
		}
	case err := <-httpDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = errors.New("Job HTTP server stopped unexpectedly")
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		if result == nil {
			result = errors.New("Job shutdown exceeded grace period")
		}
	}
	if !consumerExited {
		<-consumerDone
	}
	return result
}
