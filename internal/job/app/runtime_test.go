package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mychat/internal/job"
)

func TestProbeDistinguishesHealthyIdleBlockedAndDead(t *testing.T) {
	state := &runtimeState{}
	consumer := job.NewConsumer(nil, nil, job.ConsumerOptions{})
	handler := probeHandler(state, consumer, func(context.Context) bool { return true })
	get := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		return r
	}
	if get("/readyz").Code != http.StatusServiceUnavailable {
		t.Fatal("unstarted ready")
	}
	state.alive.Store(true)
	state.joined.Store(true)
	if get("/readyz").Code != 200 || get("/healthz").Code != 200 {
		t.Fatal("idle falsely unhealthy")
	}
	state.joined.Store(false)
	if get("/readyz").Code != 503 {
		t.Fatal("rebalance still ready")
	}
	state.alive.Store(false)
	if get("/healthz").Code != 503 {
		t.Fatal("dead loop healthy")
	}
	metrics := get("/metrics").Body.String()
	if !strings.Contains(metrics, "fable_job_committed_total 0") || strings.Contains(metrics, "user_id") {
		t.Fatal(metrics)
	}
}
func TestRPCPoolBoundsAddressesAndReusesConnection(t *testing.T) {
	p := newRPCPool(1)
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a, err := p.get(ctx, "127.0.0.1:19001")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.get(ctx, "127.0.0.1:19001")
	if err != nil || a != b {
		t.Fatal("no pooling")
	}
	if _, err := p.get(ctx, "127.0.0.1:19002"); err == nil {
		t.Fatal("pool unbounded")
	}
	if _, err := p.get(ctx, "elsewhere:19002"); err == nil {
		t.Fatal("nonloopback accepted")
	}
}

func TestConsumerCancellationInterruptsCoordinatorIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	cfg := Config{Brokers: []string{listener.Addr().String()}, Topic: "fable.push.v1.test", Group: "fable-push-job-v1.test"}
	go func() {
		done <- runConsumer(ctx, cfg, job.NewConsumer(nil, nil, job.ConsumerOptions{}), &runtimeState{})
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("coordinator not contacted")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancellation left coordinator IO blocked")
	}
}

func TestProgressTimestampFollowsProcessingNotFetching(t *testing.T) {
	state := &runtimeState{}
	state.setLag(1, 7)
	state.mu.Lock()
	p := state.partitions[1]
	state.mu.Unlock()
	if p.ProcessedAt != 0 || p.Lag != 7 {
		t.Fatal("idle lag probe claimed processed task")
	}
	state.progress(1, 9007199254740993, false)
	state.progress(1, 9007199254740993, true)
	state.mu.Lock()
	p = state.partitions[1]
	state.mu.Unlock()
	if p.ProcessedAt == 0 || p.CommittedAt == 0 || p.Offset != 9007199254740993 {
		t.Fatal("missing progress")
	}
}

func TestCorrelationJSONLogsContainSafeFieldsAndStringOffsets(t *testing.T) {
	var output bytes.Buffer
	observe := correlationObserver(&output)
	observe(job.ConsumerEvent{Stage: "commit", Outcome: "acknowledged", RequestID: "request-1", TaskID: "push:v1:message:0", MessageID: "message", SourceTopic: "fable.push.v1", SourcePartition: 1, SourceOffset: "9007199254740993", Attempts: 2, Pending: 1})
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["source_offset"] != "9007199254740993" || entry["request_id"] != "request-1" || entry["stage"] != "commit" || entry["task_id"] != "push:v1:message:0" {
		t.Fatalf("lost structured correlation %+v", entry)
	}
}
