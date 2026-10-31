//go:build integration

package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/job"
)

type fixedPartition int

func (p fixedPartition) Balance(_ kafka.Message, partitions ...int) int { return int(p) }
func testKafkaConfig(t *testing.T) (Config, *kafka.Client) {
	t.Helper()
	broker := os.Getenv("FABLE_LOGIC_TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set FABLE_LOGIC_TEST_KAFKA_BROKER for own UUID-scoped Kafka tests")
	}
	suffix := uuid.NewString()
	c := Config{HTTPAddress: "127.0.0.1:0", LogicAddress: "127.0.0.1:19001", JobToken: strings.Repeat("j", 32), Brokers: []string{broker}, Topic: "fable.push.v1." + suffix, DLQTopic: "fable.push.dlq.v1." + suffix, Group: "fable-push-job-v1." + suffix}
	tr := &kafka.Transport{DialTimeout: 2 * time.Second}
	client := &kafka.Client{Addr: kafka.TCP(broker), Transport: tr, Timeout: 3 * time.Second}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{GroupIDs: []string{c.Group}})
		if _, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{c.Topic, c.DLQTopic}}); err != nil {
			t.Errorf("own topic cleanup: %v", err)
		}
		tr.CloseIdleConnections()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: c.Topic, NumPartitions: 2, ReplicationFactor: 1}, {Topic: c.DLQTopic, NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: []kafka.ConfigEntry{{ConfigName: "retention.ms", ConfigValue: "604800000"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range r.Errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 10*time.Second, func() bool {
		r, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{c.Topic: {kafka.LastOffsetOf(0), kafka.LastOffsetOf(1)}, c.DLQTopic: {kafka.LastOffsetOf(0)}}})
		if err != nil {
			return false
		}
		if len(r.Topics[c.Topic]) != 2 || len(r.Topics[c.DLQTopic]) != 1 {
			return false
		}
		for _, partitions := range r.Topics {
			for _, p := range partitions {
				if p.Error != nil {
					return false
				}
			}
		}
		return true
	})
	return c, client
}
func waitFor(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true within " + d.String())
}
func committed(client *kafka.Client, c Config, partition int) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: c.Group, Topics: map[string][]int{c.Topic: {partition}}})
	if err != nil || r.Error != nil {
		return -1
	}
	for _, p := range r.Topics[c.Topic] {
		if p.Partition == partition && p.Error == nil {
			return p.CommittedOffset
		}
	}
	return -1
}
func writePartition(t *testing.T, c Config, partition int, raw ...[]byte) {
	t.Helper()
	sink := newSink(c, c.Topic)
	defer sink.Close()
	sink.writer.Balancer = fixedPartition(partition)
	for _, b := range raw {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := sink.publish(ctx, []byte("test"), b)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

type runtimeLogic struct {
	pb.UnimplementedLogicServiceServer
	token string
	fail  atomic.Bool
	calls atomic.Int64
}

func (s *runtimeLogic) LookupRoutes(ctx context.Context, r *pb.LookupRoutesRequest) (*pb.LookupRoutesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+s.token {
		return nil, status.Error(codes.Unauthenticated, "role")
	}
	s.calls.Add(1)
	if s.fail.Load() {
		return nil, status.Error(codes.Unavailable, "safe failure")
	}
	out := &pb.LookupRoutesResponse{}
	for _, id := range r.UserIds {
		out.Results = append(out.Results, &pb.RouteLookupResult{UserId: id, State: &pb.RouteLookupResult_Offline{Offline: pb.OfflineReason_OFFLINE_REASON_NO_ROUTE}})
	}
	return out, nil
}
func startRuntime(t *testing.T, c Config) (Config, context.CancelFunc, <-chan error) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.HTTPAddress = l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, c) }()
	return c, cancel, done
}
func stopRuntime(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("runtime did not drain")
	}
}
func ready(address string) bool {
	client := http.Client{Timeout: 3 * time.Second}
	r, e := client.Get("http://" + address + "/readyz")
	if e != nil {
		return false
	}
	defer r.Body.Close()
	return r.StatusCode == 200
}
func TestRealKafkaRuntimeRestartDLQAndPendingReplay(t *testing.T) {
	c, admin := testKafkaConfig(t)
	logic := &runtimeLogic{token: c.JobToken}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterLogicServiceServer(server, logic)
	go server.Serve(l)
	defer server.Stop()
	c.LogicAddress = l.Addr().String()
	c, cancel, done := startRuntime(t, c)
	defer func() {
		if cancel != nil {
			stopRuntime(t, cancel, done)
		}
	}()
	waitFor(t, 15*time.Second, func() bool { return ready(c.HTTPAddress) })
	record := replayFixture(t)
	task := *record.Task
	raw, _ := json.Marshal(task)
	invalid := []byte{'{', 0xff, '}', '\n'}
	writePartition(t, c, 0, raw, invalid)
	waitFor(t, 10*time.Second, func() bool { return committed(admin, c, 0) == 2 })
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: c.DLQTopic, Partition: 0, MinBytes: 1, MaxBytes: 2 * 1024 * 1024})
	defer reader.Close()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	m, err := reader.FetchMessage(ctx)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	dead, err := job.DecodeDeadLetter(m.Value)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := base64.StdEncoding.DecodeString(dead.RawPayloadBase64)
	if !bytes.Equal(b, invalid) || dead.Task != nil || dead.SourceOffset != "1" {
		t.Fatalf("invalid task bytes changed %+v", dead)
	}
	stopRuntime(t, cancel, done)
	cancel = nil
	logic.fail.Store(true)
	c, cancel, done = startRuntime(t, c)
	waitFor(t, 15*time.Second, func() bool { return ready(c.HTTPAddress) })
	writePartition(t, c, 0, raw)
	waitFor(t, 12*time.Second, func() bool { return committed(admin, c, 0) == 3 })
	ctx, stop = context.WithTimeout(context.Background(), 5*time.Second)
	m, err = reader.FetchMessage(ctx)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	dead, err = job.DecodeDeadLetter(m.Value)
	if err != nil || dead.Task == nil || len(dead.PendingRecipients) != 2 {
		t.Fatalf("lost unresolved subset %+v %v", dead, err)
	}
	opts := ReplayOptions{RunID: "test-run", Partition: 0, FromOffset: m.Offset, ToOffset: m.Offset}
	summary, err := Replay(context.Background(), c, opts)
	if err != nil || summary.Published != 0 || summary.Replayable != 1 {
		t.Fatalf("preview %+v %v", summary, err)
	}
	if committed(admin, c, 0) != 3 {
		t.Fatal("preview moved source offset")
	}
	logic.fail.Store(false)
	opts.Execute = true
	summary, err = Replay(context.Background(), c, opts)
	if err != nil || summary.Published != 1 {
		t.Fatalf("execute %+v %v", summary, err)
	}
	waitFor(t, 10*time.Second, func() bool {
		a, b := committed(admin, c, 0), committed(admin, c, 1)
		if a < 0 {
			a = 0
		}
		if b < 0 {
			b = 0
		}
		return a+b == 4
	})
}

type trackingProcessor struct {
	mu      sync.Mutex
	active  map[string]bool
	seen    map[string]int
	overlap bool
}

func (p *trackingProcessor) Process(ctx context.Context, raw []byte) (job.Result, error) {
	task, err := job.DecodeTask(raw)
	if err != nil {
		return job.Result{}, err
	}
	p.mu.Lock()
	if p.active[task.TaskID] {
		p.overlap = true
	}
	p.active[task.TaskID] = true
	p.seen[task.TaskID]++
	p.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Millisecond):
	}
	p.mu.Lock()
	delete(p.active, task.TaskID)
	p.mu.Unlock()
	return job.Result{Task: task}, nil
}
func TestRealKafkaSameGroupOwnershipRebalance(t *testing.T) {
	c, admin := testKafkaConfig(t)
	p := &trackingProcessor{active: map[string]bool{}, seen: map[string]int{}}
	sink := newSink(c, c.DLQTopic)
	defer sink.Close()
	start := func() (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runConsumer(ctx, c, job.NewConsumer(p, sink, job.ConsumerOptions{}), &runtimeState{}) }()
		return cancel, done
	}
	stop1, done1 := start()
	defer stop1()
	stop2, done2 := start()
	defer stop2()
	task := *replayFixture(t).Task
	for i := 0; i < 20; i++ {
		copy := task
		copy.Message.MessageID = "M" + fmt.Sprintf("30000000-0000-4000-8000-%012d", i+1)
		copy.TaskID = "push:v1:" + copy.Message.MessageID + ":0"
		raw, _ := json.Marshal(copy)
		writePartition(t, c, i%2, raw)
	}
	waitFor(t, 20*time.Second, func() bool { return committed(admin, c, 0) == 10 && committed(admin, c, 1) == 10 })
	stop1()
	select {
	case err := <-done1:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("first consumer did not drain")
	}
	for i := 20; i < 24; i++ {
		copy := task
		copy.Message.MessageID = "M" + fmt.Sprintf("30000000-0000-4000-8000-%012d", i+1)
		copy.TaskID = "push:v1:" + copy.Message.MessageID + ":0"
		raw, _ := json.Marshal(copy)
		writePartition(t, c, i%2, raw)
	}
	waitFor(t, 20*time.Second, func() bool { return committed(admin, c, 0) == 12 && committed(admin, c, 1) == 12 })
	stop2()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("second consumer did not drain")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.overlap || len(p.seen) != 24 {
		t.Fatalf("ownership overlap=%v tasks=%d", p.overlap, len(p.seen))
	}
}

func TestRealKafkaDLQFailureStopsBeforeLaterOffset(t *testing.T) {
	c, admin := testKafkaConfig(t)
	bad := c
	bad.DLQTopic += ".missing"
	sink := newSink(bad, bad.DLQTopic)
	defer sink.Close()
	p := &trackingProcessor{active: map[string]bool{}, seen: map[string]int{}}
	consumer := job.NewConsumer(p, sink, job.ConsumerOptions{Attempts: 2, RetryDelay: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runConsumer(ctx, c, consumer, &runtimeState{}) }()
	writePartition(t, c, 0, []byte("bad one"), []byte("bad two"))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("DLQ boundary failure ignored")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("failed loop did not stop")
	}
	if offset := committed(admin, c, 0); offset >= 1 {
		t.Fatalf("skipped failed DLQ offset: %d", offset)
	}
	stats := consumer.Stats()
	if stats.Invalid != 1 || stats.Committed != 0 || stats.DLQErrors != 2 {
		t.Fatalf("processed later task %+v", stats)
	}
	metadata, err := admin.Metadata(context.Background(), &kafka.MetadataRequest{Topics: []string{bad.DLQTopic}})
	if err != nil || len(metadata.Topics) != 1 || metadata.Topics[0].Error == nil {
		t.Fatal("missing DLQ auto-created")
	}
}

type heldProcessor struct {
	started chan struct{}
	once    sync.Once
}

func (p *heldProcessor) Process(ctx context.Context, raw []byte) (job.Result, error) {
	task, err := job.DecodeTask(raw)
	if err != nil {
		return job.Result{}, err
	}
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	return job.Result{Task: task}, ctx.Err()
}
func TestRealKafkaLagMetricRefreshesWhileDeliveryIsBlocked(t *testing.T) {
	c, _ := testKafkaConfig(t)
	processor := &heldProcessor{started: make(chan struct{})}
	sink := newSink(c, c.DLQTopic)
	defer sink.Close()
	consumer := job.NewConsumer(processor, sink, job.ConsumerOptions{})
	state := &runtimeState{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runConsumer(ctx, c, consumer, state) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(8 * time.Second):
			t.Error("consumer did not drain")
		}
	}()
	raw, _ := json.Marshal(replayFixture(t).Task)
	writePartition(t, c, 0, raw)
	select {
	case <-processor.started:
	case <-time.After(15 * time.Second):
		t.Fatal("first task did not begin")
	}
	writePartition(t, c, 0, raw, raw, raw)
	handler := probeHandler(state, consumer, func(context.Context) bool { return true })
	waitFor(t, 4*time.Second, func() bool {
		result := httptest.NewRecorder()
		handler.ServeHTTP(result, httptest.NewRequest("GET", "/metrics", nil))
		return strings.Contains(result.Body.String(), "fable_job_partition_lag{partition=\"0\"} 3\n")
	})
}
