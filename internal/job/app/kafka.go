package app

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"mychat/internal/job"
)

type kafkaSink struct {
	writer    *kafka.Writer
	transport *kafka.Transport
	client    *kafka.Client
	topic     string
}

func newSink(c Config, topic string) *kafkaSink {
	tr := &kafka.Transport{DialTimeout: 2 * time.Second}
	address := kafka.TCP(c.Brokers...)
	return &kafkaSink{topic: topic, transport: tr, client: &kafka.Client{Addr: address, Transport: tr, Timeout: 2 * time.Second}, writer: &kafka.Writer{Addr: address, Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, AllowAutoTopicCreation: false, MaxAttempts: 1, BatchSize: 1, BatchBytes: 2 * 1024 * 1024, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, Transport: tr}}
}

var errKafka = errors.New("Job Kafka operation unavailable")

func (s *kafkaSink) Write(ctx context.Context, r job.DeadLetter) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return errKafka
	}
	return s.publish(ctx, []byte(r.SourceTopic+":"+strconv.FormatInt(int64(r.SourcePartition), 10)+":"+r.SourceOffset), raw)
}
func (s *kafkaSink) publish(ctx context.Context, key, raw []byte) error {
	return s.publishMessage(ctx, kafka.Message{Key: append([]byte(nil), key...), Value: append([]byte(nil), raw...)})
}
func (s *kafkaSink) publishMessage(ctx context.Context, m kafka.Message) error {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if s.writer.WriteMessages(bounded, m) != nil {
		if bounded.Err() != nil {
			return bounded.Err()
		}
		return errKafka
	}
	return nil
}
func (s *kafkaSink) Close() { _ = s.writer.Close(); s.transport.CloseIdleConnections() }
func (s *kafkaSink) ready(ctx context.Context, topics ...string) bool {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r, err := s.client.Metadata(bounded, &kafka.MetadataRequest{Topics: topics})
	if err != nil || len(r.Brokers) == 0 {
		return false
	}
	for _, name := range topics {
		found := false
		for _, t := range r.Topics {
			if t.Name != name {
				continue
			}
			if t.Error != nil || len(t.Partitions) == 0 {
				return false
			}
			for _, p := range t.Partitions {
				if p.Error != nil || p.Leader.Host == "" || len(p.Isr) == 0 {
					return false
				}
			}
			found = true
		}
		if !found {
			return false
		}
	}
	return true
}

type partitionSource struct {
	reader     *kafka.Reader
	generation *kafka.Generation
	topic      string
	partition  int
	state      *runtimeState
}

func (p *partitionSource) Fetch(ctx context.Context) (kafka.Message, error) {
	m, err := p.reader.FetchMessage(ctx)
	if err == nil {
		p.state.setLag(p.generation.ID, p.partition, p.reader.Lag())
	}
	return m, err
}
func (p *partitionSource) Processed(m kafka.Message) {
	p.state.progress(p.generation.ID, p.partition, m.Offset, false)
}
func (p *partitionSource) Commit(ctx context.Context, m kafka.Message) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if m.Topic != p.topic || m.Partition != p.partition || m.Offset < 0 {
		return errKafka
	}
	// CommitOffsets uses the generation ID/member fencing and the group's 2s
	// coordinator timeout. This API deliberately accepts the NEXT offset.
	err := p.generation.CommitOffsets(map[string]map[int]int64{p.topic: {p.partition: m.Offset + 1}})
	if err == nil {
		p.state.progress(p.generation.ID, p.partition, m.Offset, true)
	}
	return err
}
func runConsumer(ctx context.Context, c Config, consumer *job.Consumer, state *runtimeState) error {
	ctx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	network := newKafkaNetwork(ctx)
	defer network.Close()
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{ID: c.Group, Brokers: c.Brokers, Topics: []string{c.Topic}, Dialer: network.Dialer(), StartOffset: kafka.FirstOffset, Timeout: 2 * time.Second, HeartbeatInterval: time.Second, SessionTimeout: 10 * time.Second, RebalanceTimeout: 10 * time.Second, JoinGroupBackoff: 200 * time.Millisecond})
	if err != nil {
		return errKafka
	}
	stopClose := context.AfterFunc(ctx, func() { _ = group.Close() })
	defer stopClose()
	var failed atomic.Bool
	var workers sync.WaitGroup
	defer func() { cancelConsumer(); network.Close(); _ = group.Close(); workers.Wait() }()
	fail := func() {
		failed.Store(true)
		state.joined.Store(false)
		cancelConsumer()
	}
	errorsInRow := 0
	for {
		if ctx.Err() != nil {
			if failed.Load() {
				return errKafka
			}
			return nil
		}
		if failed.Load() {
			return errKafka
		}
		gen, err := group.Next(ctx)
		if err != nil {
			state.joined.Store(false)
			if ctx.Err() != nil {
				if failed.Load() {
					return errKafka
				}
				return nil
			}
			if errors.Is(err, kafka.RebalanceInProgress) {
				continue
			}
			errorsInRow++
			if errorsInRow >= 3 {
				return errKafka
			}
			continue
		}
		if failed.Load() {
			return errKafka
		}
		errorsInRow = 0
		ids := make([]int, 0, len(gen.Assignments[c.Topic]))
		for _, assignment := range gen.Assignments[c.Topic] {
			ids = append(ids, assignment.ID)
		}
		state.assign(gen.ID, ids)
		// A generation without assigned partitions remains a healthy idle member.
		// This watcher also clears readiness immediately when ownership ends.
		gen.Start(func(gctx context.Context) { <-gctx.Done(); state.release(gen.ID) })
		for _, assignment := range gen.Assignments[c.Topic] {
			assignment := assignment
			workers.Add(1)
			gen.Start(func(gctx context.Context) {
				defer workers.Done()
				workerctx, cancel := context.WithCancel(gctx)
				defer cancel()
				stop := context.AfterFunc(ctx, cancel)
				defer stop()
				reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: c.Topic, Partition: assignment.ID, Dialer: network.Dialer(), MinBytes: 1, MaxBytes: 2 * 1024 * 1024, MaxWait: time.Second, QueueCapacity: 1, ReadLagInterval: -1, MaxAttempts: 3})
				defer reader.Close()
				if reader.SetOffset(assignment.Offset) != nil {
					fail()
					return
				}
				source := &partitionSource{reader: reader, generation: gen, topic: c.Topic, partition: assignment.ID, state: state}
				// Lag stays observable while a healthy assigned partition is idle.
				lagDone := make(chan struct{})
				go func() {
					defer close(lagDone)
					ticker := time.NewTicker(time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-workerctx.Done():
							return
						case <-ticker.C:
							// Lag() is cached at the last FetchMessage. Query
							// the current broker watermark while delivery or
							// acknowledgement may still hold this partition.
							bounded, stop := context.WithTimeout(workerctx, 500*time.Millisecond)
							lag, err := reader.ReadLag(bounded)
							stop()
							if err == nil {
								state.setLag(gen.ID, assignment.ID, lag)
							}
						}
					}
				}()
				defer func() { cancel(); <-lagDone }()
				err := consumer.RunPartition(workerctx, source)
				if err != nil && workerctx.Err() == nil {
					fail()
				}
			})
		}
		// A worker failure cancels the local group context and owned network,
		// interrupting Next without ever starting a replacement generation.
	}
}
