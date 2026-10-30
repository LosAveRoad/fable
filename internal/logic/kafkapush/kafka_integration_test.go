//go:build integration

package kafkapush

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func kafkaTest(t *testing.T) (*Publisher, *kafka.Client, string) {
	t.Helper()
	broker := os.Getenv("FABLE_LOGIC_TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set FABLE_LOGIC_TEST_KAFKA_BROKER for real Kafka tests")
	}
	topic := "fable.push.v1." + uuid.NewString()
	p, err := New([]string{broker}, topic)
	if err != nil {
		t.Fatal(err)
	}
	p.writer.ErrorLogger = kafka.LoggerFunc(t.Logf)
	transport := &kafka.Transport{DialTimeout: 2 * time.Second}
	admin := &kafka.Client{Addr: kafka.TCP(broker), Transport: transport, Timeout: 5 * time.Second}
	t.Cleanup(func() {
		p.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}}); err != nil {
			t.Errorf("test topic cleanup: %v", err)
		}
		transport.CloseIdleConnections()
	})
	return p, admin, broker
}
func createTestTopic(t *testing.T, admin *kafka.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := admin.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 6, ReplicationFactor: 1}}})
	if err != nil {
		t.Fatalf("explicit create topic: %v", err)
	}
	if err = response.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	for {
		response, err := admin.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Topics) == 1 && response.Topics[0].Error == nil && len(response.Topics[0].Partitions) == 6 {
			ready := true
			for _, partition := range response.Topics[0].Partitions {
				if partition.Error != nil || partition.Leader.Host == "" {
					ready = false
				}
			}
			if ready {
				// KRaft controller metadata can advertise a leader before the
				// broker has installed its partition. Poll a read-only leader
				// operation; do not retry Produce to conceal duplicate writes.
				requests := make([]kafka.OffsetRequest, 6)
				for i := range requests {
					requests[i] = kafka.LastOffsetOf(i)
				}
				offsets, err := admin.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: requests}})
				if err == nil && len(offsets.Topics[topic]) == 6 {
					for _, partition := range offsets.Topics[topic] {
						if partition.Error != nil || partition.LastOffset != 0 {
							ready = false
						}
					}
					if ready {
						return
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("topic leader metadata did not become ready")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func TestRealKafkaRequiresExplicitTopicForReadinessAndPublish(t *testing.T) {
	p, admin, _ := kafkaTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Ping(ctx); err == nil {
		t.Fatal("missing topic reported ready")
	}
	if err := p.Publish(ctx, directKey, payload); err == nil || errors.Is(err, ErrNotPublished) {
		t.Fatalf("missing topic write should remain uncertain: %v", err)
	}
	metadata, err := admin.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{p.topic}})
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Topics) != 1 || metadata.Topics[0].Error == nil {
		t.Fatalf("readiness/publish auto-created topic: %v", metadata.Topics)
	}
	createTestTopic(t, admin, p.topic)
	// A fresh publisher avoids relying on Kafka transport negative metadata cache.
	fresh, err := New([]string{os.Getenv("FABLE_LOGIC_TEST_KAFKA_BROKER")}, p.topic)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err = fresh.Ping(ctx); err != nil {
		t.Fatalf("explicit topic unready: %v", err)
	}
}
func TestRealKafkaPreservesPayloadPositionAndConversationPartition(t *testing.T) {
	p, admin, broker := kafkaTest(t)
	createTestTopic(t, admin, p.topic)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	bodies := [][]byte{
		[]byte(`{"schema_version":1,"kind":"message_push","task_id":"push:v1:M11111111-1111-4111-8111-111111111111:0","message":{"position":"9007199254740993","conversation_id":"direct:S11111111-1111-4111-8111-111111111111"},"recipients":["U22222222-2222-4222-8222-222222222222"],"targets":[],"chunk_index":0,"chunk_count":1}`),
		[]byte(`{"schema_version":1,"kind":"message_push","task_id":"push:v1:M22222222-2222-4222-8222-222222222222:0","message":{"position":"9007199254740994","conversation_id":"direct:S11111111-1111-4111-8111-111111111111"},"recipients":["U22222222-2222-4222-8222-222222222222"],"targets":[],"chunk_index":0,"chunk_count":1}`),
	}
	for _, body := range bodies {
		if err := p.Publish(ctx, directKey, body); err != nil {
			t.Fatal(err)
		}
	}
	// FNV-1a of this literal key is 993035945; Kafka Hash's six-partition
	// placement is therefore partition 5. This expected value is hand-derived.
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: p.topic, Partition: 5, MinBytes: 1, MaxBytes: 1024 * 1024, MaxWait: 100 * time.Millisecond})
	defer reader.Close()
	for i, expected := range bodies {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if message.Partition != 5 || message.Offset != int64(i) || string(message.Key) != directKey || !bytes.Equal(message.Value, expected) {
			t.Fatalf("record changed: partition=%d offset=%d key=%q value=%s", message.Partition, message.Offset, message.Key, message.Value)
		}
		var task struct {
			Message struct {
				Position string `json:"position"`
			} `json:"message"`
		}
		if err = json.Unmarshal(message.Value, &task); err != nil {
			t.Fatal(err)
		}
		if i == 0 && task.Message.Position != "9007199254740993" {
			t.Fatalf("position lost precision: %s", task.Message.Position)
		}
	}
	// Each successful adapter call emitted exactly one record. No consumer or
	// database operation is part of this adapter's lifecycle.
	connection, err := kafka.DialLeader(ctx, "tcp", broker, p.topic, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetDeadline(time.Now().Add(2 * time.Second))
	_, last, err := connection.ReadOffsets()
	if err != nil || last != 2 {
		t.Fatalf("unexpected extra records: last offset=%d err=%v", last, err)
	}
}
