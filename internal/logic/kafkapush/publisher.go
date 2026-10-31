// Package kafkapush synchronously publishes Logic's serialized push tasks.
package kafkapush

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// ErrNotPublished means this adapter rejected the operation before handing it
// to Kafka. Other publish errors leave the broker outcome uncertain.
var ErrNotPublished = errors.New("push not published")
var errInvalidConfig = errors.New("invalid push publisher configuration")
var errUnknown = errors.New("push publish outcome unknown")
var errNotReady = errors.New("push publisher unavailable")

const maxPayload = 256 * 1024

// Publisher owns a synchronous writer and its transport. It never creates
// topics, consumes messages, or persists business records.
type Publisher struct {
	writer    *kafka.Writer
	client    *kafka.Client
	transport *kafka.Transport
	topic     string
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func New(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 || !validTopic(topic) {
		return nil, errInvalidConfig
	}
	for _, broker := range brokers {
		if !validBroker(broker) {
			return nil, errInvalidConfig
		}
	}
	address := kafka.TCP(append([]string(nil), brokers...)...)
	transport := &kafka.Transport{DialTimeout: 2 * time.Second}
	writer := &kafka.Writer{
		Addr: address, Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll,
		Async: false, AllowAutoTopicCreation: false, MaxAttempts: 1, BatchSize: 1,
		BatchBytes: maxPayload + 1024, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		Transport: transport,
	}
	return &Publisher{writer: writer, transport: transport, topic: topic, client: &kafka.Client{Addr: address, Transport: transport, Timeout: 2 * time.Second}}, nil
}
func validTopic(topic string) bool {
	if len(topic) > 249 || (topic != "fable.push.v1" && (!strings.HasPrefix(topic, "fable.push.v1.") || len(topic) == len("fable.push.v1."))) {
		return false
	}
	for _, c := range topic {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func validBroker(broker string) bool {
	if len(broker) > 512 || strings.ContainsAny(broker, "/@?# \t\n\r") {
		return false
	}
	host, port, err := net.SplitHostPort(broker)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535 && strconv.Itoa(n) == port
}
func validKey(key string) bool {
	var id string
	if strings.HasPrefix(key, "direct:S") {
		id = strings.TrimPrefix(key, "direct:S")
	} else if strings.HasPrefix(key, "group:G") {
		id = strings.TrimPrefix(key, "group:G")
	} else {
		return false
	}
	if len(id) != 36 {
		return false
	}
	parsed, err := uuid.Parse(id)
	return err == nil && parsed != uuid.Nil && parsed.String() == id
}

func (p *Publisher) Publish(ctx context.Context, key string, raw []byte) error {
	trimmed := bytes.TrimSpace(raw)
	if p == nil || p.closed.Load() || ctx == nil || !validKey(key) || len(raw) > maxPayload || len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(raw) {
		return ErrNotPublished
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(ErrNotPublished, err)
	}
	// kafka-go may continue using a message after cancellation returns. Give it
	// private bytes so a caller can safely reuse its serialization buffer.
	message := kafka.Message{Key: []byte(key), Value: bytes.Clone(raw)}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return errors.Join(ErrNotPublished, err)
	}
	err := p.writer.WriteMessages(bounded, message)
	if err == nil {
		return nil
	}
	// Only this exact error comes from Writer rejecting a closed writer before
	// enqueueing. Wrapped broker/network errors keep the uncertain classification.
	if err == io.ErrClosedPipe {
		return ErrNotPublished
	}
	return safeError(errUnknown, bounded, err)
}

// Ping checks metadata for the explicit configured topic without requesting
// automatic topic creation. A reachable broker with a missing topic is unready.
func (p *Publisher) Ping(ctx context.Context) error {
	if p == nil || p.closed.Load() || ctx == nil {
		return errNotReady
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	metadata, err := p.client.Metadata(bounded, &kafka.MetadataRequest{Topics: []string{p.topic}})
	if err != nil {
		return safeError(errNotReady, bounded, err)
	}
	if len(metadata.Brokers) == 0 {
		return errNotReady
	}
	for _, topic := range metadata.Topics {
		if topic.Name != p.topic {
			continue
		}
		if topic.Error != nil || len(topic.Partitions) == 0 {
			return errNotReady
		}
		for _, partition := range topic.Partitions {
			if partition.Error != nil || partition.Leader.Host == "" || partition.Leader.Port <= 0 || len(partition.Isr) == 0 {
				return errNotReady
			}
		}
		return nil
	}
	return errNotReady
}

// safeError preserves cancellation classification without exposing broker
// diagnostics, addresses, task payloads, or credentials.
func safeError(base error, ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return errors.Join(base, ctx.Err())
	}
	if errors.Is(err, context.Canceled) {
		return errors.Join(base, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(base, context.DeadlineExceeded)
	}
	return base
}
func (p *Publisher) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if err := p.writer.Close(); err != nil {
			p.closeErr = errNotReady
		}
		p.transport.CloseIdleConnections()
	})
	return p.closeErr
}
