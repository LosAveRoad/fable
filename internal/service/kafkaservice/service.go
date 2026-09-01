package kafkaservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"mychat/internal/config"
	"mychat/internal/dto/wschat"
)

var ErrNotConfigured = errors.New("kafka not configured")

type Service struct {
	writer    *kafka.Writer
	reader    *kafka.Reader
	brokers   []string
	topic     string
	healthy   atomic.Bool
	closeOnce sync.Once
}

func New(cfg config.KafkaConfig) (*Service, error) {
	if !cfg.Enabled || cfg.Mode != "kafka" {
		return nil, ErrNotConfigured
	}
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.ConsumerGroup == "" {
		return nil, errors.New("invalid kafka configuration")
	}
	brokers := append([]string(nil), cfg.Brokers...)
	instanceID := cfg.InstanceID
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}
	if instanceID == "" {
		return nil, errors.New("kafka instance id is required")
	}
	// Every application instance needs its own consumer group. A shared group
	// would deliver each event to only one pod, while WebSocket clients can be
	// connected to any pod.
	groupID := cfg.ConsumerGroup + "-" + instanceID
	s := &Service{brokers: brokers, topic: cfg.Topic, writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.Topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: false, WriteTimeout: 10 * time.Second}, reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: cfg.Topic, GroupID: groupID, StartOffset: kafka.LastOffset, MinBytes: 1, MaxBytes: 10e6})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], cfg.Topic, 0)
	if err != nil {
		_ = s.writer.Close()
		_ = s.reader.Close()
		return nil, fmt.Errorf("kafka topic health check: %w", err)
	}
	_ = conn.Close()
	s.healthy.Store(true)
	return s, nil
}

// Ready verifies that the configured topic is reachable and that the
// background consumer has not stopped.
func (s *Service) Ready(ctx context.Context) error {
	if s == nil || !s.healthy.Load() {
		return errors.New("kafka consumer is not running")
	}
	conn, err := kafka.DialLeader(ctx, "tcp", s.brokers[0], s.topic, 0)
	if err != nil {
		return fmt.Errorf("kafka topic readiness: %w", err)
	}
	return conn.Close()
}

func (s *Service) Publish(ctx context.Context, event wschat.ChatEvent) error {
	if s == nil || s.writer == nil {
		return ErrNotConfigured
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	key := event.ReceiveID
	if event.ReceiveType == wschat.ReceiveTypeUser {
		key = event.SenderID + ":" + event.ReceiveID
	}
	return s.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: raw})
}

func (s *Service) Consume(ctx context.Context, handler func(context.Context, wschat.ChatEvent) error) error {
	if s == nil || s.reader == nil {
		return ErrNotConfigured
	}
	defer s.healthy.Store(false)
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var event wschat.ChatEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			continue
		}
		if event.EventID == "" {
			continue
		}
		if err := handler(ctx, event); err != nil {
			return err
		}
		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		if s.writer != nil {
			if e := s.writer.Close(); e != nil {
				err = e
			}
		}
		if s.reader != nil {
			if e := s.reader.Close(); e != nil && err == nil {
				err = e
			}
		}
	})
	return err
}
