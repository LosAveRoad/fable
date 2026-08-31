package kafkaservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"mychat/internal/config"
	"mychat/internal/dto/wschat"
)

var ErrNotConfigured = errors.New("kafka not configured")

type Service struct {
	writer    *kafka.Writer
	reader    *kafka.Reader
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
	s := &Service{writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.Topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: false, WriteTimeout: 10 * time.Second}, reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: cfg.Topic, GroupID: cfg.ConsumerGroup, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10e6})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := (&kafka.Dialer{}).DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		_ = s.writer.Close()
		_ = s.reader.Close()
		return nil, fmt.Errorf("kafka health check: %w", err)
	}
	_ = conn.Close()
	return s, nil
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
