package app

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"mychat/internal/job"
)

type ReplayOptions struct {
	RunID                string
	Partition            int
	FromOffset, ToOffset int64 // explicit INCLUSIVE range, at most 10,000 offsets
	Execute              bool  // false is the default preview; no writes, commits, or DB access
}
type ReplaySummary struct {
	Scanned, Replayable, Rejected, Published int
	Executed                                 bool
}

var errReplay = errors.New("invalid or unavailable bounded DLQ replay selection")

func validateReplay(o ReplayOptions) error {
	if o.Partition < 0 || o.Partition > math.MaxInt32 || o.FromOffset < 0 || o.ToOffset < o.FromOffset || o.ToOffset == math.MaxInt64 || o.ToOffset-o.FromOffset >= 10000 || len(o.RunID) == 0 || len(o.RunID) > 128 {
		return errReplay
	}
	for _, r := range o.RunID {
		if r < 32 || r > 126 {
			return errReplay
		}
	}
	return nil
}
func prepareReplay(c Config, o ReplayOptions, messages []kafka.Message) ([]kafka.Message, ReplaySummary, error) {
	var out []kafka.Message
	var summary ReplaySummary
	if err := validateReplay(o); err != nil {
		return nil, summary, err
	}
	// Defense in depth: preflight requires every selected offset, even when
	// retention or compaction changed the range after its metadata snapshot.
	if int64(len(messages)) != o.ToOffset-o.FromOffset+1 {
		return nil, summary, errReplay
	}
	for i, m := range messages {
		if m.Partition != o.Partition || m.Offset != o.FromOffset+int64(i) {
			return nil, summary, errReplay
		}
	}
	for _, m := range messages {
		summary.Scanned++
		r, err := job.DecodeDeadLetter(m.Value)
		if err != nil || r.SourceTopic != c.Topic {
			summary.Rejected++
			continue
		}
		task, err := job.ReplayTask(r, o.RunID)
		if err != nil {
			summary.Rejected++
			continue
		}
		raw, err := json.Marshal(task)
		if err != nil {
			return nil, summary, errReplay
		}
		out = append(out, kafka.Message{Key: []byte(task.Message.ConversationID), Value: raw, Headers: []kafka.Header{{Key: "replay_run_id", Value: []byte(o.RunID)}, {Key: "dlq_topic", Value: []byte(c.DLQTopic)}, {Key: "dlq_partition", Value: []byte(strconv.Itoa(m.Partition))}, {Key: "dlq_offset", Value: []byte(strconv.FormatInt(m.Offset, 10))}}})
		summary.Replayable++
	}
	return out, summary, nil
}

// Replay snapshots a finite explicit DLQ partition range, validates everything
// before publishing, and previews by default. It never consumes with a group,
// deletes a DLQ record, resets offsets, or calls a persistence RPC. A failed
// publication returns the confirmed count; the failed write may be uncertain.
func Replay(ctx context.Context, c Config, o ReplayOptions) (ReplaySummary, error) {
	var summary ReplaySummary
	if err := validate(c); err != nil {
		return summary, err
	}
	if err := validateReplay(o); err != nil {
		return summary, err
	}
	messages, err := readReplayRange(ctx, c, o)
	if err != nil {
		return summary, err
	}
	selected, summary, err := prepareReplay(c, o, messages)
	if err != nil || !o.Execute {
		return summary, err
	}
	summary.Executed = true
	sink := newSink(c, c.Topic)
	defer sink.Close()
	for _, m := range selected {
		if err := sink.publishMessage(ctx, m); err != nil {
			return summary, err
		}
		summary.Published++
	}
	return summary, nil
}
func readReplayRange(ctx context.Context, c Config, o ReplayOptions) ([]kafka.Message, error) {
	dialer := &kafka.Dialer{Timeout: 2 * time.Second}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := dialer.DialLeader(bounded, "tcp", c.Brokers[0], c.DLQTopic, o.Partition)
	if err != nil {
		return nil, errReplay
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	first, last, err := conn.ReadOffsets()
	if err != nil || o.FromOffset < first || o.ToOffset >= last {
		return nil, errReplay
	}
	// The selected byte budget bounds memory independently from the offset span.
	// Truncated/compacted ranges fail closed before anything is published.
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: c.DLQTopic, Partition: o.Partition, Dialer: dialer, MinBytes: 1, MaxBytes: 2 * 1024 * 1024, MaxWait: 500 * time.Millisecond, QueueCapacity: 1, MaxAttempts: 2})
	defer reader.Close()
	if reader.SetOffset(o.FromOffset) != nil {
		return nil, errReplay
	}
	return collectReplayRange(ctx, o, reader.FetchMessage)
}

// collectReplayRange requires the complete inclusive snapshot. A reader may
// clamp an out-of-range offset after retention or skip compacted offsets; both
// must fail rather than approving an incomplete selection for publication.
func collectReplayRange(ctx context.Context, o ReplayOptions, fetch func(context.Context) (kafka.Message, error)) ([]kafka.Message, error) {
	if err := validateReplay(o); err != nil {
		return nil, err
	}
	messages := make([]kafka.Message, 0)
	bytes := 0
	for expected := o.FromOffset; expected <= o.ToOffset; expected++ {
		bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
		m, err := fetch(bounded)
		cancel()
		if err != nil || m.Partition != o.Partition || m.Offset != expected {
			return nil, errReplay
		}
		bytes += len(m.Value) + len(m.Key)
		if bytes > 16*1024*1024 {
			return nil, errReplay
		}
		messages = append(messages, m)
	}
	return messages, nil
}
