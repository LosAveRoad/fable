package job

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"mychat/internal/dto/im"
)

// PartitionSource is owned by one generation and one partition. Fetch must not
// acknowledge offsets. Commit acknowledges m.Offset+1, never a later fetched item.
type PartitionSource interface {
	Fetch(context.Context) (kafka.Message, error)
	Commit(context.Context, kafka.Message) error
}

// ProcessingObserver receives completed task processing before acknowledgement.
// Fetch lag updates must not claim delivery processing has completed.
type ProcessingObserver interface{ Processed(kafka.Message) }

type TaskProcessor interface {
	Process(context.Context, []byte) (Result, error)
}
type DeadLetterSink interface {
	Write(context.Context, DeadLetter) error
}

// DeadLetter extends the shared record without changing its mutually exclusive
// task/raw representation. OriginalPayloadBase64 retains the exact supported
// task bytes as well; SourceKeyBase64 preserves the original Kafka key.
type DeadLetter struct {
	im.DLQRecord
	OriginalPayloadBase64 string `json:"original_payload_base64,omitempty"`
	SourceKeyBase64       string `json:"source_key_base64,omitempty"`
}

// ConsumerEvent contains correlation only; it deliberately has no raw bytes,
// message content, credential, recipient identities or upstream error fields.
type ConsumerEvent struct {
	Stage, Outcome               string
	RequestID, TaskID, MessageID string
	SourceTopic                  string
	SourcePartition              int
	SourceOffset                 string
	Attempts, Pending            int
}
type ConsumerOptions struct {
	Observe          func(ConsumerEvent)
	Attempts         int
	RetryDelay       time.Duration
	IdentityCapacity int
}
type ConsumerStats struct {
	Processed, Committed, DLQ, Invalid, Conflicts, Retries, DLQErrors, CommitErrors, Offline, Enqueued, Duplicate, Unresolved uint64
	RecentIdentities                                                                                                          int
}
type Consumer struct {
	processor  TaskProcessor
	sink       DeadLetterSink
	options    ConsumerOptions
	mu         sync.Mutex
	stats      ConsumerStats
	identities map[string][32]byte
	order      []string
	next       int
	blocked    int
}

func NewConsumer(p TaskProcessor, s DeadLetterSink, o ConsumerOptions) *Consumer {
	if o.Attempts < 1 || o.Attempts > 3 {
		o.Attempts = 3
	}
	if o.RetryDelay <= 0 || o.RetryDelay > time.Second {
		o.RetryDelay = 200 * time.Millisecond
	}
	if o.IdentityCapacity < 1 || o.IdentityCapacity > 65536 {
		o.IdentityCapacity = 4096
	}
	return &Consumer{processor: p, sink: s, options: o, identities: make(map[string][32]byte)}
}
func (c *Consumer) Stats() ConsumerStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.RecentIdentities = len(c.identities)
	return s
}
func (c *Consumer) Blocked() bool              { c.mu.Lock(); defer c.mu.Unlock(); return c.blocked > 0 }
func (c *Consumer) add(f func(*ConsumerStats)) { c.mu.Lock(); defer c.mu.Unlock(); f(&c.stats) }

// ImmutableDigest excludes refreshed hints, tracing and replay scope. Message,
// chunk identity and FULL recipients always participate.
func ImmutableDigest(task im.PushTask) [32]byte {
	raw, _ := json.Marshal(struct {
		Version      uint32
		Kind, ID     string
		Message      im.Message
		Recipients   []string
		Index, Count uint32
	}{task.SchemaVersion, task.Kind, task.TaskID, task.Message, task.Recipients, task.ChunkIndex, task.ChunkCount})
	return sha256.Sum256(raw)
}
func (c *Consumer) conflict(task im.PushTask) bool {
	digest := ImmutableDigest(task)
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.identities[task.TaskID]; ok {
		return old != digest
	}
	if len(c.order) < c.options.IdentityCapacity {
		c.order = append(c.order, task.TaskID)
	} else {
		delete(c.identities, c.order[c.next])
		c.order[c.next] = task.TaskID
		c.next = (c.next + 1) % len(c.order)
	}
	c.identities[task.TaskID] = digest
	return false
}

var errBoundary = errors.New("Job partition blocked at acknowledgement boundary")

// RunPartition never fetches a later record until this record's DLQ (if any)
// and original commit both succeed. Failed acknowledgements retry only that
// acknowledgement, retaining known delivery progress. Exhaustion stops the
// worker so its generation cannot advance and a supervisor can restart safely.
func (c *Consumer) RunPartition(ctx context.Context, source PartitionSource) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := source.Fetch(ctx)
		if err != nil {
			return err
		}
		first := time.Now().UTC()
		task, decodeErr := DecodeTask(m.Value)
		event := ConsumerEvent{SourceTopic: m.Topic, SourcePartition: m.Partition, SourceOffset: strconv.FormatInt(m.Offset, 10)}
		if decodeErr == nil {
			event.RequestID = task.RequestID
			event.TaskID = task.TaskID
			event.MessageID = task.Message.MessageID
		}
		emit := func(stage, outcome string) {
			if c.options.Observe != nil {
				event.Stage = stage
				event.Outcome = outcome
				c.options.Observe(event)
			}
		}
		var dead *DeadLetter
		base := DeadLetter{DLQRecord: im.DLQRecord{SourceTopic: m.Topic, SourcePartition: int32(m.Partition), SourceOffset: strconv.FormatInt(m.Offset, 10), FirstAttemptAt: first}, SourceKeyBase64: base64.StdEncoding.EncodeToString(m.Key)}
		if decodeErr != nil || c.conflict(task) {
			base.Reason = "invalid_task"
			c.add(func(s *ConsumerStats) { s.Invalid++ })
			if decodeErr == nil {
				base.Reason = "immutable_task_conflict"
				c.add(func(s *ConsumerStats) { s.Conflicts++ })
			}
			base.RawPayloadBase64 = base64.StdEncoding.EncodeToString(m.Value)
			dead = &base
			emit("process", base.Reason)
		} else {
			result, processErr := c.processor.Process(ctx, m.Value)
			// Rebalance/cancellation discards no original offset. The next owner may
			// replay uncertain delivery, including a lost acknowledgement.
			if ctx.Err() != nil {
				event.Attempts = result.Attempts
				event.Pending = len(result.PendingRecipients())
				emit("process", "interrupted")
				return ctx.Err()
			}
			pending := result.PendingRecipients()
			event.Attempts = result.Attempts
			event.Pending = len(pending)
			c.add(func(s *ConsumerStats) {
				s.Processed++
				if result.Attempts > 1 {
					s.Retries += uint64(result.Attempts - 1)
				}
				for _, r := range result.Recipients {
					switch r.Outcome {
					case Offline:
						s.Offline++
					case Enqueued:
						s.Enqueued++
					case Duplicate:
						s.Duplicate++
					case Unresolved:
						s.Unresolved++
					}
				}
			})
			if processErr != nil || len(pending) > 0 {
				// An unexpected processor error cannot fabricate a successful empty subset.
				if len(pending) == 0 {
					pending = task.PendingRecipients
					if pending == nil {
						pending = task.Recipients
					}
				}
				base.Reason = "unresolved"
				base.Task = &task
				base.PendingRecipients = append([]string(nil), pending...)
				base.OriginalPayloadBase64 = base64.StdEncoding.EncodeToString(m.Value)
				dead = &base
			}
			if dead != nil {
				event.Pending = len(dead.PendingRecipients)
				emit("process", "unresolved")
			} else {
				emit("process", "terminal")
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if observer, ok := source.(ProcessingObserver); ok {
			observer.Processed(m)
		}
		if dead != nil {
			dead.LastAttemptAt = time.Now().UTC()
			if err := c.ack(ctx, func() error { return c.sink.Write(ctx, *dead) }, true); err != nil {
				emit("dlq", "blocked")
				return err
			}
			emit("dlq", "acknowledged")
			c.add(func(s *ConsumerStats) { s.DLQ++ })
		}
		if err := c.ack(ctx, func() error { return source.Commit(ctx, m) }, false); err != nil {
			emit("commit", "blocked")
			return err
		}
		emit("commit", "acknowledged")
		c.add(func(s *ConsumerStats) { s.Committed++ })
	}
}
func (c *Consumer) ack(ctx context.Context, fn func() error, dlq bool) error {
	blocked := false
	defer func() {
		if blocked {
			c.mu.Lock()
			c.blocked--
			c.mu.Unlock()
		}
	}()
	for attempt := 0; attempt < c.options.Attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fn(); err == nil {
			return nil
		}
		c.add(func(s *ConsumerStats) {
			if dlq {
				s.DLQErrors++
			} else {
				s.CommitErrors++
			}
		})
		if !blocked {
			c.mu.Lock()
			c.blocked++
			c.mu.Unlock()
			blocked = true
		}
		if attempt+1 < c.options.Attempts {
			c.add(func(s *ConsumerStats) { s.Retries++ })
			if err := pause(ctx, c.options.RetryDelay); err != nil {
				return err
			}
		}
	}
	return errBoundary
}
func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
