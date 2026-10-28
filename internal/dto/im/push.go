package im

import "time"

const (
	PushSchemaVersion uint32 = 1
	PushKind                 = "message_push"
)

// PushTask is a saved-message notification. A Job must never persist it again.
type PushTask struct {
	SchemaVersion     uint32               `json:"schema_version"`
	Kind              string               `json:"kind"`
	TaskID            string               `json:"task_id"`
	Message           Message              `json:"message"`
	Recipients        []string             `json:"recipients"`
	Targets           []ConnectionIdentity `json:"targets"`
	ChunkIndex        uint32               `json:"chunk_index"`
	ChunkCount        uint32               `json:"chunk_count"`
	RequestID         string               `json:"request_id,omitempty"`
	PendingRecipients []string             `json:"pending_recipients,omitempty"`
	ReplayRunID       string               `json:"replay_run_id,omitempty"`
}

// DLQRecord preserves complete original data and an unresolved recipient subset.
// Task is for validated supported tasks whose delivery failed. Decode, schema
// and semantic failures preserve original bytes in RawPayloadBase64 instead.
// Task and RawPayloadBase64 are mutually exclusive, see the contract spec.
type DLQRecord struct {
	SourceTopic       string    `json:"source_topic"`
	SourcePartition   int32     `json:"source_partition"`
	SourceOffset      string    `json:"source_offset"`
	Reason            string    `json:"reason"`
	Task              *PushTask `json:"task,omitempty"`
	PendingRecipients []string  `json:"pending_recipients,omitempty"`
	RawPayloadBase64  string    `json:"raw_payload_base64,omitempty"`
	FirstAttemptAt    time.Time `json:"first_attempt_at"`
	LastAttemptAt     time.Time `json:"last_attempt_at"`
}
