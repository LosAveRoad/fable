package app

import (
	"io"
	"log/slog"

	"mychat/internal/job"
)

func correlationObserver(output io.Writer) func(job.ConsumerEvent) {
	logger := slog.New(slog.NewJSONHandler(output, nil))
	return func(e job.ConsumerEvent) {
		fields := []any{"stage", e.Stage, "outcome", e.Outcome, "source_topic", e.SourceTopic, "source_partition", e.SourcePartition, "source_offset", e.SourceOffset, "attempts", e.Attempts, "pending", e.Pending}
		if e.RequestID != "" {
			fields = append(fields, "request_id", e.RequestID)
		}
		if e.MessageID != "" {
			fields = append(fields, "message_id", e.MessageID)
		}
		if e.TaskID != "" {
			fields = append(fields, "task_id", e.TaskID)
		}
		logger.Info("Job task boundary", fields...)
	}
}
