package job

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"mychat/internal/dto/im"
)

var ErrInvalidReplay = errors.New("DLQ record is not safely replayable")

// DecodeDeadLetter rejects ambiguous/unrecognized envelopes, preserving the
// exact original task bytes for validation before any selected replay is sent.
func DecodeDeadLetter(raw []byte) (DeadLetter, error) {
	var r DeadLetter
	if len(raw) > 2*1024*1024 || !utf8.Valid(raw) || !validUnicodeEscapes(raw) {
		return r, ErrInvalidReplay
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if checkJSON(d, 0) != nil {
		return r, ErrInvalidReplay
	}
	if _, err := d.Token(); err != io.EOF {
		return r, ErrInvalidReplay
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil {
		return r, ErrInvalidReplay
	}
	return r, nil
}
func ReplayTask(r DeadLetter, runID string) (im.PushTask, error) {
	fail := func() (im.PushTask, error) { return im.PushTask{}, ErrInvalidReplay }
	n, err := strconv.ParseInt(r.SourceOffset, 10, 64)
	if !printable(runID, 128) || r.Task == nil || r.RawPayloadBase64 != "" || r.Reason != "unresolved" || len(r.PendingRecipients) == 0 || r.SourcePartition < 0 || err != nil || n < 0 || strconv.FormatInt(n, 10) != r.SourceOffset || !pushTopic(r.SourceTopic) || r.FirstAttemptAt.IsZero() || r.LastAttemptAt.Before(r.FirstAttemptAt) {
		return fail()
	}
	raw, err := json.Marshal(r.Task)
	if err != nil {
		return fail()
	}
	task, err := DecodeTask(raw)
	if err != nil {
		return fail()
	}
	original, err := base64.StdEncoding.Strict().DecodeString(r.OriginalPayloadBase64)
	if err != nil || len(original) == 0 {
		return fail()
	}
	source, err := DecodeTask(original)
	if err != nil || ImmutableDigest(source) != ImmutableDigest(task) {
		return fail()
	}
	// A replay of a replay may only narrow the previous pending scope. Compare
	// both the task and exact source so a modified envelope cannot widen it.
	allowed := map[string]bool{}
	for _, id := range task.Recipients {
		allowed[id] = true
	}
	for _, subset := range [][]string{task.PendingRecipients, source.PendingRecipients} {
		if subset != nil {
			set := map[string]bool{}
			for _, id := range subset {
				set[id] = true
			}
			for id := range allowed {
				allowed[id] = allowed[id] && set[id]
			}
		}
	}
	seen := map[string]bool{}
	for _, id := range r.PendingRecipients {
		if !allowed[id] || seen[id] {
			return fail()
		}
		seen[id] = true
	}
	task.PendingRecipients = append([]string(nil), r.PendingRecipients...)
	task.ReplayRunID = runID
	task.Targets = []im.ConnectionIdentity{}
	return task, nil
}
func pushTopic(topic string) bool {
	return topic == "fable.push.v1" || strings.HasPrefix(topic, "fable.push.v1.") && len(topic) > len("fable.push.v1.")
}
