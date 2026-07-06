// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"encoding/json"
	"strconv"
	"time"
)

// job is the in-flight representation of a Sidekiq job hash. Its JSON encoding
// is byte-for-byte a Sidekiq payload: encoding/json emits struct fields in
// declaration order, so the field order below fixes the key order of the wire
// payload to match Sidekiq's insertion order (retry, queue, class, args, jid,
// created_at, enqueued_at, then the failure-tracking keys added on a retry).
//
// Timestamps are float epoch seconds (Ruby's Time.now.to_f), the long-standing
// Sidekiq representation and the same unit used for the schedule/retry/dead
// sorted-set scores, so the whole engine is internally consistent and a real
// Sidekiq server interoperates with it.
type job struct {
	// Retry is the job's retry policy: bool (true = default max attempts, false
	// = never retry) or an int (an explicit max attempt count). It is always
	// present in the payload.
	Retry any `json:"retry"`
	// Queue is the queue the job runs on.
	Queue string `json:"queue"`
	// Class is the worker class name (the Ruby class whose perform runs).
	Class string `json:"class"`
	// Args are the positional arguments passed to perform.
	Args []any `json:"args"`
	// Jid is the 24-hex-character job id.
	Jid string `json:"jid"`
	// CreatedAt is when the job was first created (float epoch seconds).
	CreatedAt float64 `json:"created_at"`
	// EnqueuedAt is when the job was last pushed onto a queue. It is omitted for
	// jobs sitting in the schedule set (Sidekiq deletes it there) and set when
	// the job is moved onto its queue.
	EnqueuedAt float64 `json:"enqueued_at,omitempty"`

	// The following keys are added by the retry machinery on failure and are
	// otherwise absent, exactly as Sidekiq records them.

	// ErrorMessage / ErrorClass describe the last failure.
	ErrorMessage string `json:"error_message,omitempty"`
	ErrorClass   string `json:"error_class,omitempty"`
	// FailedAt is when the job first failed (float epoch seconds).
	FailedAt float64 `json:"failed_at,omitempty"`
	// RetriedAt is when the job was most recently retried (float epoch seconds).
	RetriedAt float64 `json:"retried_at,omitempty"`
	// RetryCount is the number of retries performed so far. It is a pointer so
	// that the value 0 (recorded on the first failure) is still emitted.
	RetryCount *int `json:"retry_count,omitempty"`
}

// marshal encodes the job to its Sidekiq JSON wire form.
func (j *job) marshal() (string, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeJob parses a Sidekiq JSON payload back into a job.
func decodeJob(payload string) (*job, error) {
	var j job
	if err := json.Unmarshal([]byte(payload), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// maxRetries returns the maximum number of retry attempts implied by the job's
// retry option and whether the job is retryable at all. A bool true (or a nil /
// unrecognised value) means the supplied default; false means no retries; a
// number means that explicit count.
func (j *job) maxRetries(def int) (max int, retryable bool) {
	switch v := j.Retry.(type) {
	case bool:
		if v {
			return def, true
		}
		return 0, false
	case float64: // JSON numbers decode to float64
		return int(v), true
	case int:
		return v, true
	default:
		return def, true
	}
}

// timeToScore renders a time as the string score Sidekiq uses for its
// sorted-set members: the float epoch seconds. This is the exact textual form
// Sidekiq writes (Time#to_f rendered by Ruby), so scores are comparable.
func timeToScore(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second)
}

// scoreString formats a score the way Sidekiq stores it (a decimal string).
func scoreString(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}
