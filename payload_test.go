// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"testing"
	"time"
)

// TestMaxRetries covers every retry-option kind.
func TestMaxRetries(t *testing.T) {
	cases := []struct {
		retry     any
		wantMax   int
		wantRetry bool
	}{
		{true, 25, true},      // bool true → default
		{false, 0, false},     // bool false → not retryable
		{float64(9), 9, true}, // JSON-decoded number
		{int(4), 4, true},     // Go-constructed int
		{"weird", 25, true},   // unrecognised → default
	}
	for _, tc := range cases {
		j := &job{Retry: tc.retry}
		max, retryable := j.maxRetries(25)
		if max != tc.wantMax || retryable != tc.wantRetry {
			t.Fatalf("retry=%v → (%d,%v) want (%d,%v)", tc.retry, max, retryable, tc.wantMax, tc.wantRetry)
		}
	}
}

// TestMarshalRoundTrip covers marshal + decodeJob for a fully-populated job.
func TestMarshalRoundTrip(t *testing.T) {
	j := &job{Retry: true, Queue: "q", Class: "C", Args: []any{"a"}, Jid: "j", CreatedAt: 1.5, EnqueuedAt: 2.5}
	s, err := j.marshal()
	if err != nil {
		t.Fatal(err)
	}
	back, err := decodeJob(s)
	if err != nil {
		t.Fatal(err)
	}
	if back.Class != "C" || back.Queue != "q" || back.CreatedAt != 1.5 || back.EnqueuedAt != 2.5 {
		t.Fatalf("round trip mismatch: %+v", back)
	}
}

// TestDecodeJobError covers invalid JSON.
func TestDecodeJobError(t *testing.T) {
	if _, err := decodeJob("{not json"); err == nil {
		t.Fatal("want decode error")
	}
}

// TestMarshalError covers a payload with an unencodable argument.
func TestMarshalError(t *testing.T) {
	j := &job{Args: []any{make(chan int)}}
	if _, err := j.marshal(); err == nil {
		t.Fatal("want marshal error")
	}
}

// TestTimeToScore covers the epoch-seconds conversion and its string form.
func TestTimeToScore(t *testing.T) {
	tm := time.Unix(1000, 250_000_000).UTC() // 1000.25s
	if got := timeToScore(tm); got != 1000.25 {
		t.Fatalf("timeToScore = %v", got)
	}
	if got := scoreString(1000.25); got != "1000.25" {
		t.Fatalf("scoreString = %q", got)
	}
}
