// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"testing"
	"time"
)

// TestWorkerPerformAsync enqueues via a worker and checks the class/queue from
// its options land on the payload.
func TestWorkerPerformAsync(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	w := c.Worker("EmailJob", WorkerOptions{Queue: "mailers", Retry: 3})
	jid, err := w.PerformAsync(ctx(), "user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if jid == "" {
		t.Fatal("empty jid")
	}
	l, _ := mr.List(queueKey("mailers"))
	want := `{"retry":3,"queue":"mailers","class":"EmailJob","args":["user@example.com"],` +
		`"jid":"000102030405060708090a0b","created_at":1700000000.5,"enqueued_at":1700000000.5}`
	if len(l) != 1 || l[0] != want {
		t.Fatalf("payload = %q", l)
	}
}

// TestWorkerPerformAt schedules via a worker.
func TestWorkerPerformAt(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	w := c.Worker("Later", WorkerOptions{})
	at := baseTime.Add(time.Hour)
	if _, err := w.PerformAt(ctx(), at, 42); err != nil {
		t.Fatal(err)
	}
	m, _ := mr.ZMembers(keySchedule)
	if len(m) != 1 {
		t.Fatalf("schedule = %v", m)
	}
}

// TestWorkerPerformIn covers both the positive-interval (schedule) and the
// non-positive-interval (immediate) branches.
func TestWorkerPerformIn(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	w := c.Worker("Soon", WorkerOptions{})

	if _, err := w.PerformIn(ctx(), 30*time.Second, 1); err != nil {
		t.Fatal(err)
	}
	m, _ := mr.ZMembers(keySchedule)
	if len(m) != 1 {
		t.Fatalf("expected scheduled job, got %v", m)
	}
	score, _ := mr.ZScore(keySchedule, m[0])
	if score != timeToScore(baseTime.Add(30*time.Second)) {
		t.Fatalf("score = %v", score)
	}

	// Non-positive interval enqueues immediately.
	if _, err := w.PerformIn(ctx(), 0, 2); err != nil {
		t.Fatal(err)
	}
	l, _ := mr.List(queueKey("default"))
	if len(l) != 1 {
		t.Fatalf("expected immediate enqueue, got %v", l)
	}
}
