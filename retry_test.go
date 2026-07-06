// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"testing"
)

// newJob builds a normalized job for retry tests.
func newJob(retry any, args ...any) *job {
	if args == nil {
		args = []any{}
	}
	return &job{Retry: retry, Queue: "default", Class: "W", Args: args, Jid: "jid", CreatedAt: 1}
}

// TestHandleFailureFirstRetry covers the first-failure branch: retry_count 0,
// failed_at stamped, job scheduled into the retry set at the backoff time.
func TestHandleFailureFirstRetry(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	j := newJob(true)
	retried, err := c.handleFailure(ctx(), j, errInjected)
	if err != nil {
		t.Fatal(err)
	}
	if !retried {
		t.Fatal("expected retried")
	}
	if j.RetryCount == nil || *j.RetryCount != 0 {
		t.Fatalf("retry_count = %v", j.RetryCount)
	}
	if j.FailedAt == 0 || j.ErrorClass == "" || j.ErrorMessage != errInjected.Error() {
		t.Fatalf("failure bookkeeping missing: %+v", j)
	}
	m, _ := mr.ZMembers(keyRetry)
	if len(m) != 1 {
		t.Fatalf("retry set = %v", m)
	}
	// Backoff for count 0 with zero jitter = 0^4 + 15 = 15s.
	score, _ := mr.ZScore(keyRetry, m[0])
	if score != timeToScore(baseTime)+15 {
		t.Fatalf("retry_at = %v want %v", score, timeToScore(baseTime)+15)
	}
}

// TestHandleFailureSubsequentRetry covers the increment branch: retry_count is
// bumped and retried_at is stamped.
func TestHandleFailureSubsequentRetry(t *testing.T) {
	c, _, _, _ := newTestClient(t)
	zero := 0
	j := newJob(true)
	j.RetryCount = &zero
	retried, err := c.handleFailure(ctx(), j, errInjected)
	if err != nil {
		t.Fatal(err)
	}
	if !retried || *j.RetryCount != 1 || j.RetriedAt == 0 {
		t.Fatalf("expected second retry: retried=%v count=%v retried_at=%v", retried, *j.RetryCount, j.RetriedAt)
	}
}

// TestHandleFailureExhaustedToDead covers the dead-set branch when retries run
// out, including the trimming commands.
func TestHandleFailureExhaustedToDead(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	// retry:0 → max 0, so the first failure (count 0) is not < 0 → dead.
	j := newJob(0)
	retried, err := c.handleFailure(ctx(), j, errInjected)
	if err != nil {
		t.Fatal(err)
	}
	if retried {
		t.Fatal("expected job to be buried, not retried")
	}
	m, _ := mr.ZMembers(keyDead)
	if len(m) != 1 {
		t.Fatalf("dead set = %v", m)
	}
}

// TestHandleFailureNoRetry covers retry:false — neither retried nor buried.
func TestHandleFailureNoRetry(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	j := newJob(false)
	retried, err := c.handleFailure(ctx(), j, errInjected)
	if err != nil || retried {
		t.Fatalf("retried=%v err=%v", retried, err)
	}
	if n, _ := mr.ZMembers(keyRetry); len(n) != 0 {
		t.Fatal("should not schedule a retry")
	}
	if n, _ := mr.ZMembers(keyDead); len(n) != 0 {
		t.Fatal("should not bury")
	}
}

// TestScheduleRetryErrors covers the marshal-error and ZAdd-error branches.
func TestScheduleRetryErrors(t *testing.T) {
	// Marshal error: an argument that cannot be JSON-encoded.
	c, _, _, _ := newTestClient(t)
	if _, err := c.handleFailure(ctx(), newJob(true, make(chan int)), errInjected); err == nil {
		t.Fatal("want marshal error on retry")
	}
	// ZAdd error.
	c2, _, rdb2, _ := newTestClient(t)
	addFault(rdb2, &faultHook{failCmd: "zadd", err: errInjected})
	if _, err := c2.handleFailure(ctx(), newJob(true), errInjected); err == nil {
		t.Fatal("want zadd error on retry")
	}
}

// TestKillErrors covers the marshal and all three Redis error branches of kill.
func TestKillErrors(t *testing.T) {
	// Marshal error while burying.
	c, _, _, _ := newTestClient(t)
	if _, err := c.handleFailure(ctx(), newJob(0, make(chan int)), errInjected); err == nil {
		t.Fatal("want marshal error on kill")
	}
	for _, name := range []string{"zadd", "zremrangebyscore", "zremrangebyrank"} {
		cc, _, rdb, _ := newTestClient(t)
		addFault(rdb, &faultHook{failCmd: name, err: errInjected})
		if _, err := cc.handleFailure(ctx(), newJob(0), errInjected); err == nil {
			t.Fatalf("want %s error on kill", name)
		}
	}
}

// TestSecondsToDelay checks the exact Sidekiq backoff formula with a fixed
// jitter injected.
func TestSecondsToDelay(t *testing.T) {
	c, _, _, _ := newTestClient(t, WithJitter(func(count int) int { return count + 1 }))
	// count=2 → 2^4 + 15 + (2+1) = 16 + 15 + 3 = 34.
	if d := c.secondsToDelay(2); d != 34 {
		t.Fatalf("secondsToDelay(2) = %d want 34", d)
	}
}
