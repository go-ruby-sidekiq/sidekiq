// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"testing"
	"time"
)

// TestEnqueueScheduledJobs covers the happy path: a scheduled job becomes due
// after the clock advances and is moved onto its queue; a not-yet-due job stays.
func TestEnqueueScheduledJobs(t *testing.T) {
	c, mr, _, tc := newTestClient(t)
	if _, err := c.Push(ctx(), Item{Class: "Due", At: baseTime.Add(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Push(ctx(), Item{Class: "NotYet", At: baseTime.Add(1 * time.Hour)}); err != nil {
		t.Fatal(err)
	}

	// Not yet: nothing due at baseTime.
	n, err := c.EnqueueScheduledJobs(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 due, got %d", n)
	}

	// Advance past the first job's run-at.
	tc.add(30 * time.Second)
	n, err = c.EnqueueScheduledJobs(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 due, got %d", n)
	}
	l, _ := mr.List(queueKey("default"))
	if len(l) != 1 {
		t.Fatalf("due job not enqueued: %v", l)
	}
	// The far-future job remains scheduled.
	m, _ := mr.ZMembers(keySchedule)
	if len(m) != 1 {
		t.Fatalf("schedule should still hold NotYet: %v", m)
	}
}

// TestEnqueueDueFromRetrySet covers draining the retry set (the second set the
// poller scans).
func TestEnqueueDueFromRetrySet(t *testing.T) {
	c, mr, _, tc := newTestClient(t)
	// Put a job directly in the retry set, due in the past.
	j := &job{Retry: true, Queue: "default", Class: "R", Args: []any{}, Jid: "x", CreatedAt: 1}
	payload, _ := j.marshal()
	mr.ZAdd(keyRetry, timeToScore(baseTime.Add(-time.Second)), payload)

	tc.set(baseTime)
	n, err := c.EnqueueScheduledJobs(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 from retry, got %d", n)
	}
	l, _ := mr.List(queueKey("default"))
	if len(l) != 1 {
		t.Fatalf("retry job not enqueued: %v", l)
	}
}

// TestEnqueueDueLostRace covers the removed==0 branch: another poller claimed
// the member first, so this one skips it.
func TestEnqueueDueLostRace(t *testing.T) {
	c, mr, rdb, tc := newTestClient(t)
	if _, err := c.Push(ctx(), Item{Class: "Racy", At: baseTime.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	tc.add(time.Minute)
	addFault(rdb, &faultHook{zeroCmd: "zrem"})
	n, err := c.EnqueueScheduledJobs(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 enqueued on lost race, got %d", n)
	}
	if _, err := mr.List(queueKey("default")); err == nil {
		t.Fatal("expected no queue created")
	}
}

// TestEnqueueDueDecodeError covers a corrupt member in the schedule set.
func TestEnqueueDueDecodeError(t *testing.T) {
	c, mr, _, tc := newTestClient(t)
	mr.ZAdd(keySchedule, timeToScore(baseTime.Add(-time.Second)), "not-json")
	tc.set(baseTime)
	if _, err := c.EnqueueScheduledJobs(ctx()); err == nil {
		t.Fatal("want decode error")
	}
}

// TestEnqueueDueRedisErrors covers the ZRangeByScore, ZRem and enqueue error
// branches of the poller.
func TestEnqueueDueRedisErrors(t *testing.T) {
	// ZRangeByScore fails.
	c, _, rdb, _ := newTestClient(t)
	addFault(rdb, &faultHook{failCmd: "zrangebyscore", err: errInjected})
	if _, err := c.EnqueueScheduledJobs(ctx()); err == nil {
		t.Fatal("want zrangebyscore error")
	}

	// ZRem fails on a due member.
	c2, mr2, rdb2, tc2 := newTestClient(t)
	if _, err := c2.Push(ctx(), Item{Class: "W", At: baseTime.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	tc2.add(time.Minute)
	addFault(rdb2, &faultHook{failCmd: "zrem", err: errInjected})
	if _, err := c2.EnqueueScheduledJobs(ctx()); err == nil {
		t.Fatal("want zrem error")
	}
	_ = mr2

	// Enqueue (SAdd) fails after a successful ZRem.
	c3, _, rdb3, tc3 := newTestClient(t)
	if _, err := c3.Push(ctx(), Item{Class: "W", At: baseTime.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	tc3.add(time.Minute)
	addFault(rdb3, &faultHook{failCmd: "sadd", err: errInjected})
	if _, err := c3.EnqueueScheduledJobs(ctx()); err == nil {
		t.Fatal("want enqueue error")
	}
}
