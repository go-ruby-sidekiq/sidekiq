// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"testing"
	"time"
)

// TestStatsSnapshot covers a full snapshot with jobs in every set, including
// the missing-counter-key (redis.Nil → 0) branch for stat:failed.
func TestStatsSnapshot(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	// Two enqueued (one default, one critical), one scheduled, one dead.
	if _, err := c.Push(ctx(), Item{Class: "A"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Push(ctx(), Item{Class: "B", Queue: "critical"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Push(ctx(), Item{Class: "S", At: baseTime.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.handleFailure(ctx(), newJob(0), errInjected); err != nil {
		t.Fatal(err)
	}
	// One processed, stat:failed left unset (exercises the Nil-as-zero path).
	mr.Set(statProcessed, "7")

	s, err := c.Stats(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if s.Processed != 7 {
		t.Fatalf("Processed = %d", s.Processed)
	}
	if s.Failed != 0 {
		t.Fatalf("Failed = %d", s.Failed)
	}
	if s.Enqueued != 2 {
		t.Fatalf("Enqueued = %d", s.Enqueued)
	}
	if s.Scheduled != 1 {
		t.Fatalf("Scheduled = %d", s.Scheduled)
	}
	if s.Dead != 1 {
		t.Fatalf("Dead = %d", s.Dead)
	}
	if s.Retries != 0 {
		t.Fatalf("Retries = %d", s.Retries)
	}
}

// TestStatsRedisErrors covers every transport-error branch of Stats in order.
func TestStatsRedisErrors(t *testing.T) {
	cases := []struct {
		cmd   string
		index int
	}{
		{"get", 1},      // stat:processed
		{"get", 2},      // stat:failed
		{"smembers", 0}, // enqueuedTotal
		{"zcard", 1},    // scheduled
		{"zcard", 2},    // retries
		{"zcard", 3},    // dead
	}
	for _, tc := range cases {
		c, _, rdb, _ := newTestClient(t)
		addFault(rdb, &faultHook{failCmd: tc.cmd, failIndex: tc.index, err: errInjected})
		if _, err := c.Stats(ctx()); err == nil {
			t.Fatalf("want error when %s (#%d) fails", tc.cmd, tc.index)
		}
	}
}

// TestStatsLLenError covers the per-queue LLEN error inside enqueuedTotal.
func TestStatsLLenError(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	if _, err := c.Push(ctx(), Item{Class: "A"}); err != nil {
		t.Fatal(err)
	}
	addFault(rdb, &faultHook{failCmd: "llen", err: errInjected})
	if _, err := c.Stats(ctx()); err == nil {
		t.Fatal("want llen error")
	}
}

// TestCounterParsesValue covers the counter happy path with a present value.
func TestCounterPresent(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	mr.Set(statProcessed, "42")
	v, err := c.counter(ctx(), statProcessed)
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Fatalf("counter = %d", v)
	}
}
