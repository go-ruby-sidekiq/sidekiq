// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"errors"
	"testing"
	"time"
)

var errInjected = errors.New("injected redis failure")

// TestPushPayloadByteExact asserts the enqueued payload is byte-for-byte the
// documented Sidekiq job hash, so a real Sidekiq server can consume it.
func TestPushPayloadByteExact(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	jid, err := c.Push(ctx(), Item{Class: "HardWorker", Args: []any{1, "two"}})
	if err != nil {
		t.Fatal(err)
	}
	if jid != "000102030405060708090a0b" {
		t.Fatalf("jid = %q", jid)
	}
	got, err := mr.List(queueKey("default"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"retry":true,"queue":"default","class":"HardWorker","args":[1,"two"],` +
		`"jid":"000102030405060708090a0b","created_at":1700000000.5,"enqueued_at":1700000000.5}`
	if len(got) != 1 || got[0] != want {
		t.Fatalf("payload mismatch:\n got %q\nwant %q", got, want)
	}
	// The queue name is registered in the queues set.
	if ok, _ := mr.SIsMember(keyQueues, "default"); !ok {
		t.Fatal("queue not added to queues set")
	}
}

// TestPushDefaultsAndOverrides covers normalize's default and override branches.
func TestPushDefaultsAndOverrides(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	// Explicit queue, retry count, jid and nil args.
	jid, err := c.Push(ctx(), Item{Class: "W", Queue: "critical", Retry: 5, Jid: "deadbeef"})
	if err != nil {
		t.Fatal(err)
	}
	if jid != "deadbeef" {
		t.Fatalf("jid = %q", jid)
	}
	got, err := mr.List(queueKey("critical"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"retry":5,"queue":"critical","class":"W","args":[],` +
		`"jid":"deadbeef","created_at":1700000000.5,"enqueued_at":1700000000.5}`
	if got[0] != want {
		t.Fatalf("payload = %q", got[0])
	}
	// Explicit retry:false is preserved.
	if _, err := c.Push(ctx(), Item{Class: "W", Retry: false}); err != nil {
		t.Fatal(err)
	}
	l, _ := mr.List(queueKey("default"))
	if len(l) != 1 || l[0][:14] != `{"retry":false` {
		t.Fatalf("retry:false payload = %q", l)
	}
}

// TestScheduledPush covers the At branch: a ZADD to the schedule set with the
// run-at timestamp as score, and no enqueued_at in the stored payload.
func TestScheduledPush(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	at := baseTime.Add(90 * time.Second)
	if _, err := c.Push(ctx(), Item{Class: "Later", At: at}); err != nil {
		t.Fatal(err)
	}
	members, err := mr.ZMembers(keySchedule)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("schedule members = %v", members)
	}
	want := `{"retry":true,"queue":"default","class":"Later","args":[],` +
		`"jid":"000102030405060708090a0b","created_at":1700000000.5}`
	if members[0] != want {
		t.Fatalf("scheduled payload = %q", members[0])
	}
	score, err := mr.ZScore(keySchedule, members[0])
	if err != nil {
		t.Fatal(err)
	}
	if score != timeToScore(at) {
		t.Fatalf("score = %v want %v", score, timeToScore(at))
	}
}

// TestPushBulk enqueues several jobs sharing options and checks their jids.
func TestPushBulk(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	jids, err := c.PushBulk(ctx(), Item{Class: "Bulk"}, [][]any{{1}, {2}, {3}})
	if err != nil {
		t.Fatal(err)
	}
	if len(jids) != 3 || jids[0] == jids[1] {
		t.Fatalf("jids = %v", jids)
	}
	l, _ := mr.List(queueKey("default"))
	if len(l) != 3 {
		t.Fatalf("want 3 enqueued, got %d", len(l))
	}
}

// TestJIDGenerationError covers the entropy-failure path.
func TestJIDGenerationError(t *testing.T) {
	c, _, _, _ := newTestClient(t, WithRand(failReader{}))
	if _, err := c.Push(ctx(), Item{Class: "W"}); err == nil {
		t.Fatal("want jid generation error")
	}
	// Also via the schedule path (normalize runs first, so same error).
	if _, err := c.Push(ctx(), Item{Class: "W", At: baseTime.Add(time.Second)}); err == nil {
		t.Fatal("want jid generation error on scheduled push")
	}
	// And via PushBulk.
	if _, err := c.PushBulk(ctx(), Item{Class: "W"}, [][]any{{1}}); err == nil {
		t.Fatal("want jid generation error on bulk push")
	}
}

// TestPushMarshalErrors covers the JSON-marshal failure branches of enqueue and
// schedule (an argument that cannot be encoded).
func TestPushMarshalErrors(t *testing.T) {
	c, _, _, _ := newTestClient(t)
	bad := []any{make(chan int)}
	if _, err := c.Push(ctx(), Item{Class: "W", Args: bad}); err == nil {
		t.Fatal("want marshal error on enqueue")
	}
	if _, err := c.Push(ctx(), Item{Class: "W", Args: bad, At: baseTime.Add(time.Second)}); err == nil {
		t.Fatal("want marshal error on schedule")
	}
}

// TestEnqueueRedisErrors covers the SAdd and LPush transport-error branches.
func TestEnqueueRedisErrors(t *testing.T) {
	for _, name := range []string{"sadd", "lpush"} {
		c, _, rdb, _ := newTestClient(t)
		addFault(rdb, &faultHook{failCmd: name, err: errInjected})
		if _, err := c.Push(ctx(), Item{Class: "W"}); err == nil {
			t.Fatalf("want error when %s fails", name)
		}
	}
}

// TestScheduleRedisError covers the ZAdd transport-error branch of schedule.
func TestScheduleRedisError(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	addFault(rdb, &faultHook{failCmd: "zadd", err: errInjected})
	if _, err := c.Push(ctx(), Item{Class: "W", At: baseTime.Add(time.Second)}); err == nil {
		t.Fatal("want error when zadd fails")
	}
}

// TestPushBulkError covers propagation of a Push error mid-batch.
func TestPushBulkError(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	addFault(rdb, &faultHook{failCmd: "lpush", err: errInjected})
	jids, err := c.PushBulk(ctx(), Item{Class: "W"}, [][]any{{1}, {2}})
	if err == nil {
		t.Fatal("want error")
	}
	if len(jids) != 0 {
		t.Fatalf("expected no jids before failure, got %v", jids)
	}
}

// TestNewDefaults constructs a Client with no options to cover the production
// defaults (real clock, crypto/rand, jitter, max retries).
func TestNewDefaults(t *testing.T) {
	mr := miniredisClient(t)
	c := New(mr)
	if c.clock == nil || c.rand == nil || c.jitter == nil {
		t.Fatal("defaults not applied")
	}
	if c.maxRetries != DefaultMaxRetries {
		t.Fatalf("maxRetries = %d", c.maxRetries)
	}
	// A real crypto/rand jid is 24 hex chars.
	jid, err := c.generateJID()
	if err != nil {
		t.Fatal(err)
	}
	if len(jid) != 24 {
		t.Fatalf("jid len = %d", len(jid))
	}
}
