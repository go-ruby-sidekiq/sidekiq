// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestProcessOneSuccess runs a job through the perform seam and checks the
// processed counter is bumped.
func TestProcessOneSuccess(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	var gotClass string
	var gotArgs []any
	perform := func(class string, args []any) error {
		gotClass, gotArgs = class, args
		return nil
	}
	if _, err := c.Push(ctx(), Item{Class: "Hello", Args: []any{"world"}}); err != nil {
		t.Fatal(err)
	}
	p := c.NewProcessor(perform)
	p.SetTimeout(10 * time.Millisecond)

	processed, err := p.ProcessOne(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("expected a job to be processed")
	}
	if gotClass != "Hello" || len(gotArgs) != 1 || gotArgs[0] != "world" {
		t.Fatalf("perform got %q %v", gotClass, gotArgs)
	}
	if v, _ := mr.Get(statProcessed); v != "1" {
		t.Fatalf("stat:processed = %q", v)
	}
}

// TestProcessOneFailureSchedulesRetry covers the failure path: processed and
// failed counters bump and the job lands in the retry set.
func TestProcessOneFailureSchedulesRetry(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	perform := func(string, []any) error { return errInjected }
	if _, err := c.Push(ctx(), Item{Class: "Boom"}); err != nil {
		t.Fatal(err)
	}
	p := c.NewProcessor(perform, "default")
	processed, err := p.ProcessOne(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("expected processed")
	}
	if v, _ := mr.Get(statProcessed); v != "1" {
		t.Fatalf("processed = %q", v)
	}
	if v, _ := mr.Get(statFailed); v != "1" {
		t.Fatalf("failed = %q", v)
	}
	m, _ := mr.ZMembers(keyRetry)
	if len(m) != 1 {
		t.Fatalf("expected job in retry set, got %v", m)
	}
}

// TestProcessOneEmpty covers the BRPOP-timeout (redis.Nil) branch.
func TestProcessOneEmpty(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	addFault(rdb, &faultHook{failCmd: "brpop", err: redis.Nil})
	p := c.NewProcessor(func(string, []any) error { return nil })
	processed, err := p.ProcessOne(ctx())
	if err != nil {
		t.Fatalf("timeout should not be an error: %v", err)
	}
	if processed {
		t.Fatal("expected no job processed")
	}
}

// TestProcessOneFetchError covers a non-nil BRPOP transport error.
func TestProcessOneFetchError(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	addFault(rdb, &faultHook{failCmd: "brpop", err: errInjected})
	p := c.NewProcessor(func(string, []any) error { return nil })
	if _, err := p.ProcessOne(ctx()); err == nil {
		t.Fatal("want fetch error")
	}
}

// TestProcessOneDecodeError covers a corrupt payload on the queue.
func TestProcessOneDecodeError(t *testing.T) {
	c, mr, _, _ := newTestClient(t)
	mr.Lpush(queueKey("default"), "not-json")
	p := c.NewProcessor(func(string, []any) error { return nil })
	if _, err := p.ProcessOne(ctx()); err == nil {
		t.Fatal("want decode error")
	}
}

// TestProcessOneCounterErrors covers the processed- and failed-counter
// transport-error branches of run.
func TestProcessOneCounterErrors(t *testing.T) {
	// processed counter fails (first incr).
	c, _, rdb, _ := newTestClient(t)
	if _, err := c.Push(ctx(), Item{Class: "W"}); err != nil {
		t.Fatal(err)
	}
	addFault(rdb, &faultHook{failCmd: "incr", failIndex: 1, err: errInjected})
	p := c.NewProcessor(func(string, []any) error { return nil })
	if _, err := p.ProcessOne(ctx()); err == nil {
		t.Fatal("want processed-counter error")
	}

	// failed counter fails (second incr): perform errors so both incrs run.
	c2, _, rdb2, _ := newTestClient(t)
	if _, err := c2.Push(ctx(), Item{Class: "W"}); err != nil {
		t.Fatal(err)
	}
	addFault(rdb2, &faultHook{failCmd: "incr", failIndex: 2, err: errInjected})
	p2 := c2.NewProcessor(func(string, []any) error { return errInjected })
	if _, err := p2.ProcessOne(ctx()); err == nil {
		t.Fatal("want failed-counter error")
	}
}

// TestProcessOneReturnsHandleFailureError covers run returning a retry-machinery
// error (ProcessOne surfaces it with processed=true).
func TestProcessOneReturnsHandleFailureError(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	if _, err := c.Push(ctx(), Item{Class: "W"}); err != nil {
		t.Fatal(err)
	}
	// Let both incrs succeed, but the retry zadd fails.
	addFault(rdb, &faultHook{failCmd: "zadd", err: errInjected})
	p := c.NewProcessor(func(string, []any) error { return errInjected })
	processed, err := p.ProcessOne(ctx())
	if !processed || err == nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
}

// TestProcessorDefaultQueue covers NewProcessor's no-queue default.
func TestProcessorDefaultQueue(t *testing.T) {
	c, _, _, _ := newTestClient(t)
	p := c.NewProcessor(func(string, []any) error { return nil })
	if len(p.queues) != 1 || p.queues[0] != DefaultQueue {
		t.Fatalf("default queue = %v", p.queues)
	}
}

// TestRunStopsOnCanceledContext covers Run's context-done exit.
func TestRunStopsOnCanceledContext(t *testing.T) {
	c, _, _, _ := newTestClient(t)
	p := c.NewProcessor(func(string, []any) error { return nil })
	cx, cancel := context.WithCancel(ctx())
	cancel()
	if err := p.Run(cx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// TestRunStopsOnError covers Run returning a ProcessOne error.
func TestRunStopsOnError(t *testing.T) {
	c, _, rdb, _ := newTestClient(t)
	addFault(rdb, &faultHook{failCmd: "brpop", err: errInjected})
	p := c.NewProcessor(func(string, []any) error { return nil })
	if err := p.Run(ctx()); !errors.Is(err, errInjected) {
		t.Fatalf("Run err = %v", err)
	}
}
