// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"
	"time"
)

// WorkerOptions models a worker's sidekiq_options declaration: the per-class
// defaults applied to every job it enqueues. A zero value means "use the
// engine defaults" (queue "default", retry true).
type WorkerOptions struct {
	// Queue is the queue jobs of this worker run on; empty means [DefaultQueue].
	Queue string
	// Retry is the retry policy: nil (default true), a bool, or an int max
	// attempt count, matching sidekiq_options retry:.
	Retry any
}

// Worker binds a worker class name and its options to a [Client], exposing the
// familiar perform_async / perform_in / perform_at enqueue methods. It is the
// Go analogue of a class that includes Sidekiq::Job and calls sidekiq_options.
type Worker struct {
	client *Client
	class  string
	opts   WorkerOptions
}

// Worker returns a [Worker] for the named class with the given options. This
// mirrors declaring a class that includes Sidekiq::Job and calls
// sidekiq_options(...).
func (c *Client) Worker(class string, opts WorkerOptions) *Worker {
	return &Worker{client: c, class: class, opts: opts}
}

// item builds the base [Item] for this worker from its options and the given
// args.
func (w *Worker) item(args []any) Item {
	return Item{
		Class: w.class,
		Args:  args,
		Queue: w.opts.Queue,
		Retry: w.opts.Retry,
	}
}

// PerformAsync enqueues the job to run now (Sidekiq's perform_async) and
// returns its jid.
func (w *Worker) PerformAsync(ctx context.Context, args ...any) (string, error) {
	return w.client.Push(ctx, w.item(args))
}

// PerformAt schedules the job to run at time t (Sidekiq's perform_at) and
// returns its jid.
func (w *Worker) PerformAt(ctx context.Context, t time.Time, args ...any) (string, error) {
	it := w.item(args)
	it.At = t
	return w.client.Push(ctx, it)
}

// PerformIn schedules the job to run after the given interval from now
// (Sidekiq's perform_in). A non-positive interval enqueues immediately, exactly
// as Sidekiq does.
func (w *Worker) PerformIn(ctx context.Context, interval time.Duration, args ...any) (string, error) {
	if interval <= 0 {
		return w.PerformAsync(ctx, args...)
	}
	return w.PerformAt(ctx, w.client.clock().Add(interval), args...)
}
