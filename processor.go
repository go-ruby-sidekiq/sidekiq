// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Perform is the host seam that runs a job's body. Given a worker class name
// and its decoded arguments it executes the work and returns nil on success or
// an error on failure. This is the one inherently interpreter-dependent piece
// of Sidekiq — a worker's perform method is Ruby — so it is injected: a host
// such as go-embedded-ruby dispatches (class, args) to the matching Ruby
// perform body. To record the true Ruby exception class on a retry, the
// returned error may implement [ClassedError].
type Perform func(class string, args []any) error

// Processor is the pure-Go model of a Sidekiq server process: it fetches a job
// off a queue, decodes it and invokes the [Perform] seam, then does Sidekiq's
// success/failure bookkeeping (processed/failed counters, retry scheduling and
// the dead set). It spawns no goroutines; a host drives it by calling
// [Processor.ProcessOne] or [Processor.Run].
type Processor struct {
	client  *Client
	perform Perform
	queues  []string
	// timeout is the BRPOP block time used to wait for a job.
	timeout time.Duration
}

// NewProcessor builds a Processor that fetches from the given queues (in
// priority order, highest first) and runs jobs through perform. With no queues
// it defaults to [DefaultQueue]. The BRPOP wait defaults to one second and can
// be tuned with [Processor.SetTimeout].
func (c *Client) NewProcessor(perform Perform, queues ...string) *Processor {
	if len(queues) == 0 {
		queues = []string{DefaultQueue}
	}
	return &Processor{
		client:  c,
		perform: perform,
		queues:  queues,
		timeout: time.Second,
	}
}

// SetTimeout sets the BRPOP block duration used when waiting for a job.
func (p *Processor) SetTimeout(d time.Duration) { p.timeout = d }

// queueKeys returns the fetch order (queue:<name> for each configured queue).
func (p *Processor) queueKeys() []string {
	keys := make([]string, len(p.queues))
	for i, q := range p.queues {
		keys[i] = queueKey(q)
	}
	return keys
}

// ProcessOne fetches at most one job (BRPOP across the configured queues,
// highest priority first) and runs it. It reports whether a job was processed;
// a false result with a nil error means the fetch timed out with no work. Any
// error from the job's perform body is handled internally (retry or dead set)
// and is not returned; the returned error is reserved for Redis/transport
// failures.
func (p *Processor) ProcessOne(ctx context.Context) (processed bool, err error) {
	res, err := p.client.rdb.BRPop(ctx, p.timeout, p.queueKeys()...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	// res is [queueKey, payload].
	payload := res[1]
	j, err := decodeJob(payload)
	if err != nil {
		return false, err
	}
	if err := p.run(ctx, j); err != nil {
		return true, err
	}
	return true, nil
}

// run invokes the perform seam and records the outcome, mirroring the Sidekiq
// processor: the processed counter is bumped for every attempt, and on failure
// the failed counter is bumped and the retry machinery engaged.
func (p *Processor) run(ctx context.Context, j *job) error {
	jobErr := p.perform(j.Class, j.Args)
	if incErr := p.client.rdb.Incr(ctx, statProcessed).Err(); incErr != nil {
		return incErr
	}
	if jobErr == nil {
		return nil
	}
	if incErr := p.client.rdb.Incr(ctx, statFailed).Err(); incErr != nil {
		return incErr
	}
	_, err := p.client.handleFailure(ctx, j, jobErr)
	return err
}

// Run drives the processor until ctx is cancelled, fetching and running jobs in
// a loop. It returns ctx.Err() when the context is done, or any Redis/transport
// error encountered. It runs entirely on the calling goroutine — no background
// goroutines are started.
func (p *Processor) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := p.ProcessOne(ctx); err != nil {
			return err
		}
	}
}
