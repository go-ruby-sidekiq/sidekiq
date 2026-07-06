// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"
	"encoding/hex"
	"io"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client enqueues jobs into Redis exactly as Sidekiq's client does. It owns no
// socket: the caller supplies a go-redis client (a [redis.Cmdable], which a
// *redis.Client satisfies), mirroring the go-ruby-* host-seam design. Time,
// randomness and retry jitter are injectable via [Option] for deterministic
// testing.
//
// A Client is safe for concurrent use to the same extent its underlying
// go-redis client is.
type Client struct {
	rdb        redis.Cmdable
	clock      func() time.Time
	rand       io.Reader
	jitter     func(count int) int
	maxRetries int
}

// New builds a Client over the supplied go-redis client. With no options it is
// production-ready: real time, crypto/rand job ids and randomised retry jitter.
func New(rdb redis.Cmdable, opts ...Option) *Client {
	c := &Client{rdb: rdb}
	for _, opt := range opts {
		opt(c)
	}
	c.applyDefaults()
	return c
}

// Item describes a job to enqueue. Only Class is required; Queue defaults to
// [DefaultQueue], Retry defaults to true, Jid is generated when empty, and a
// non-zero At schedules the job for that time rather than enqueuing it now.
type Item struct {
	// Class is the worker class name whose perform will run.
	Class string
	// Args are the positional arguments passed to perform.
	Args []any
	// Queue is the target queue; empty means [DefaultQueue].
	Queue string
	// Retry is the retry policy: nil (default true), a bool, or an int max
	// attempt count.
	Retry any
	// At, when non-zero, schedules the job to run at that time (ZADD to the
	// schedule set) instead of enqueuing it immediately.
	At time.Time
	// Jid is the job id; when empty a fresh 24-hex id is generated.
	Jid string
}

// generateJID returns a fresh 24-character hex job id, matching Ruby's
// SecureRandom.hex(12).
func (c *Client) generateJID() (string, error) {
	var b [12]byte
	if _, err := io.ReadFull(c.rand, b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// normalize turns an Item into a fully-populated job, applying Sidekiq's
// defaults (queue, retry, jid, created_at).
func (c *Client) normalize(item Item) (*job, error) {
	jid := item.Jid
	if jid == "" {
		var err error
		if jid, err = c.generateJID(); err != nil {
			return nil, err
		}
	}
	queue := item.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	retry := item.Retry
	if retry == nil {
		retry = true
	}
	args := item.Args
	if args == nil {
		args = []any{}
	}
	return &job{
		Retry:     retry,
		Queue:     queue,
		Class:     item.Class,
		Args:      args,
		Jid:       jid,
		CreatedAt: timeToScore(c.clock()),
	}, nil
}

// Push enqueues a single job and returns its jid. A job with a zero At is
// pushed onto its queue immediately; a job with a non-zero At is added to the
// schedule set to run later. This mirrors Sidekiq::Client#push.
func (c *Client) Push(ctx context.Context, item Item) (string, error) {
	j, err := c.normalize(item)
	if err != nil {
		return "", err
	}
	if item.At.IsZero() {
		if err := c.enqueue(ctx, j); err != nil {
			return "", err
		}
	} else {
		if err := c.schedule(ctx, j, item.At); err != nil {
			return "", err
		}
	}
	return j.Jid, nil
}

// PushBulk enqueues many jobs that share a class/queue/retry policy, one per
// entry in argsList, and returns their jids in order. It mirrors
// Sidekiq::Client#push_bulk. A non-zero At schedules every job for that time.
func (c *Client) PushBulk(ctx context.Context, item Item, argsList [][]any) ([]string, error) {
	jids := make([]string, 0, len(argsList))
	for _, args := range argsList {
		it := item
		it.Args = args
		it.Jid = ""
		jid, err := c.Push(ctx, it)
		if err != nil {
			return jids, err
		}
		jids = append(jids, jid)
	}
	return jids, nil
}

// enqueue LPUSHes a job onto its queue and records the queue name, stamping
// enqueued_at. This is Sidekiq's atomic_push for the immediate case.
func (c *Client) enqueue(ctx context.Context, j *job) error {
	j.EnqueuedAt = timeToScore(c.clock())
	payload, err := j.marshal()
	if err != nil {
		return err
	}
	if err := c.rdb.SAdd(ctx, keyQueues, j.Queue).Err(); err != nil {
		return err
	}
	return c.rdb.LPush(ctx, queueKey(j.Queue), payload).Err()
}

// schedule ZADDs a job to the schedule set scored by its run-at time. The
// stored payload carries no enqueued_at (Sidekiq deletes it), which the job's
// zero EnqueuedAt omits automatically.
func (c *Client) schedule(ctx context.Context, j *job, at time.Time) error {
	payload, err := j.marshal()
	if err != nil {
		return err
	}
	return c.rdb.ZAdd(ctx, keySchedule, redis.Z{
		Score:  timeToScore(at),
		Member: payload,
	}).Err()
}
