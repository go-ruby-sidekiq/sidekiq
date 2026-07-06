// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// Stats is a snapshot of the engine's global counters and set sizes, modelling
// Sidekiq::Stats: lifetime processed/failed totals, the number of jobs waiting
// across all queues, and the sizes of the schedule, retry and dead sets.
type Stats struct {
	// Processed is the lifetime count of processed jobs (stat:processed).
	Processed int64
	// Failed is the lifetime count of failed jobs (stat:failed).
	Failed int64
	// Enqueued is the total number of jobs waiting across all known queues.
	Enqueued int64
	// Scheduled, Retries and Dead are the cardinalities of the schedule, retry
	// and dead sorted-sets.
	Scheduled int64
	Retries   int64
	Dead      int64
}

// Stats gathers a [Stats] snapshot from Redis.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var err error

	if s.Processed, err = c.counter(ctx, statProcessed); err != nil {
		return s, err
	}
	if s.Failed, err = c.counter(ctx, statFailed); err != nil {
		return s, err
	}
	if s.Enqueued, err = c.enqueuedTotal(ctx); err != nil {
		return s, err
	}
	if s.Scheduled, err = c.rdb.ZCard(ctx, keySchedule).Result(); err != nil {
		return s, err
	}
	if s.Retries, err = c.rdb.ZCard(ctx, keyRetry).Result(); err != nil {
		return s, err
	}
	if s.Dead, err = c.rdb.ZCard(ctx, keyDead).Result(); err != nil {
		return s, err
	}
	return s, nil
}

// counter reads an integer stat key, treating a missing key as zero.
func (c *Client) counter(ctx context.Context, key string) (int64, error) {
	v, err := c.rdb.Get(ctx, key).Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

// enqueuedTotal sums the lengths of every known queue's list.
func (c *Client) enqueuedTotal(ctx context.Context) (int64, error) {
	queues, err := c.rdb.SMembers(ctx, keyQueues).Result()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, q := range queues {
		n, err := c.rdb.LLen(ctx, queueKey(q)).Result()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
