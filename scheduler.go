// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// EnqueueScheduledJobs moves every job in the schedule and retry sorted-sets
// whose run-at time is now due (score <= now) onto its target queue. It is the
// pure-Go model of Sidekiq's Scheduled::Poller#enqueue: a host calls it on
// whatever cadence it likes, and it advances deterministically against the
// Client's injected clock — no sleeps, no background goroutine. It returns the
// number of jobs enqueued.
func (c *Client) EnqueueScheduledJobs(ctx context.Context) (int, error) {
	total := 0
	for _, set := range []string{keySchedule, keyRetry} {
		n, err := c.enqueueDue(ctx, set)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// enqueueDue drains the due members of one sorted-set. For each member scored
// at or before now it atomically claims the job with ZREM (so that a concurrent
// poller cannot double-enqueue it — only the caller whose ZREM removed the
// member proceeds) and pushes it onto its queue.
func (c *Client) enqueueDue(ctx context.Context, set string) (int, error) {
	now := scoreString(timeToScore(c.clock()))
	members, err := c.rdb.ZRangeByScore(ctx, set, &redis.ZRangeBy{
		Min: "-inf",
		Max: now,
	}).Result()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, payload := range members {
		removed, err := c.rdb.ZRem(ctx, set, payload).Result()
		if err != nil {
			return count, err
		}
		if removed == 0 {
			// Another poller claimed this job first; skip it.
			continue
		}
		j, err := decodeJob(payload)
		if err != nil {
			return count, err
		}
		if err := c.enqueue(ctx, j); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
