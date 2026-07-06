// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// secondsToDelay is Sidekiq's exponential retry backoff: count**4 + 15 plus
// jitter (an added random number of seconds, injected via WithJitter). The
// result is the number of seconds to wait before the next attempt.
func (c *Client) secondsToDelay(count int) int {
	return count*count*count*count + 15 + c.jitter(count)
}

// handleFailure records a failed job attempt exactly as Sidekiq's JobRetry
// does: it stamps the error and attempt bookkeeping onto the payload, then
// either schedules a retry (ZADD to the retry set at the backed-off time) or,
// once attempts are exhausted, moves the job to the dead set. A job whose retry
// option is false is neither retried nor buried — the failure simply
// propagates. It reports whether the job was retried.
func (c *Client) handleFailure(ctx context.Context, j *job, jobErr error) (retried bool, err error) {
	max, retryable := j.maxRetries(c.maxRetries)
	if !retryable {
		return false, nil
	}

	j.ErrorClass = errorClass(jobErr)
	j.ErrorMessage = jobErr.Error()

	var count int
	if j.RetryCount != nil {
		count = *j.RetryCount + 1
		c.setRetryCount(j, count)
		j.RetriedAt = timeToScore(c.clock())
	} else {
		count = 0
		c.setRetryCount(j, 0)
		j.FailedAt = timeToScore(c.clock())
	}

	if count < max {
		return true, c.scheduleRetry(ctx, j, count)
	}
	return false, c.kill(ctx, j)
}

// setRetryCount sets the retry_count field to n (via a fresh pointer so the
// zero value is still emitted in the payload).
func (c *Client) setRetryCount(j *job, n int) {
	v := n
	j.RetryCount = &v
}

// scheduleRetry ZADDs the job to the retry set at now + backoff(count).
func (c *Client) scheduleRetry(ctx context.Context, j *job, count int) error {
	delay := c.secondsToDelay(count)
	retryAt := timeToScore(c.clock()) + float64(delay)
	payload, err := j.marshal()
	if err != nil {
		return err
	}
	return c.rdb.ZAdd(ctx, keyRetry, redis.Z{Score: retryAt, Member: payload}).Err()
}

// kill moves a job to the dead set (morgue) and trims it to Sidekiq's bounds:
// members older than deadTimeout and everything beyond deadMaxJobs are removed.
func (c *Client) kill(ctx context.Context, j *job) error {
	now := timeToScore(c.clock())
	payload, err := j.marshal()
	if err != nil {
		return err
	}
	if err := c.rdb.ZAdd(ctx, keyDead, redis.Z{Score: now, Member: payload}).Err(); err != nil {
		return err
	}
	cutoff := scoreString(now - deadTimeout)
	if err := c.rdb.ZRemRangeByScore(ctx, keyDead, "-inf", "("+cutoff).Err(); err != nil {
		return err
	}
	return c.rdb.ZRemRangeByRank(ctx, keyDead, 0, -deadMaxJobs-1).Err()
}
