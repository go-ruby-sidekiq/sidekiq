// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	cryptorand "crypto/rand"
	"io"
	mathrand "math/rand/v2"
	"time"
)

// Option configures a [Client]. All the injectable seams (clock, randomness,
// retry jitter) default to production values, so New(rdb) yields a fully
// working client; tests override them for determinism.
type Option func(*Client)

// WithClock injects the time source used for created_at / enqueued_at stamps,
// schedule scoring and retry timing. It defaults to [time.Now].
func WithClock(clock func() time.Time) Option {
	return func(c *Client) { c.clock = clock }
}

// WithRand injects the entropy source for generating the 24-hex job id. It
// defaults to [crypto/rand.Reader].
func WithRand(r io.Reader) Option {
	return func(c *Client) { c.rand = r }
}

// WithJitter injects the retry-jitter function. Given the current retry count
// it returns an added delay in seconds; Sidekiq's default is a random value in
// [0, 30*(count+1)). It defaults to the randomised production jitter.
func WithJitter(jitter func(count int) int) Option {
	return func(c *Client) { c.jitter = jitter }
}

// WithMaxRetries sets the default maximum retry attempts for jobs whose retry
// option is true. It defaults to [DefaultMaxRetries].
func WithMaxRetries(n int) Option {
	return func(c *Client) { c.maxRetries = n }
}

// applyDefaults fills in any seam the caller did not override.
func (c *Client) applyDefaults() {
	if c.clock == nil {
		c.clock = time.Now
	}
	if c.rand == nil {
		c.rand = cryptorand.Reader
	}
	if c.jitter == nil {
		c.jitter = defaultJitter
	}
	if c.maxRetries == 0 {
		c.maxRetries = DefaultMaxRetries
	}
}

// defaultJitter is Sidekiq's retry jitter: a random number of seconds in the
// half-open interval [0, 30*(count+1)).
func defaultJitter(count int) int {
	return mathrand.IntN(30 * (count + 1))
}
