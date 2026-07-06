// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

// Package sidekiq is a pure-Go (no cgo) reimplementation of the core engine of
// the Ruby [Sidekiq] background-job framework: the client that enqueues jobs,
// the job/worker option model, the scheduled-job poller and a basic
// server/processor — backed by Redis.
//
// It writes the exact same Redis data structures a real Sidekiq process does,
// so a real Sidekiq server can consume jobs this package enqueues and vice
// versa. The job payload is the documented Sidekiq JSON hash (class, args,
// queue, jid, created_at, enqueued_at, retry) LPUSH'd to queue:<name> with the
// queue name SADD'd to the queues set; scheduled jobs are ZADD'd to the
// schedule sorted-set keyed by their run-at timestamp; retries land in the
// retry sorted-set and exhausted jobs in the dead set — all byte-compatible
// with Sidekiq.
//
// The only piece that is inherently interpreter-dependent is the body of a job
// (a Worker's perform method, which is Ruby). That is modelled as an injected
// host seam — a [Perform] function — mirroring the go-ruby-* design: the
// deterministic enqueue/schedule/retry machinery lives here in pure Go, and the
// host (for example go-embedded-ruby) supplies the perform bodies. The Redis
// socket itself is likewise a seam: the caller supplies a go-redis client.
//
// Time, randomness (the 24-hex jid) and retry jitter are all injectable so the
// whole engine runs deterministically under test with no real Redis server, no
// sleeps and no background goroutines.
//
// [Sidekiq]: https://github.com/sidekiq/sidekiq
package sidekiq
