// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

// Redis key names, matching Sidekiq's layout exactly. A real Sidekiq process
// reads and writes these same keys.
const (
	// keyQueues is the set of known queue names (SADD on every enqueue).
	keyQueues = "queues"
	// queuePrefix is prepended to a queue name to form its list key, e.g.
	// queue:default. Jobs are LPUSH'd here and BRPOP'd off the tail.
	queuePrefix = "queue:"
	// keySchedule is the sorted-set of not-yet-due jobs, scored by run-at time.
	keySchedule = "schedule"
	// keyRetry is the sorted-set of failed jobs awaiting a retry, scored by the
	// next-attempt time.
	keyRetry = "retry"
	// keyDead is the sorted-set (morgue) of jobs whose retries are exhausted.
	keyDead = "dead"

	// statProcessed / statFailed are the lifetime job counters Sidekiq keeps.
	statProcessed = "stat:processed"
	statFailed    = "stat:failed"
)

// queueKey returns the Redis list key backing the named queue.
func queueKey(name string) string { return queuePrefix + name }

// DefaultQueue is the queue a job lands on when none is specified, matching
// Sidekiq's default.
const DefaultQueue = "default"

// DefaultMaxRetries is the number of retry attempts Sidekiq makes before a job
// is moved to the dead set when its retry option is true (Sidekiq's
// DEFAULT_MAX_RETRY_ATTEMPTS).
const DefaultMaxRetries = 25

// Dead-set trimming bounds, matching Sidekiq's DeadSet defaults.
const (
	// deadMaxJobs caps the number of jobs retained in the dead set.
	deadMaxJobs = 10000
	// deadTimeout is how long (in seconds) a dead job is retained: 6 months.
	deadTimeout = 180 * 24 * 60 * 60
)
