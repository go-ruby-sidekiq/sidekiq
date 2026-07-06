// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// baseTime is the fixed instant the deterministic test clock starts at. Its
// fractional half-second exercises the float epoch-seconds payload format.
var baseTime = time.Unix(1_700_000_000, 500_000_000).UTC()

// testClock is a settable clock injected via WithClock so tests advance time
// deterministically with no sleeps.
type testClock struct{ t time.Time }

func (c *testClock) now() time.Time  { return c.t }
func (c *testClock) set(t time.Time) { c.t = t }
func (c *testClock) add(d time.Duration) {
	c.t = c.t.Add(d)
}

// seqReader is a deterministic, inexhaustible entropy source: it fills each read
// with an incrementing byte so successive job ids differ predictably.
type seqReader struct{ n byte }

func (r *seqReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.n
		r.n++
	}
	return len(p), nil
}

// failReader always errors, used to exercise the job-id generation error path.
type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// faultHook injects Redis faults through a real go-redis client so error and
// edge branches are covered without a broken server. It can fail a named
// command (optionally only its failIndex-th occurrence) or force an integer
// reply of zero (to model a lost ZREM race).
type faultHook struct {
	failCmd   string // command name to fail, e.g. "lpush"
	failIndex int    // 1-based occurrence to fail; 0 = every occurrence
	err       error  // error to inject
	zeroCmd   string // command name whose integer reply is forced to 0
	seen      int    // occurrences of failCmd observed so far
}

func (h *faultHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *faultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := cmd.Name()
		if h.failCmd != "" && name == h.failCmd {
			h.seen++
			if h.failIndex == 0 || h.seen == h.failIndex {
				cmd.SetErr(h.err)
				return h.err
			}
		}
		if h.zeroCmd != "" && name == h.zeroCmd {
			if ic, ok := cmd.(*redis.IntCmd); ok {
				ic.SetVal(0)
			}
			return nil
		}
		return next(ctx, cmd)
	}
}

func (h *faultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// newTestClient wires a Client to an in-process miniredis (closed on cleanup)
// with a deterministic clock, entropy source and zero retry jitter. Extra
// options override these defaults. It returns the client, the miniredis handle,
// the go-redis client and the controllable clock.
func newTestClient(t *testing.T, opts ...Option) (*Client, *miniredis.Miniredis, *redis.Client, *testClock) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	tc := &testClock{t: baseTime}
	base := []Option{
		WithClock(tc.now),
		WithRand(&seqReader{}),
		WithJitter(func(int) int { return 0 }),
	}
	c := New(rdb, append(base, opts...)...)
	return c, mr, rdb, tc
}

// addFault attaches a fault hook to the go-redis client.
func addFault(rdb *redis.Client, h *faultHook) { rdb.AddHook(h) }

// miniredisClient returns a go-redis client bound to a fresh in-process
// miniredis, closed on cleanup. Used to build a Client with production defaults.
func miniredisClient(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// ctx is a convenience background context for tests.
func ctx() context.Context { return context.Background() }
