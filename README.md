<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-sidekiq/brand/main/social/go-ruby-sidekiq-sidekiq.png" alt="go-ruby-sidekiq/sidekiq" width="720"></p>

# sidekiq — go-ruby-sidekiq

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-sidekiq.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) reimplementation of the core engine of the
[`sidekiq`](https://github.com/sidekiq/sidekiq) background-job framework** — the
client that enqueues jobs, the job/worker option model, the scheduled-job poller
and a basic server/processor, all backed by Redis. It writes the **exact same
Redis data structures a real Sidekiq process does**, so a real Sidekiq server can
consume jobs this package enqueues, and this package can process jobs a real
Sidekiq client wrote — the payloads are byte-compatible.

It is the Sidekiq backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), but is a
**standalone, reusable** module — a sibling of
[go-ruby-redis](https://github.com/go-ruby-redis/redis),
[go-ruby-regexp](https://github.com/go-ruby-regexp/regexp) and
[go-ruby-marshal](https://github.com/go-ruby-marshal/marshal).

> **What it is — and isn't.** Enqueue, scheduling, retry backoff and the dead-set
> machinery are fully deterministic and live here as pure Go. The two
> interpreter-/host-dependent pieces are **seams**: a job's `perform` body is Ruby,
> injected as a [`Perform`](processor.go) function; and the Redis **socket** is a
> [go-redis](https://github.com/redis/go-redis) client the caller supplies. Time,
> the 24-hex job id and retry jitter are injectable too, so the whole engine runs
> deterministically under test with **no real Redis server, no sleeps and no
> background goroutines**.

## Features

- **Byte-compatible payload** — `Client.Push` writes the documented Sidekiq job
  hash `{"retry","queue","class","args","jid","created_at","enqueued_at"}` (jid =
  24 hex chars, `SecureRandom.hex(12)`; timestamps = float epoch seconds,
  `Time.now.to_f`) `LPUSH`'d to `queue:<name>`, with the queue name `SADD`'d to
  the `queues` set — exactly as Sidekiq's client does.
- **Bulk enqueue** — `Client.PushBulk` mirrors `Sidekiq::Client#push_bulk`.
- **Worker options** — `Client.Worker(class, WorkerOptions{Queue, Retry})` models
  `sidekiq_options`, exposing `PerformAsync` (enqueue now), `PerformAt` and
  `PerformIn` (`ZADD` to the `schedule` sorted-set scored by run-at time).
- **Scheduler / poller** — `Client.EnqueueScheduledJobs` moves due jobs from the
  `schedule` and `retry` sorted-sets onto their queues, claiming each with `ZREM`
  so concurrent pollers never double-enqueue. Deterministic against an injected
  clock; no sleeps, no goroutine.
- **Server / processor** — `Client.NewProcessor(perform, queues...)` `BRPOP`s a
  job, decodes it and invokes the `Perform` seam, then does Sidekiq's
  bookkeeping: bumps `stat:processed` / `stat:failed`, and on failure schedules a
  retry (`ZADD retry`) with Sidekiq's exact backoff `count⁴ + 15 + jitter`, or
  moves the job to the `dead` set (trimmed to 10 000 jobs / 6 months) once
  attempts are exhausted. `retry_count`, `failed_at`, `retried_at`,
  `error_class` and `error_message` are recorded on the payload exactly as
  Sidekiq records them.
- **Stats** — `Client.Stats` snapshots the `Sidekiq::Stats` counters and set
  sizes (processed, failed, enqueued, scheduled, retries, dead).
- **CGO-free** and validated on all six supported 64-bit targets (amd64, arm64,
  riscv64, loong64, ppc64le, s390x) across Linux, macOS and Windows.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-ruby-sidekiq/sidekiq"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	c := sidekiq.New(rdb)

	// Enqueue via a worker (models a class with sidekiq_options).
	mailer := c.Worker("EmailJob", sidekiq.WorkerOptions{Queue: "mailers", Retry: 3})
	jid, _ := mailer.PerformAsync(ctx, "user@example.com")
	fmt.Println("enqueued", jid)

	// Schedule for later (ZADD to the schedule set).
	mailer.PerformIn(ctx, 5*time.Minute, "later@example.com")

	// Move due scheduled/retry jobs onto their queues (call on a cadence).
	c.EnqueueScheduledJobs(ctx)

	// Run jobs. The perform body is the host seam (Ruby, in go-embedded-ruby).
	perform := func(class string, args []any) error {
		fmt.Printf("run %s%v\n", class, args)
		return nil // return an error to trigger retry/dead handling
	}
	p := c.NewProcessor(perform, "mailers", "default")
	p.ProcessOne(ctx) // or p.Run(ctx) to loop until the context is cancelled
}
```

## API

```go
func New(rdb redis.Cmdable, opts ...Option) *Client

// options (all injectable for deterministic testing)
func WithClock(func() time.Time) Option   // created_at / enqueued_at / scoring
func WithRand(io.Reader) Option            // 24-hex jid entropy
func WithJitter(func(count int) int) Option // retry jitter (seconds)
func WithMaxRetries(int) Option            // default max attempts (25)

// client
func (c *Client) Push(ctx, Item) (jid string, err error)
func (c *Client) PushBulk(ctx, Item, argsList [][]any) (jids []string, err error)
func (c *Client) Worker(class string, opts WorkerOptions) *Worker
func (c *Client) EnqueueScheduledJobs(ctx) (moved int, err error)
func (c *Client) NewProcessor(perform Perform, queues ...string) *Processor
func (c *Client) Stats(ctx) (Stats, error)

// worker (models sidekiq_options + perform_async / perform_at / perform_in)
func (w *Worker) PerformAsync(ctx, args ...any) (jid string, err error)
func (w *Worker) PerformAt(ctx, t time.Time, args ...any) (jid string, err error)
func (w *Worker) PerformIn(ctx, d time.Duration, args ...any) (jid string, err error)

// processor (the server)
type Perform func(class string, args []any) error
func (p *Processor) SetTimeout(d time.Duration)
func (p *Processor) ProcessOne(ctx) (processed bool, err error)
func (p *Processor) Run(ctx) error

// carry a Ruby exception class name through a failed job's error
type ClassedError interface { error; ErrorClass() string }
```

The `Perform` seam is the one inherently interpreter-dependent piece — a worker's
`perform` method is Ruby — so a host (for example go-embedded-ruby) dispatches
`(class, args)` to the matching Ruby body. To record the true Ruby exception
class on a retry, the returned error may implement `ClassedError`; otherwise the
Go type name is used.

## Tests & coverage

The suite runs entirely against
[`miniredis`](https://github.com/alicebob/miniredis) — a pure-Go, in-process
Redis started per test and closed in `t.Cleanup`, so there is **no external Redis
server and no leaked goroutines**. miniredis is strictly test-scoped (imported
only from `_test.go`); the sole runtime dependency is the go-redis **client**.
Time, randomness and retry jitter are injected, so payload byte-exactness,
scheduled→enqueue promotion and process→success/retry/dead transitions are all
asserted deterministically, with every Redis transport-error branch driven via a
go-redis fault hook.

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

CGO-free, `gofmt` + `go vet` clean, `-race` clean, **100% line coverage**, and
green across the six 64-bit Go targets (amd64, arm64, riscv64, loong64, ppc64le,
s390x) and three OSes (Linux, macOS, Windows).

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-ruby-sidekiq/sidekiq
authors.
