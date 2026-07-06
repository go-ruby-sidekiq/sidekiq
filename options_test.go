// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"testing"
)

// TestWithMaxRetries covers the non-default max-retries branch of applyDefaults.
func TestWithMaxRetries(t *testing.T) {
	c := New(miniredisClient(t), WithMaxRetries(3))
	if c.maxRetries != 3 {
		t.Fatalf("maxRetries = %d", c.maxRetries)
	}
}

// TestDefaultJitter covers the production jitter: a value in [0, 30*(count+1)).
func TestDefaultJitter(t *testing.T) {
	for count := 0; count < 5; count++ {
		j := defaultJitter(count)
		if j < 0 || j >= 30*(count+1) {
			t.Fatalf("defaultJitter(%d) = %d out of range", count, j)
		}
	}
}
