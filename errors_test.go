// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import (
	"errors"
	"testing"
)

// classedErr implements ClassedError, carrying a Ruby exception class name.
type classedErr struct{ class, msg string }

func (e classedErr) Error() string      { return e.msg }
func (e classedErr) ErrorClass() string { return e.class }

// valueErr is a non-pointer error type, exercising the non-pointer reflect
// branch of errorClass.
type valueErr string

func (e valueErr) Error() string { return string(e) }

// TestErrorClass covers all three classification branches.
func TestErrorClass(t *testing.T) {
	// ClassedError: the Ruby class is used verbatim.
	if got := errorClass(classedErr{class: "ArgumentError", msg: "bad"}); got != "ArgumentError" {
		t.Fatalf("classed = %q", got)
	}
	// Pointer error type (errors.New → *errors.errorString): the element name.
	if got := errorClass(errors.New("x")); got != "errors.errorString" {
		t.Fatalf("pointer = %q", got)
	}
	// Non-pointer error type: the type name directly.
	if got := errorClass(valueErr("y")); got != "sidekiq.valueErr" {
		t.Fatalf("value = %q", got)
	}
}

// TestErrorClassRecordedOnFailure confirms a ClassedError's class lands in the
// payload's error_class field.
func TestErrorClassRecordedOnFailure(t *testing.T) {
	c, _, _, _ := newTestClient(t)
	j := newJob(true)
	if _, err := c.handleFailure(ctx(), j, classedErr{class: "RuntimeError", msg: "boom"}); err != nil {
		t.Fatal(err)
	}
	if j.ErrorClass != "RuntimeError" {
		t.Fatalf("error_class = %q", j.ErrorClass)
	}
}
