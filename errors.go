// Copyright (c) the go-ruby-sidekiq/sidekiq authors
//
// SPDX-License-Identifier: BSD-3-Clause

package sidekiq

import "reflect"

// ClassedError lets a host carry the Ruby exception class name of a failed job
// through the Go error returned by a [Perform] seam. When a job's perform body
// (Ruby) raises, the host wraps the raised exception in an error implementing
// this interface so the retry machinery records the true Ruby class name in the
// payload's error_class field, matching Sidekiq.
type ClassedError interface {
	error
	// ErrorClass returns the Ruby exception class name, e.g. "RuntimeError".
	ErrorClass() string
}

// errorClass returns the class name recorded for a failed job. If the error
// implements [ClassedError] its reported class is used (the Ruby exception
// class); otherwise the concrete Go type name is used as a best-effort stand-in.
func errorClass(err error) string {
	if ce, ok := err.(ClassedError); ok {
		return ce.ErrorClass()
	}
	t := reflect.TypeOf(err)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.String()
}
