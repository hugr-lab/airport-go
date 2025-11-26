// Package recovery provides panic recovery middleware for Flight RPC handlers.
// Ensures user-provided catalog implementations don't crash the server.
package recovery

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoverToError wraps a function call with panic recovery.
// If the function panics, converts the panic to a gRPC error.
// Use this to wrap user-provided functions (scan functions, catalog methods).
//
// Example:
//
//	err := recovery.RecoverToError(logger, "Scan", func() error {
//	    return table.Scan(ctx, opts)
//	})
func RecoverToError(logger *slog.Logger, operation string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Capture stack trace
			stack := debug.Stack()

			// Log the panic with stack trace
			logger.Error("Panic recovered",
				"operation", operation,
				"panic", r,
				"stack", string(stack),
			)

			// Convert panic to gRPC error
			err = status.Errorf(codes.Internal,
				"%s panicked: %v", operation, r)
		}
	}()

	return fn()
}

// RecoverToValue wraps a function that returns a value and error.
// If the function panics, returns zero value and error.
//
// Example:
//
//	result, err := recovery.RecoverToValue(logger, "GetSchema", func() (*Schema, error) {
//	    return catalog.Schema(ctx, name)
//	})
func RecoverToValue[T any](logger *slog.Logger, operation string, fn func() (T, error)) (result T, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Capture stack trace
			stack := debug.Stack()

			// Log the panic
			logger.Error("Panic recovered",
				"operation", operation,
				"panic", r,
				"stack", string(stack),
			)

			// Return zero value and error
			var zero T
			result = zero
			err = fmt.Errorf("%s panicked: %v", operation, r)
		}
	}()

	return fn()
}

// Recover wraps a void function with panic recovery.
// Logs the panic but doesn't return an error.
// Use for cleanup operations where errors can't be returned.
func Recover(logger *slog.Logger, operation string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()

			logger.Error("Panic recovered in cleanup",
				"operation", operation,
				"panic", r,
				"stack", string(stack),
			)
		}
	}()

	fn()
}
