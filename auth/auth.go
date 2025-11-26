// Package auth provides authentication interfaces and helpers for Airport Flight servers.
package auth

import (
	"context"
)

// Authenticator validates bearer tokens and returns user identity.
// Implementations MUST be goroutine-safe.
type Authenticator interface {
	// Authenticate validates a bearer token and returns user identity.
	// Returns error if token is invalid or expired.
	// Identity string is used for authorization and logging.
	// Context allows timeout for auth backend calls.
	Authenticate(ctx context.Context, token string) (identity string, err error)
}

// noAuthenticator is an Authenticator that allows all requests.
// Used for development/testing. DO NOT use in production.
type noAuthenticator struct{}

// NoAuth returns an Authenticator that allows all requests.
// Useful for development/testing. DO NOT use in production.
func NoAuth() Authenticator {
	return &noAuthenticator{}
}

// Authenticate implements Authenticator for noAuthenticator.
// Always returns "anonymous" as the identity.
func (n *noAuthenticator) Authenticate(ctx context.Context, token string) (string, error) {
	return "anonymous", nil
}
