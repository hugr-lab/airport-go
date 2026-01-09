// Package auth provides authentication interfaces and helpers for Airport Flight servers.
package auth

import (
	"context"
	"errors"
	"strings"
)

var (
	// ErrInvalidAuthHeader is returned when the authorization header is malformed.
	ErrInvalidAuthHeader = errors.New("authorization header must use Bearer scheme")

	// ErrTokenNotFound is returned when no authorization token is found in context.
	ErrTokenIsEmpty = errors.New("authorization token is empty")

	// ErrUnauthenticated is returned when authentication fails.
	ErrUnauthenticated = errors.New("unauthenticated")
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

// CatalogAuthorizer is an optional interface that Authenticator implementations
// can also implement to provide per-catalog authorization.
//
// When an Authenticator also implements CatalogAuthorizer:
//  1. Authenticate(ctx, token) is called first to validate the token
//  2. AuthorizeCatalog(ctx, catalog, token) is called to authorize catalog access
//
// This allows separating authentication (who are you?) from authorization
// (can you access this catalog?).
type CatalogAuthorizer interface {
	// AuthorizeCatalog authorizes access to a specific catalog.
	// Called after successful Authenticate() to check catalog-level permissions.
	// Parameters:
	//   - ctx: Request context with identity already set from Authenticate()
	//   - catalog: Target catalog name (empty string for default)
	//   - token: Bearer token (same as passed to Authenticate)
	// Returns:
	//   - ctx: Potentially enriched context (e.g., with catalog-specific claims)
	//   - err: Non-nil if authorization fails (returns gRPC PermissionDenied status)
	AuthorizeCatalog(ctx context.Context, catalog string) (context.Context, error)
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

// contextKey is a private type for context keys to avoid collisions.
type contextKey int

const (
	identityKey contextKey = iota
)

// IdentityFromContext retrieves the authenticated user identity from context.
// Returns empty string if no identity is set (unauthenticated request).
func IdentityFromContext(ctx context.Context) string {
	val, ok := ctx.Value(identityKey).(string)
	if !ok {
		return ""
	}
	return val
}

// WithIdentity adds the authenticated user identity to the context.
func WithIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, identityKey, identity)
}

const bearerPrefix = "Bearer "

func TokenFromAuthorizationHeader(authHeader string) (string, error) {
	// Expected format: "Bearer <token>"
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return "", ErrInvalidAuthHeader
	}

	token := strings.TrimPrefix(authHeader, bearerPrefix)
	if token == "" {
		return "", ErrTokenIsEmpty
	}
	return token, nil
}

// ValidateToken validates a bearer token using the provided Authenticator.
// Returns context with identity set or error.
func ValidateToken(ctx context.Context, token string, authenticator Authenticator) (context.Context, error) {
	if token == "" {
		return ctx, ErrTokenIsEmpty
	}

	identity, err := authenticator.Authenticate(ctx, token)
	if err != nil {
		return ctx, ErrUnauthenticated
	}

	// Add identity to context
	return WithIdentity(ctx, identity), nil
}
