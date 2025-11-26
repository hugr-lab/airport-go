package auth

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// contextKey is a private type for context keys to avoid collisions.
type contextKey int

const (
	// identityKey is the context key for storing authenticated user identity.
	identityKey contextKey = iota
)

// WithIdentity returns a new context with the given user identity.
// Used by auth interceptors to propagate authenticated user info.
func WithIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, identityKey, identity)
}

// IdentityFromContext retrieves the authenticated user identity from context.
// Returns empty string if no identity is set (unauthenticated request).
func IdentityFromContext(ctx context.Context) string {
	identity, ok := ctx.Value(identityKey).(string)
	if !ok {
		return ""
	}
	return identity
}

// ExtractToken extracts the bearer token from gRPC metadata.
// Looks for "authorization" header with "Bearer <token>" format.
// Returns empty string if header is missing or malformed.
func ExtractToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil // No metadata, no token
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", nil // No authorization header
	}

	// Use first authorization header
	authHeader := authHeaders[0]

	// Expected format: "Bearer <token>"
	const bearerPrefix = "Bearer "
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return "", status.Error(codes.Unauthenticated, "authorization header must use Bearer scheme")
	}

	token := strings.TrimPrefix(authHeader, bearerPrefix)
	if token == "" {
		return "", status.Error(codes.Unauthenticated, "bearer token is empty")
	}

	return token, nil
}

// ValidateToken validates a bearer token using the provided Authenticator.
// Returns context with identity set, or error with appropriate gRPC status code.
func ValidateToken(ctx context.Context, token string, authenticator Authenticator) (context.Context, error) {
	if token == "" {
		return ctx, status.Error(codes.Unauthenticated, "missing bearer token")
	}

	identity, err := authenticator.Authenticate(ctx, token)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, fmt.Sprintf("invalid token: %v", err))
	}

	// Add identity to context
	return WithIdentity(ctx, identity), nil
}
