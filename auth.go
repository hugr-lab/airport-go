package airport

import (
	"context"

	"github.com/hugr-lab/airport-go/auth"
)

// Authenticator validates bearer tokens and returns user identity.
// This is re-exported from the auth package for convenience.
type Authenticator = auth.Authenticator

// BearerAuth creates an Authenticator from a validation function.
// This is the simplest way to add authentication to your Flight server.
//
// Example:
//
//	auth := airport.BearerAuth(func(token string) (string, error) {
//	    user, err := validateWithMyBackend(token)
//	    if err != nil {
//	        return "", airport.ErrUnauthorized
//	    }
//	    return user.ID, nil
//	})
//
//	config := airport.ServerConfig{
//	    Catalog: catalog,
//	    Auth:    auth,
//	}
func BearerAuth(validateFunc func(token string) (identity string, err error)) Authenticator {
	return auth.BearerAuth(validateFunc)
}

// NoAuth returns an Authenticator that allows all requests without validation.
// Useful for development and testing. DO NOT use in production.
func NoAuth() Authenticator {
	return auth.NoAuth()
}

// IdentityFromContext retrieves the authenticated user identity from context.
// Returns empty string if no identity is set (unauthenticated request).
// This can be used in scan functions or custom handlers to check who is making the request.
//
// Example:
//
//	func myScanFunc(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
//	    identity := airport.IdentityFromContext(ctx)
//	    if identity == "" {
//	        return nil, errors.New("authentication required")
//	    }
//	    // Return data based on identity...
//	}
func IdentityFromContext(ctx context.Context) string {
	return auth.IdentityFromContext(ctx)
}
