package auth

import (
	"context"
)

// bearerAuthenticator wraps a user-provided validation function.
type bearerAuthenticator struct {
	validateFunc func(token string) (identity string, err error)
}

// BearerAuth creates an Authenticator from a validation function.
// This is the simplest way to add authentication.
//
// Example:
//
//	auth := BearerAuth(func(token string) (string, error) {
//	    user, err := validateWithMyBackend(token)
//	    if err != nil {
//	        return "", airport.ErrUnauthorized
//	    }
//	    return user.ID, nil
//	})
func BearerAuth(validateFunc func(token string) (identity string, err error)) Authenticator {
	return &bearerAuthenticator{
		validateFunc: validateFunc,
	}
}

// Authenticate implements Authenticator for bearerAuthenticator.
// Calls the user-provided validation function with the token.
func (b *bearerAuthenticator) Authenticate(ctx context.Context, token string) (string, error) {
	// Call user's validation function
	// Note: This doesn't use the context directly, but the user's function might
	// perform I/O operations that should respect context deadlines.
	return b.validateFunc(token)
}
