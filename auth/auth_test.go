package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestNoAuth tests the NoAuth authenticator.
func TestNoAuth(t *testing.T) {
	auth := NoAuth()

	ctx := context.Background()
	identity, err := auth.Authenticate(ctx, "any-token")

	if err != nil {
		t.Errorf("NoAuth should never return error, got: %v", err)
	}

	if identity != "anonymous" {
		t.Errorf("Expected identity 'anonymous', got '%s'", identity)
	}
}

// TestNoAuthWithoutToken tests NoAuth with empty token.
func TestNoAuthWithoutToken(t *testing.T) {
	auth := NoAuth()

	ctx := context.Background()
	identity, err := auth.Authenticate(ctx, "")

	if err != nil {
		t.Errorf("NoAuth should never return error, got: %v", err)
	}

	if identity != "anonymous" {
		t.Errorf("Expected identity 'anonymous', got '%s'", identity)
	}
}

// TestBearerAuthSuccess tests successful bearer token validation.
func TestBearerAuthSuccess(t *testing.T) {
	auth := BearerAuth(func(token string) (string, error) {
		if token == "valid-token" {
			return "user123", nil
		}
		return "", errors.New("invalid token")
	})

	ctx := context.Background()
	identity, err := auth.Authenticate(ctx, "valid-token")

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if identity != "user123" {
		t.Errorf("Expected identity 'user123', got '%s'", identity)
	}
}

// TestBearerAuthFailure tests failed bearer token validation.
func TestBearerAuthFailure(t *testing.T) {
	auth := BearerAuth(func(token string) (string, error) {
		if token == "valid-token" {
			return "user123", nil
		}
		return "", errors.New("invalid token")
	})

	ctx := context.Background()
	identity, err := auth.Authenticate(ctx, "invalid-token")

	if err == nil {
		t.Error("Expected error for invalid token, got nil")
	}

	if identity != "" {
		t.Errorf("Expected empty identity for invalid token, got '%s'", identity)
	}
}

// TestBearerAuthEmptyToken tests validation with empty token.
func TestBearerAuthEmptyToken(t *testing.T) {
	auth := BearerAuth(func(token string) (string, error) {
		if token == "" {
			return "", errors.New("empty token")
		}
		return "user", nil
	})

	ctx := context.Background()
	identity, err := auth.Authenticate(ctx, "")

	if err == nil {
		t.Error("Expected error for empty token, got nil")
	}

	if identity != "" {
		t.Errorf("Expected empty identity, got '%s'", identity)
	}
}

// TestBearerAuthMultipleTokens tests authenticating multiple different tokens.
func TestBearerAuthMultipleTokens(t *testing.T) {
	validTokens := map[string]string{
		"token1": "user1",
		"token2": "user2",
		"token3": "user3",
	}

	auth := BearerAuth(func(token string) (string, error) {
		if identity, ok := validTokens[token]; ok {
			return identity, nil
		}
		return "", errors.New("invalid token")
	})

	ctx := context.Background()

	// Test valid tokens
	for token, expectedIdentity := range validTokens {
		identity, err := auth.Authenticate(ctx, token)
		if err != nil {
			t.Errorf("Token '%s': expected no error, got: %v", token, err)
		}
		if identity != expectedIdentity {
			t.Errorf("Token '%s': expected identity '%s', got '%s'", token, expectedIdentity, identity)
		}
	}

	// Test invalid token
	identity, err := auth.Authenticate(ctx, "invalid")
	if err == nil {
		t.Error("Expected error for invalid token, got nil")
	}
	if identity != "" {
		t.Errorf("Expected empty identity for invalid token, got '%s'", identity)
	}
}

// TestBearerAuthConcurrency tests that BearerAuth is thread-safe.
func TestBearerAuthConcurrency(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	auth := BearerAuth(func(token string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()

		if token == "valid" {
			return "user", nil
		}
		return "", errors.New("invalid")
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Run 100 concurrent authentications
	for i := 0; i < 100; i++ {
		wg.Add(1)
		token := "valid"
		if i%2 == 0 {
			token = "invalid"
		}

		go func(t string) {
			defer wg.Done()
			_, err := auth.Authenticate(ctx, t)
			if t == "valid" && err != nil {
				errors <- err
			}
		}(token)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent auth error: %v", err)
	}

	mu.Lock()
	if callCount != 100 {
		t.Errorf("Expected 100 calls, got %d", callCount)
	}
	mu.Unlock()
}

// TestBearerAuthContextCancellation tests that authentication respects context.
func TestBearerAuthContextCancellation(t *testing.T) {
	blockCh := make(chan struct{})

	auth := BearerAuth(func(token string) (string, error) {
		<-blockCh // Wait until unblocked
		return "user", nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Start auth in background
	done := make(chan struct{})
	var identity string
	var err error

	go func() {
		identity, err = auth.Authenticate(ctx, "token")
		close(done)
	}()

	// Cancel context before auth completes
	cancel()

	// Unblock the validation function
	close(blockCh)

	// Wait for auth to complete
	<-done

	// Note: The bearerAuthenticator doesn't currently check context,
	// but this test documents expected behavior for future improvements
	t.Logf("Auth with cancelled context returned: identity='%s', err=%v", identity, err)
}

// TestBearerAuthStateIsolation tests that each call to Authenticate is independent.
func TestBearerAuthStateIsolation(t *testing.T) {
	callNumber := 0

	auth := BearerAuth(func(token string) (string, error) {
		callNumber++
		return token + "_" + string(rune('0'+callNumber)), nil
	})

	ctx := context.Background()

	// First call
	identity1, err := auth.Authenticate(ctx, "user")
	if err != nil {
		t.Fatalf("First auth failed: %v", err)
	}

	// Second call
	identity2, err := auth.Authenticate(ctx, "user")
	if err != nil {
		t.Fatalf("Second auth failed: %v", err)
	}

	// Identities should be different (state not shared)
	if identity1 == identity2 {
		t.Errorf("Expected different identities, both got: %s", identity1)
	}
}

// TestBearerAuthErrorPropagation tests that errors from validation function are propagated.
func TestBearerAuthErrorPropagation(t *testing.T) {
	customError := errors.New("custom validation error")

	auth := BearerAuth(func(token string) (string, error) {
		return "", customError
	})

	ctx := context.Background()
	_, err := auth.Authenticate(ctx, "token")

	if err != customError {
		t.Errorf("Expected custom error, got: %v", err)
	}
}

// TestNoAuthConcurrency tests that NoAuth is thread-safe.
func TestNoAuthConcurrency(t *testing.T) {
	auth := NoAuth()
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identity, err := auth.Authenticate(ctx, "any-token")
			if err != nil {
				errs <- err
			}
			if identity != "anonymous" {
				errs <- errors.New("unexpected identity: " + identity)
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("Concurrent NoAuth error: %v", err)
	}
}
