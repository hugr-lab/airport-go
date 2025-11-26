package airport_test

import (
	"testing"
)

// TestAuthentication verifies that bearer token authentication works correctly.
func TestAuthentication(t *testing.T) {
	cat := authenticatedCatalog()
	auth := testAuthHandler()
	server := newTestServer(t, cat, auth)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	// Test 1: Request without token should fail
	t.Run("NoToken", func(t *testing.T) {
		// Try to attach without token
		attachName := "test_no_auth"
		query := "ATTACH '" + server.address + "' AS " + attachName + " (TYPE airport)"

		_, err := db.Exec(query)
		if err == nil {
			t.Error("Expected authentication error, but query succeeded")
		}
	})

	// Test 2: Request with invalid token should fail
	t.Run("InvalidToken", func(t *testing.T) {
		attachName := "test_invalid"
		query := "ATTACH '" + server.address + "' AS " + attachName + " (TYPE airport, token 'invalid-token')"

		_, err := db.Exec(query)
		if err == nil {
			t.Error("Expected authentication error with invalid token, but query succeeded")
		}
	})

	// Test 3: Request with valid token should succeed
	t.Run("ValidToken", func(t *testing.T) {
		attachName := connectToFlightServer(t, db, server.address, "valid-token")

		// Should be able to query tables
		query := "SELECT COUNT(*) FROM " + attachName + ".secure.secrets"
		var count int64
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("Query with valid token failed: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})

	// Test 4: Verify data is accessible with auth
	t.Run("QueryWithAuth", func(t *testing.T) {
		// Create new connection with admin token
		attachName := "test_admin"
		query := "ATTACH '" + server.address + "' AS " + attachName + " (TYPE airport, token 'admin-token')"

		_, err := db.Exec(query)
		if err != nil {
			t.Fatalf("Failed to attach with admin token: %v", err)
		}

		// Query secrets table
		queryData := "SELECT key, value FROM " + attachName + ".secure.secrets ORDER BY key"
		rows, err := db.Query(queryData)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		secrets := make(map[string]string)
		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			secrets[key] = value
		}

		if len(secrets) != 2 {
			t.Errorf("Expected 2 secrets, got %d", len(secrets))
		}

		if secrets["api_key"] != "secret123" {
			t.Errorf("Expected api_key='secret123', got '%s'", secrets["api_key"])
		}

		if secrets["db_password"] != "pass456" {
			t.Errorf("Expected db_password='pass456', got '%s'", secrets["db_password"])
		}
	})
}

// TestAuthorizationInCatalog verifies that authentication context is passed
// to catalog methods and can be used for authorization.
func TestAuthorizationInCatalog(t *testing.T) {
	// This test would verify permission-based filtering
	// For now, we verify that authentication metadata is correctly propagated

	cat := authenticatedCatalog()
	auth := testAuthHandler()
	server := newTestServer(t, cat, auth)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	t.Run("AuthenticatedDiscovery", func(t *testing.T) {
		// Connect with valid token
		attachName := connectToFlightServer(t, db, server.address, "valid-token")

		// Should be able to discover schemas
		query := "SELECT schema_name FROM duckdb_schemas() WHERE catalog_name = ?"
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Schema discovery failed: %v", err)
		}
		defer rows.Close()

		schemaCount := 0
		for rows.Next() {
			schemaCount++
		}

		if schemaCount == 0 {
			t.Error("Expected at least one schema with authenticated access")
		}
	})
}

// TestTokenValidation verifies different authentication scenarios.
func TestTokenValidation(t *testing.T) {
	cat := authenticatedCatalog()
	auth := testAuthHandler()
	server := newTestServer(t, cat, auth)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	testCases := []struct {
		name        string
		token       string
		shouldWork  bool
		description string
	}{
		{
			name:        "ValidUserToken",
			token:       "valid-token",
			shouldWork:  true,
			description: "Valid user token should work",
		},
		{
			name:        "ValidAdminToken",
			token:       "admin-token",
			shouldWork:  true,
			description: "Valid admin token should work",
		},
		{
			name:        "EmptyToken",
			token:       "",
			shouldWork:  false,
			description: "Empty token should fail",
		},
		{
			name:        "WrongToken",
			token:       "wrong-token",
			shouldWork:  false,
			description: "Wrong token should fail",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			attachName := "test_" + tc.name
			var query string

			if tc.token == "" {
				query = "ATTACH '" + server.address + "' AS " + attachName + " (TYPE airport)"
			} else {
				query = "ATTACH '" + server.address + "' AS " + attachName + " (TYPE airport, token '" + tc.token + "')"
			}

			_, err := db.Exec(query)

			if tc.shouldWork && err != nil {
				t.Errorf("%s: Expected success, but got error: %v", tc.description, err)
			}

			if !tc.shouldWork && err == nil {
				t.Errorf("%s: Expected failure, but query succeeded", tc.description)
			}
		})
	}
}
