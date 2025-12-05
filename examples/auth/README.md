# Authentication Example

This example demonstrates bearer token authentication with the Airport Go package.

## Prerequisites

- Go 1.25+
- DuckDB 1.4+ (for client testing)
- Airport extension for DuckDB

## Installation

### Install DuckDB and Airport Extension

See the [basic example README](../basic/README.md) for installation instructions.

## Running the Server

Start the authenticated Airport Flight server:

```bash
go run main.go
```

The server will start on `localhost:50052` with authentication enabled.

## Testing with DuckDB Client

### With Authentication

Run the DuckDB client with the bearer token:

```bash
duckdb < client.sql
```

Or manually:

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Create a persistent secret with auth_token
-- Valid tokens: secret-admin-token, secret-user1-token, secret-user2-token, secret-guest-token
CREATE PERSISTENT SECRET airport_auth_secret (
    TYPE airport,
    auth_token 'secret-admin-token',
    scope 'grpc://localhost:50052'
);

-- Attach the server (secret applies automatically via scope)
ATTACH 'airport_catalog' AS airport_catalog (
    TYPE AIRPORT,
    location 'grpc://localhost:50052'
);

-- Query protected data
SELECT * FROM airport_catalog.app.users;
```

### Without Authentication (Should Fail)

Try connecting without a secret (no matching scope):

```sql
-- Attach without authentication secret (different port, no secret)
ATTACH 'no_auth_catalog' AS no_auth_catalog (
    TYPE AIRPORT,
    location 'grpc://localhost:50053'
);

-- This will fail with authentication error (assuming server is running)
SELECT * FROM no_auth_catalog.app.users;
```

Expected error:
```
Error: Authentication failed: missing or invalid bearer token
```

## Authentication Flow

1. **Server**: Configured with `BearerAuth` validator function
2. **Client**: Creates persistent secret with `auth_token` scoped to server URL
3. **Request**: DuckDB automatically sends token for matching scopes
4. **Validation**: Server validates token and extracts user identity
5. **Authorization**: Server can use identity for access control

## Custom Authentication

Modify the auth function in `main.go` to implement custom validation:

```go
auth := airport.BearerAuth(func(token string) (string, error) {
    // Validate against your auth system
    user, err := validateTokenWithYourAuthService(token)
    if err != nil {
        return "", airport.ErrUnauthorized
    }
    return user.ID, nil
})
```

## Next Steps

- Try the [dynamic example](../dynamic/) for live data sources
- Implement custom authorization logic in your catalog
- See [airport/auth](../../auth/) package for advanced auth patterns
