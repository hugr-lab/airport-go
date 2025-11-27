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

The server will start on `localhost:50051` with authentication enabled.

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

-- Connect with bearer token
CREATE SECRET airport_auth_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:50051',
    bearer_token 'secret-api-key'
);

-- Query protected data
SELECT * FROM airport_catalog.main.users;
```

### Without Authentication (Should Fail)

Try connecting without a bearer token:

```sql
CREATE SECRET airport_no_auth (
    TYPE AIRPORT,
    uri 'grpc://localhost:50051'
);

-- This will fail with authentication error
SELECT * FROM airport_catalog.main.users;
```

Expected error:
```
Error: Authentication failed: missing or invalid bearer token
```

## Authentication Flow

1. **Server**: Configured with `BearerAuth` validator function
2. **Client**: Includes `bearer_token` in DuckDB connection secret
3. **Request**: DuckDB sends token in Flight RPC metadata headers
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
