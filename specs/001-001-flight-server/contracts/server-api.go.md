# Contract: Server API

**Package**: `github.com/your-org/airport-go`
**Purpose**: Main entry point for registering Flight server on gRPC
**Go Version**: 1.23+
**Arrow Version**: `github.com/apache/arrow-go/v18/arrow`

## NewServer Function

```go
package airport

import (
    "context"
    "log/slog"

    "github.com/apache/arrow-go/v18/arrow/memory"
    "google.golang.org/grpc"

    "github.com/your-org/airport-go/catalog"
)

// NewServer registers Airport Flight service handlers on the provided gRPC server.
// This is the main entry point for the airport package.
//
// The function:
// 1. Validates the ServerConfig
// 2. Creates Flight service implementation
// 3. Registers it on grpcServer
// 4. Optionally registers auth interceptors
//
// Returns error if config is invalid (e.g., nil Catalog).
// Does NOT start the gRPC server - user controls lifecycle via grpcServer.Serve().
//
// Example:
//   grpcServer := grpc.NewServer()
//   err := airport.NewServer(grpcServer, airport.ServerConfig{
//       Catalog: myCatalog,
//       Auth: airport.BearerAuth(validateToken),
//   })
//   lis, _ := net.Listen("tcp", ":50051")
//   grpcServer.Serve(lis)
func NewServer(grpcServer *grpc.Server, config ServerConfig) error
```

## ServerConfig Struct

```go
// ServerConfig contains configuration for Airport Flight server.
type ServerConfig struct {
    // Catalog provides schemas, tables, and functions.
    // REQUIRED: MUST NOT be nil.
    Catalog catalog.Catalog

    // Auth provides authentication logic.
    // OPTIONAL: If nil, no authentication (all requests allowed).
    Auth Authenticator

    // Allocator for Arrow memory management.
    // OPTIONAL: Uses memory.DefaultAllocator if nil.
    Allocator memory.Allocator

    // Logger for internal logging.
    // OPTIONAL: Uses slog.Default() if nil.
    Logger *slog.Logger

    // MaxMessageSize sets maximum gRPC message size in bytes.
    // OPTIONAL: If 0, uses gRPC default (4MB).
    // Recommended: 16MB for large Arrow batches.
    MaxMessageSize int
}
```

## Authenticator Interface

```go
// Authenticator validates bearer tokens and returns user identity.
// Implementations MUST be goroutine-safe.
type Authenticator interface {
    // Authenticate validates a bearer token and returns user identity.
    // Returns error if token is invalid or expired.
    // Identity string is used for authorization and logging.
    // Context allows timeout for auth backend calls.
    Authenticate(ctx context.Context, token string) (identity string, err error)
}
```

## Built-in Authenticators

```go
// BearerAuth creates an Authenticator from a validation function.
// This is the simplest way to add authentication.
//
// Example:
//   auth := airport.BearerAuth(func(token string) (string, error) {
//       user, err := validateWithMyBackend(token)
//       if err != nil {
//           return "", airport.ErrUnauthorized
//       }
//       return user.ID, nil
//   })
func BearerAuth(validateFunc func(token string) (identity string, err error)) Authenticator

// NoAuth returns an Authenticator that allows all requests.
// Useful for development/testing. DO NOT use in production.
func NoAuth() Authenticator
```

## Standard Errors

```go
// Standard errors returned by airport package.
var (
    // ErrUnauthorized indicates authentication failed.
    // Return this from Authenticator.Authenticate() for invalid tokens.
    ErrUnauthorized = errors.New("unauthorized")

    // ErrInvalidConfig indicates ServerConfig validation failed.
    ErrInvalidConfig = errors.New("invalid server config")

    // ErrCatalogNotFound indicates catalog/schema/table lookup failed.
    ErrCatalogNotFound = errors.New("catalog entity not found")

    // ErrInvalidParameters indicates function parameters are invalid.
    ErrInvalidParameters = errors.New("invalid function parameters")
)
```

## Example Usage

### Basic Server (No Auth)

```go
package main

import (
    "context"
    "log"
    "net"

    "google.golang.org/grpc"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"

    "github.com/your-org/airport-go"
    "github.com/your-org/airport-go/catalog"
)

func main() {
    // Build catalog with single table
    cat := airport.NewCatalogBuilder().
        Schema("main").
            Comment("Main schema").
            SimpleTable(catalog.SimpleTableDef{
                Name:    "users",
                Comment: "User accounts",
                Schema: arrow.NewSchema([]arrow.Field{
                    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
                    {Name: "name", Type: arrow.BinaryTypes.String},
                }, nil),
                ScanFunc: scanUsers,
            }).
        Build()

    // Create gRPC server
    grpcServer := grpc.NewServer(
        grpc.MaxRecvMsgSize(16 * 1024 * 1024),
        grpc.MaxSendMsgSize(16 * 1024 * 1024),
    )

    // Register Airport handlers
    err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: cat,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Start serving
    lis, _ := net.Listen("tcp", ":50051")
    log.Println("Airport server listening on :50051")
    grpcServer.Serve(lis)
}

func scanUsers(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Implement data fetching logic
    // Return arrow.RecordReader streaming user data
    return nil, nil
}
```

### Server with Authentication

```go
func main() {
    cat := buildCatalog()

    // Create auth validator
    auth := airport.BearerAuth(func(token string) (string, error) {
        // Validate token with your backend
        user, err := authBackend.ValidateToken(token)
        if err != nil {
            return "", airport.ErrUnauthorized
        }
        return user.ID, nil
    })

    grpcServer := grpc.NewServer()

    err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: cat,
        Auth:    auth,
    })
    if err != nil {
        log.Fatal(err)
    }

    lis, _ := net.Listen("tcp", ":50051")
    grpcServer.Serve(lis)
}
```

### Server with Custom Authenticator

```go
type myAuthenticator struct {
    jwtSecret []byte
}

func (a *myAuthenticator) Authenticate(ctx context.Context, token string) (string, error) {
    // Custom JWT validation
    claims, err := jwt.Parse(token, a.jwtSecret)
    if err != nil {
        return "", airport.ErrUnauthorized
    }

    return claims.UserID, nil
}

func main() {
    auth := &myAuthenticator{jwtSecret: []byte("secret")}

    grpcServer := grpc.NewServer()
    airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: buildCatalog(),
        Auth:    auth,
    })

    lis, _ := net.Listen("tcp", ":50051")
    grpcServer.Serve(lis)
}
```

### Server with TLS

```go
func main() {
    // Load TLS credentials
    creds, err := credentials.NewServerTLSFromFile("server.crt", "server.key")
    if err != nil {
        log.Fatal(err)
    }

    // Create gRPC server with TLS
    grpcServer := grpc.NewServer(
        grpc.Creds(creds),
        grpc.MaxRecvMsgSize(16 * 1024 * 1024),
    )

    // Airport registration (TLS handled by gRPC)
    airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: buildCatalog(),
        Auth:    airport.BearerAuth(validateToken),
    })

    lis, _ := net.Listen("tcp", ":50051")
    grpcServer.Serve(lis)
}
```

## Validation Rules

`NewServer` MUST validate:
1. `ServerConfig.Catalog` is not nil (return `ErrInvalidConfig`)
2. If `Auth` is provided, it implements `Authenticator` interface
3. gRPC server is not nil (panic if nil - this is programmer error)

`NewServer` MUST NOT:
- Start the gRPC server (user calls `grpcServer.Serve()`)
- Create TLS configuration (user configures via `grpc.Creds()`)
- Manage server lifecycle (user calls `grpcServer.Stop()` or `GracefulStop()`)

## Thread Safety

- `NewServer` is NOT thread-safe (call once during initialization)
- Registered Flight handlers ARE thread-safe (concurrent requests supported)
- `Authenticator.Authenticate()` MUST be thread-safe (called concurrently)
- `Catalog` interfaces MUST be thread-safe (concurrent catalog queries)

## Testing Recommendations

```go
func TestServerCreation(t *testing.T) {
    grpcServer := grpc.NewServer()
    defer grpcServer.Stop()

    err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: mockCatalog,
    })
    assert.NoError(t, err)
}

func TestServerRequiresCatalog(t *testing.T) {
    grpcServer := grpc.NewServer()
    defer grpcServer.Stop()

    err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: nil, // Invalid
    })
    assert.ErrorIs(t, err, airport.ErrInvalidConfig)
}

func TestServerWithAuth(t *testing.T) {
    grpcServer := grpc.NewServer()
    defer grpcServer.Stop()

    auth := airport.BearerAuth(func(token string) (string, error) {
        if token == "valid" {
            return "user123", nil
        }
        return "", airport.ErrUnauthorized
    })

    err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: mockCatalog,
        Auth:    auth,
    })
    assert.NoError(t, err)

    // Test authenticated request (requires integration test with client)
}
```
