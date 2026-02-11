# Quickstart: Multi-Catalog Server

## Overview

The `NewMultiCatalogServer` function allows you to expose multiple catalogs through a single gRPC endpoint. Clients specify which catalog to access using the `airport-catalog` metadata header.

## Basic Usage

### Creating a Multi-Catalog Server (Recommended)

Use the high-level `NewMultiCatalogServer` function - similar to `NewServer` but for multiple catalogs:

```go
package main

import (
    "log/slog"
    "net"

    "github.com/hugr-lab/airport-go"
    "github.com/hugr-lab/airport-go/catalog"
    "google.golang.org/grpc"
)

func main() {
    // Create catalogs (each must implement catalog.NamedCatalog)
    salesCatalog := catalog.NewCatalogBuilder("sales").
        Schema("main").
        Table("orders", ordersSchema, ordersReader).
        Build()

    analyticsCatalog := catalog.NewCatalogBuilder("analytics").
        Schema("main").
        Table("metrics", metricsSchema, metricsReader).
        Build()

    // Create gRPC server
    grpcServer := grpc.NewServer()

    // Create and register multi-catalog server
    err := airport.NewMultiCatalogServer(grpcServer, airport.MultiCatalogServerConfig{
        Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
        Logger:   slog.Default(),
    })
    if err != nil {
        panic(err) // Duplicate catalog names or validation error
    }

    // Serve
    lis, _ := net.Listen("tcp", ":50051")
    grpcServer.Serve(lis)
}
```

### With Transaction Manager

For multi-catalog transactions, use `CatalogTransactionManager`:

```go
// Implement CatalogTransactionManager
type MyTxManager struct {
    // tracks txID -> catalogName mapping
    txCatalogs map[string]string
    mu         sync.RWMutex
}

func (m *MyTxManager) BeginTransaction(ctx context.Context, catalogName string) (string, error) {
    txID := uuid.New().String()
    m.mu.Lock()
    m.txCatalogs[txID] = catalogName
    m.mu.Unlock()
    // Start transaction in the specific catalog's backend
    return txID, nil
}

func (m *MyTxManager) CommitTransaction(ctx context.Context, txID string) error {
    m.mu.RLock()
    catalogName := m.txCatalogs[txID]
    m.mu.RUnlock()
    // Commit in the correct catalog's backend
    return nil
}

// ... implement other methods

// Use in config
err := airport.NewMultiCatalogServer(grpcServer, airport.MultiCatalogServerConfig{
    Catalogs:           []catalog.Catalog{salesCatalog, analyticsCatalog},
    TransactionManager: &MyTxManager{txCatalogs: make(map[string]string)},
    Logger:             slog.Default(),
})
```

### With Catalog-Aware Authorization

For per-catalog authorization, implement both `Authenticator` and `CatalogAuthorizer`:

```go
// MyCatalogAuth implements both Authenticator and CatalogAuthorizer
type MyCatalogAuth struct{}

// Authenticate validates the token (standard auth)
func (a *MyCatalogAuth) Authenticate(ctx context.Context, token string) (string, error) {
    claims, err := validateToken(token)
    if err != nil {
        return "", err
    }
    return claims.UserID, nil
}

// AuthorizeCatalog checks catalog-specific permissions (called after Authenticate)
func (a *MyCatalogAuth) AuthorizeCatalog(
    ctx context.Context,
    catalog string,
    token string,
) (context.Context, error) {
    claims, _ := validateToken(token) // Already validated in Authenticate

    if !claims.HasAccess(catalog) {
        return ctx, status.Errorf(codes.PermissionDenied,
            "no access to catalog: %s", catalog)
    }

    // Optionally enrich context with catalog-specific claims
    return ctx, nil
}

// Use in config
err := airport.NewMultiCatalogServer(grpcServer, airport.MultiCatalogServerConfig{
    Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
    Auth:     &MyCatalogAuth{}, // Implements both Authenticator and CatalogAuthorizer
    Logger:   slog.Default(),
})
```

### Dynamic Catalog Management

Add or remove catalogs at runtime:

```go
// Get the MultiCatalogServer instance (returned from internal creation)
// Through the flight package's low-level API if needed:

multiServer := flight.NewMultiCatalogServerInternal(...)

// Add a new catalog at runtime
newCatalog := catalog.NewCatalogBuilder("warehouse").Build()
if err := multiServer.AddCatalog(newCatalog); err != nil {
    // Handle error (nil catalog or duplicate name)
}

// Remove a catalog at runtime
if err := multiServer.RemoveCatalog("warehouse"); err != nil {
    // Handle error (catalog not found)
}

// List all registered catalogs
catalogs := multiServer.Catalogs()
for _, cat := range catalogs {
    if named, ok := cat.(catalog.NamedCatalog); ok {
        fmt.Println(named.Name())
    }
}
```

## Client Usage

Clients specify the target catalog using the `airport-catalog` gRPC metadata header:

```go
// Go client example
ctx := metadata.AppendToOutgoingContext(ctx,
    "airport-catalog", "sales",
    "airport-trace-id", "abc123",
    "airport-client-session-id", "session456",
)

// Request goes to "sales" catalog
info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{...})
```

```python
# Python client example
metadata = [
    ("airport-catalog", "analytics"),
    ("airport-trace-id", "abc123"),
]
info = client.get_flight_info(descriptor, metadata=metadata)
```

If no `airport-catalog` header is provided, the request routes to the default catalog (empty name).

## Accessing Request Metadata

Within catalog implementations, access trace/session IDs from context:

```go
func (t *MyTable) GetRecordBatches(ctx context.Context) ([]arrow.Record, error) {
    traceID := auth.TraceIDFromContext(ctx)
    sessionID := auth.SessionIDFromContext(ctx)
    catalogName := auth.CatalogNameFromContext(ctx)

    // Use for logging, metrics, etc.
    slog.Info("processing request",
        "trace_id", traceID,
        "session_id", sessionID,
        "catalog", catalogName,
    )

    // ... rest of implementation
}
```

## Error Handling

| Scenario | gRPC Status | Message |
|----------|-------------|---------|
| Catalog not found | `NotFound` | "catalog not found: {name}" |
| No default catalog | `NotFound` | "default catalog not found" |
| Duplicate catalog on add | Error returned | "catalog already exists: {name}" |
| Nil catalog on add | Error returned | "catalog cannot be nil" |
| No catalogs in config | Error returned | "at least one catalog is required" |

## Best Practices

1. **Use the high-level API**: Prefer `NewMultiCatalogServer` over low-level flight package
2. **Name catalogs descriptively**: Use meaningful names like "sales", "analytics", "warehouse"
3. **Use default catalog sparingly**: Reserve empty name for backward compatibility
4. **Log trace IDs**: Always include trace ID in logs for debugging
5. **Implement CatalogAuthorizer**: For multi-tenant scenarios, add authorization after authentication
6. **Implement CatalogTransactionManager**: Track which catalog owns each transaction
7. **Handle removals gracefully**: In-flight requests complete normally after removal
