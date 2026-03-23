# Research: Multi-Catalog Server Support

**Feature**: 001-multicatalog-server
**Date**: 2026-01-08

## Research Tasks

### 1. Thread-Safe Map Implementation for Catalog Registry

**Decision**: Use `sync.RWMutex` with a standard Go map

**Rationale**:
- `sync.Map` is optimized for append-only or read-heavy workloads with stable keys, which doesn't fit our use case (dynamic add/remove)
- `sync.RWMutex` provides clear semantics: multiple readers OR single writer
- Catalog lookups (reads) are far more frequent than add/remove operations (writes)
- Standard map + RWMutex is idiomatic Go and easy to reason about

**Alternatives Considered**:
- `sync.Map`: Rejected - not ideal for dynamic key management; harder to implement "catalog exists" checks
- Atomic pointer swap with copy-on-write: Rejected - more complex, marginal benefit for low-frequency writes
- Channel-based coordination: Rejected - adds latency, overly complex for simple map operations

### 2. gRPC Metadata Extraction Pattern

**Decision**: Use `metadata.FromIncomingContext(ctx)` with defensive nil checks

**Rationale**:
- Standard gRPC pattern, already used in auth package for token extraction
- Returns (md, ok) allowing graceful handling of missing metadata
- Header values are case-insensitive in HTTP/2 but gRPC lowercases keys

**Alternatives Considered**:
- Custom header extraction: Rejected - reinvents the wheel, breaks gRPC conventions

**Implementation Notes**:
```go
md, ok := metadata.FromIncomingContext(ctx)
if !ok {
    // No metadata - use default catalog (empty string)
    return ""
}
values := md.Get("airport-catalog")
if len(values) == 0 {
    return ""
}
return values[0] // First value per spec
```

### 3. Flight RPC Method Delegation Pattern

**Decision**: Direct delegation to underlying `flight.Server` methods

**Rationale**:
- `flight.Server` already implements `flight.FlightServer` interface
- MultiCatalogServer wraps multiple servers and delegates based on catalog header
- Each method extracts catalog name, looks up server, calls corresponding method
- Errors propagate naturally through gRPC error handling

**Alternatives Considered**:
- Middleware/interceptor-only approach: Rejected - Flight servers are registered directly with gRPC; need wrapper server
- Proxy pattern with request/response copying: Rejected - unnecessary overhead, direct delegation is cleaner

### 4. CatalogAuthorizer Interface Design

**Decision**: Separate optional interface for catalog authorization

**Rationale**:
- Separates authentication (who are you?) from authorization (can you access this catalog?)
- Authenticator implementations can optionally also implement CatalogAuthorizer
- No changes to existing Authenticator interface - fully backward compatible
- Returns context allowing catalog-specific claims to be added

**Interface Design**:
```go
// CatalogAuthorizer is an optional interface for per-catalog authorization.
// Authenticator implementations can also implement this interface.
type CatalogAuthorizer interface {
    // AuthorizeCatalog authorizes access to a specific catalog.
    // Called after Authenticate() to check catalog-level permissions.
    AuthorizeCatalog(ctx context.Context, catalog string, token string) (context.Context, error)
}
```

**Usage Flow**:
1. `Authenticate(ctx, token)` is called first
2. If authenticator implements `CatalogAuthorizer`, `AuthorizeCatalog(ctx, catalog, token)` is called
3. Returns enriched context or PermissionDenied error

**Alternatives Considered**:
- Modify existing Authenticator interface: Rejected - breaking change
- Single combined interface: Rejected - conflates authentication and authorization
- Pass catalog in context: Rejected - less explicit, harder to test

### 5. Context Propagation for Trace/Session IDs

**Decision**: Follow existing `auth.WithIdentity`/`auth.IdentityFromContext` pattern

**Rationale**:
- Consistent with existing codebase patterns
- Type-safe context keys prevent collisions
- Simple getter/setter functions for ergonomic use

**Implementation Notes**:
```go
type contextKey int

const (
    traceIDKey contextKey = iota
    sessionIDKey
)

func WithTraceID(ctx context.Context, traceID string) context.Context {
    return context.WithValue(ctx, traceIDKey, traceID)
}

func TraceIDFromContext(ctx context.Context) string {
    id, _ := ctx.Value(traceIDKey).(string)
    return id
}
```

**Alternatives Considered**:
- Store in gRPC metadata: Rejected - metadata is for transport, context is for request lifecycle
- Struct with all values: Rejected - less composable, harder to test individual values

### 6. Error Handling for Unknown Catalogs

**Decision**: Return `codes.NotFound` gRPC status with descriptive message

**Rationale**:
- `NotFound` is semantically correct - the requested catalog resource doesn't exist
- Consistent with gRPC error handling conventions
- Message includes catalog name for debugging

**Alternatives Considered**:
- `InvalidArgument`: Rejected - argument is valid, resource just doesn't exist
- `FailedPrecondition`: Rejected - not a precondition violation
- `Unimplemented`: Rejected - method IS implemented, just catalog missing

### 7. Handling In-Flight Requests During Catalog Removal

**Decision**: Don't track in-flight requests; let them complete naturally

**Rationale**:
- Once a request has resolved its catalog server reference, it holds that reference
- Removing a catalog from the map doesn't affect existing goroutines with server reference
- Simpler implementation, no reference counting or graceful shutdown logic needed
- Aligns with spec: "In-flight requests to the removed catalog complete normally"

**Alternatives Considered**:
- Reference counting with graceful drain: Rejected - adds complexity, no clear benefit
- Cancel in-flight requests: Rejected - violates spec requirement

### 8. CatalogTransactionManager Interface Design

**Decision**: New interface with `BeginTransaction(ctx, catalogName)` signature

**Rationale**:
- Transactions must be scoped to a single catalog for correct commit/rollback routing
- The transaction manager must store txID → catalogName mapping
- GetTransactionStatus returns both state and catalog name for routing decisions
- Follows the existing `TransactionManager` interface pattern but adds catalog awareness

**Interface Design**:
```go
type CatalogTransactionManager interface {
    BeginTransaction(ctx context.Context, catalogName string) (txID string, err error)
    CommitTransaction(ctx context.Context, txID string) error
    RollbackTransaction(ctx context.Context, txID string) error
    GetTransactionStatus(ctx context.Context, txID string) (state TransactionState, catalogName string, exists bool)
}
```

**Alternatives Considered**:
- Modify existing TransactionManager: Rejected - breaking change to existing interface
- Store catalog in context: Rejected - transaction manager needs to look up catalog at commit/rollback time
- Separate manager per catalog: Rejected - more complex, harder to manage globally unique txIDs

### 9. High-Level API Design (NewMultiCatalogServer)

**Decision**: Provide `NewMultiCatalogServer(grpcServer, config)` function in root package

**Rationale**:
- Mirrors existing `NewServer(grpcServer, config)` pattern for consistency
- Accepts `[]catalog.Catalog` instead of `[]flight.Server` - creates internal servers
- Handles all setup: validation, server creation, interceptor registration
- Follows Go convention of config struct with optional fields

**Implementation Notes**:
```go
func NewMultiCatalogServer(grpcServer *grpc.Server, config MultiCatalogServerConfig) error {
    // Validate: at least one catalog, unique names
    // Create flight.Server for each catalog (using shared allocator, logger, address)
    // Create dispatcher MultiCatalogServer
    // Set up auth interceptors if configured
    // Register with gRPC server
}
```

**Alternatives Considered**:
- Require users to create flight.Server instances: Rejected - error-prone, duplicates boilerplate
- Builder pattern: Rejected - config struct is sufficient and more idiomatic
- Return the MultiCatalogServer: Considered but returning error-only follows NewServer pattern

## Summary

All research items resolved. Key decisions:
- `sync.RWMutex` + map for thread-safe catalog registry
- Standard gRPC metadata extraction
- Direct method delegation to underlying servers
- New `CatalogAuthorizer` interface (optional, non-breaking extension to Authenticator)
- New `CatalogTransactionManager` interface with catalog-scoped transactions
- Context value pattern for trace/session IDs
- `codes.NotFound` for unknown catalogs
- Natural completion for in-flight requests during removal
- High-level `NewMultiCatalogServer(grpcServer, config)` API matching existing patterns
