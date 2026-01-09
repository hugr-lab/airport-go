# Data Model: Multi-Catalog Server Support

**Feature**: 001-multicatalog-server
**Date**: 2026-01-08

## Entities

### MultiCatalogServer

The main server implementation that aggregates multiple `flight.Server` instances.

| Field | Type | Description |
|-------|------|-------------|
| catalogs | map[string]*flight.Server | Thread-safe map of catalog name to server instance |
| mu | sync.RWMutex | Protects concurrent access to catalogs map |
| logger | *slog.Logger | Shared logger for error/info logging |

**Invariants**:
- Catalog names are unique (including empty string for default)
- Empty string ("") is a valid catalog name (represents default catalog)
- Map access is always protected by mutex

**Lifecycle**:
- Created via `NewMultiCatalogServer()` with initial servers
- Servers added dynamically via `AddCatalog()`
- Servers removed dynamically via `RemoveCatalog()`
- No explicit shutdown; individual catalog servers manage their own lifecycle

### MultiCatalogServerConfig

Configuration struct for the high-level `NewMultiCatalogServer` function.

| Field | Type | Description |
|-------|------|-------------|
| Catalogs | []catalog.Catalog | List of catalogs to serve (required, must have at least one) |
| Allocator | memory.Allocator | Arrow memory allocator (optional, defaults to DefaultAllocator) |
| Logger | *slog.Logger | Logger for server events (optional) |
| LogLevel | *slog.Level | Log level if Logger not provided (optional, defaults to Info) |
| Address | string | Server's public address for FlightEndpoint (optional) |
| TransactionManager | CatalogTransactionManager | Transaction coordinator (optional) |
| Auth | auth.Authenticator | Authenticator for requests (optional, may also implement CatalogAuthorizer) |
| MaxMessageSize | int | Maximum gRPC message size (optional) |

**Invariants**:
- At least one catalog must be provided
- All catalogs must have unique names (including empty string for default)
- Each catalog should implement `catalog.NamedCatalog` for routing

### CatalogAuthorizer (Interface)

Optional interface for per-catalog authorization. Authenticator implementations can also implement this.

| Method | Signature | Description |
|--------|-----------|-------------|
| AuthorizeCatalog | `(ctx, catalog, token string) (context.Context, error)` | Authorizes access to specific catalog |

**Usage Flow**:
1. `Authenticate(ctx, token)` is called first (standard auth)
2. If authenticator implements `CatalogAuthorizer`, `AuthorizeCatalog(ctx, catalog, token)` is called
3. Returns enriched context or error

**Relationship**:
- Optional extension to `Authenticator` interface
- Separates authentication (who?) from authorization (can access catalog?)
- Returns context allowing catalog-specific claims to be added

### CatalogTransactionManager (Interface)

Extended transaction manager that tracks catalog ownership for transactions.

| Method | Signature | Description |
|--------|-----------|-------------|
| BeginTransaction | `(ctx, catalogName string) (txID string, err error)` | Creates transaction in specific catalog |
| CommitTransaction | `(ctx, txID string) error` | Commits transaction (routes to correct catalog) |
| RollbackTransaction | `(ctx, txID string) error` | Rolls back transaction (routes to correct catalog) |
| GetTransactionStatus | `(ctx, txID string) (state, catalogName string, exists bool)` | Returns state and owning catalog |

**Relationship**:
- Extends the existing `TransactionManager` interface concept
- Implementation must store txID → catalogName mapping
- Commit/rollback operations use stored mapping to route to correct catalog backend

**Invariants**:
- Transaction IDs are globally unique
- Each transaction belongs to exactly one catalog
- Catalog name is immutable once transaction is created

### Request Context Extensions

New context values for observability.

| Key | Type | Header Source | Description |
|-----|------|---------------|-------------|
| traceIDKey | contextKey | `airport-trace-id` | Distributed trace identifier |
| sessionIDKey | contextKey | `airport-client-session-id` | Client session identifier |
| catalogNameKey | contextKey | `airport-catalog` | Target catalog name (for auth) |

**Behavior**:
- Empty string if header not present
- First value used if multiple values present
- Values are immutable once set in context

## Relationships

```text
┌─────────────────────────────────────────────────────────────┐
│                    MultiCatalogServer                        │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  catalogs: map[string]*flight.Server                │    │
│  │    ""        → *flight.Server (default)             │    │
│  │    "sales"   → *flight.Server                       │    │
│  │    "analytics" → *flight.Server                     │    │
│  └─────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           │ delegates to                     │
│                           ▼                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              flight.Server (existing)                 │   │
│  │  - DoAction(), DoExchange(), DoGet(), ...            │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  Auth Flow                                   │
│                                                              │
│  Request → Extract Headers → CatalogAwareAuthenticator      │
│              │                      │                        │
│              │ airport-catalog      │ AuthenticateWithCatalog│
│              │ airport-trace-id     │    (token, catalog)    │
│              │ airport-session-id   │         │              │
│              ▼                      ▼         ▼              │
│         Context enriched      identity + catalog-specific   │
│                                    authorization             │
└─────────────────────────────────────────────────────────────┘
```

## State Transitions

### Catalog Lifecycle

```text
                 AddCatalog(server)
    [Not Registered] ─────────────────► [Registered]
                                              │
                                              │ RemoveCatalog(name)
                                              ▼
                                        [Not Registered]
```

**State: Not Registered**
- Catalog name not in map
- Requests for this catalog return `codes.NotFound`

**State: Registered**
- Catalog name maps to a `*flight.Server`
- Requests routed to corresponding server

### Request Lifecycle

```text
    [Incoming gRPC Request]
            │
            ▼
    [Extract Metadata]
    - airport-catalog → catalog name (or "")
    - airport-trace-id → trace ID
    - airport-client-session-id → session ID
            │
            ▼
    [Authenticate (if configured)]
    - CatalogAwareAuthenticator.AuthenticateWithCatalog()
            │
            ▼
    [Lookup Catalog Server]
    - mu.RLock(), catalogs[name], mu.RUnlock()
            │
            ├── [Not Found] → Return codes.NotFound error
            │
            └── [Found] → Delegate to server method
                              │
                              ▼
                        [Response]
```

## Validation Rules

| Rule | Entity | Enforcement |
|------|--------|-------------|
| Unique catalog names | MultiCatalogServer | Error on AddCatalog if name exists |
| Non-nil server | MultiCatalogServer | Error on AddCatalog if server is nil |
| Catalog must exist | RemoveCatalog | Error if name not in map |
| Valid token | CatalogAwareAuthenticator | Return error on invalid token |

## Data Volume Assumptions

- Expected catalogs: 1-20 per MultiCatalogServer instance
- Concurrent requests: 100-10,000 per second
- Catalog changes: Rare (< 1 per minute typically)
- Read/write ratio for catalog map: ~10,000:1
