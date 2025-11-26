# Data Model: Airport Go Flight Server Package

**Date**: 2025-11-25
**Context**: Core entities and interfaces for the airport package

## Overview

The airport package follows an interface-based design to support both static (builder-generated) and dynamic (user-implemented) catalogs. The model is hierarchical: `Catalog` → `Schema` → `Table`/`ScalarFunction`. User-provided scan functions return `arrow.RecordReader` for data streaming.

---

## Core Entities

### 1. Catalog (Interface)

**Purpose**: Top-level metadata container representing available databases/schemas.

**Responsibilities**:
- Return list of available schemas
- Support dynamic implementations (can change over time)
- Be safe for concurrent access

**Interface Definition**:
```go
type Catalog interface {
    // Schemas returns all schemas visible in this catalog.
    // Context allows cancellation and may contain auth info for permission filtering.
    // Implementations MUST respect context cancellation.
    Schemas(ctx context.Context) ([]Schema, error)

    // Schema returns a specific schema by name.
    // Returns nil if schema doesn't exist (not an error - allows existence checking).
    Schema(ctx context.Context, name string) (Schema, error)
}
```

**Validation Rules**:
- `Schemas()` MUST NOT return nil slice (return empty slice `[]Schema{}` if no schemas)
- `Schema()` MUST return `nil, nil` if schema doesn't exist (not an error)
- Implementations MUST be goroutine-safe
- Context deadlines MUST be respected (don't block indefinitely)

**Implementation Types**:
1. **Static Catalog** (from builder): Immutable catalog created at startup
2. **Dynamic Catalog** (user-implemented): Can reflect live database state, permissions, etc.

**State Transitions**: N/A (interface has no internal state)

---

### 2. Schema (Interface)

**Purpose**: Represents a database schema containing tables and functions.

**Responsibilities**:
- Return metadata (name, comment)
- List tables and scalar functions
- Support dynamic implementations

**Interface Definition**:
```go
type Schema interface {
    // Name returns the schema name (e.g., "main", "information_schema").
    // MUST NOT return empty string.
    Name() string

    // Comment returns optional schema documentation.
    // Returns empty string if no comment.
    Comment() string

    // Tables returns all tables in this schema.
    // Context allows cancellation and permission filtering.
    Tables(ctx context.Context) ([]Table, error)

    // Table returns a specific table by name.
    // Returns nil if table doesn't exist (not an error).
    Table(ctx context.Context, name string) (Table, error)

    // ScalarFunctions returns all scalar functions in this schema.
    // Returns empty slice if schema has no functions.
    ScalarFunctions(ctx context.Context) ([]ScalarFunction, error)
}
```

**Validation Rules**:
- `Name()` MUST return non-empty string
- `Tables()` MUST NOT return nil slice (empty slice if no tables)
- `Table()` MUST return `nil, nil` if table doesn't exist
- `ScalarFunctions()` MUST NOT return nil slice
- Implementations MUST be goroutine-safe

**Relationships**:
- Parent: `Catalog` (one catalog contains many schemas)
- Children: `Table`, `ScalarFunction` (one schema contains many tables/functions)

**State Transitions**: N/A (interface has no internal state)

---

### 3. Table (Interface)

**Purpose**: Represents a queryable table or view.

**Responsibilities**:
- Provide table metadata (name, comment, Arrow schema)
- Execute scan operations via user-provided scan function
- Return Arrow data as `arrow.RecordReader`

**Interface Definition**:
```go
type Table interface {
    // Name returns the table name (e.g., "users", "orders").
    // MUST NOT return empty string.
    Name() string

    // Comment returns optional table documentation.
    // Returns empty string if no comment.
    Comment() string

    // ArrowSchema returns the Arrow schema describing table columns.
    // This is the logical schema (column names and types).
    // MUST NOT return nil.
    ArrowSchema() *arrow.Schema

    // Scan executes a scan operation and returns a RecordReader.
    // Context allows cancellation; caller MUST call reader.Release().
    // ScanOptions may contain filters, projections, limits, etc.
    Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}
```

**Validation Rules**:
- `Name()` MUST return non-empty string
- `ArrowSchema()` MUST return valid `*arrow.Schema` (never nil)
- `Scan()` MUST respect context cancellation
- Returned `RecordReader` MUST match `ArrowSchema()` (same field names/types)
- Caller MUST call `reader.Release()` to free memory

**Relationships**:
- Parent: `Schema` (one schema contains many tables)
- Data Source: User-provided `ScanFunc` (table delegates to scan function for data)

**State Transitions**:
```
Created → Ready → Scanning → Ready
                     ↓
                  Error
```

- **Created**: Table registered in catalog
- **Ready**: Available for scanning
- **Scanning**: Actively streaming data via `RecordReader`
- **Error**: Scan failed (returns error, table remains Ready for retry)

---

### 4. ScalarFunction (Interface)

**Purpose**: Represents a user-defined scalar function callable from DuckDB queries.

**Responsibilities**:
- Provide function metadata (name, signature, comment)
- Execute function logic on input batches
- Return results as Arrow arrays

**Interface Definition**:
```go
type ScalarFunction interface {
    // Name returns the function name (e.g., "UPPERCASE", "HASH").
    // MUST NOT return empty string.
    // By convention, use UPPERCASE for function names.
    Name() string

    // Comment returns optional function documentation.
    // Returns empty string if no comment.
    Comment() string

    // Signature returns the function signature (parameter types + return type).
    // Example: "(string) -> string" for UPPERCASE(text) -> text
    Signature() FunctionSignature

    // Execute runs the function on input batch and returns result array.
    // Context allows cancellation.
    // Input arrays match parameter types from Signature.
    // Returned array MUST match return type from Signature.
    // Caller MUST call result.Release().
    Execute(ctx context.Context, inputs []arrow.Array) (arrow.Array, error)
}
```

**Validation Rules**:
- `Name()` MUST return non-empty string (conventionally UPPERCASE)
- `Signature()` MUST return valid signature with at least 1 parameter
- `Execute()` input length MUST match parameter count
- `Execute()` input types MUST match parameter types
- `Execute()` returned array type MUST match return type
- `Execute()` MUST respect context cancellation
- Caller MUST call `result.Release()`

**Relationships**:
- Parent: `Schema` (one schema contains many scalar functions)
- Invoked by: Flight RPC handlers (when DuckDB calls function in query)

**State Transitions**:
```
Registered → Ready → Executing → Ready
                         ↓
                      Error
```

- **Registered**: Function registered in catalog
- **Ready**: Available for invocation
- **Executing**: Actively computing result
- **Error**: Execution failed (returns error, function remains Ready for retry)

---

## Supporting Types

### 5. ServerConfig (Struct)

**Purpose**: Configuration passed to `NewServer()` for server setup.

**Structure**:
```go
type ServerConfig struct {
    // Catalog provides schemas, tables, and functions.
    // REQUIRED: MUST NOT be nil.
    Catalog Catalog

    // Auth provides authentication logic.
    // OPTIONAL: If nil, no authentication (all requests allowed).
    Auth Authenticator

    // Allocator for Arrow memory management.
    // OPTIONAL: Uses memory.DefaultAllocator if nil.
    Allocator memory.Allocator

    // Logger for internal logging.
    // OPTIONAL: Uses slog.Default() if nil.
    Logger *slog.Logger
}
```

**Validation**:
- `Catalog` MUST NOT be nil (validation error if nil)
- `Auth` MAY be nil (no authentication)
- `Allocator` MAY be nil (defaults to `memory.DefaultAllocator`)
- `Logger` MAY be nil (defaults to `slog.Default()`)

**Lifecycle**: Created once, passed to `NewServer()`, not modified after server start.

---

### 6. ScanOptions (Struct)

**Purpose**: Options passed to `Table.Scan()` for filtering, projections, limits.

**Structure**:
```go
type ScanOptions struct {
    // Columns is the list of column names to return.
    // If nil or empty, return all columns.
    Columns []string

    // Filter is optional predicate serialized as Arrow expression.
    // If nil, no filtering (return all rows).
    Filter []byte

    // Limit is maximum rows to return.
    // If 0 or negative, no limit.
    Limit int64

    // BatchSize is hint for RecordReader batch size.
    // If 0, implementation chooses default.
    // Implementations MAY ignore this hint.
    BatchSize int
}
```

**Validation**:
- All fields are optional
- Invalid `Filter` should return error from `Scan()`, not panic

**Lifecycle**: Created per scan request, passed to `Table.Scan()`, discarded after scan completes.

---

### 7. FunctionSignature (Struct)

**Purpose**: Describes scalar function parameter types and return type.

**Structure**:
```go
type FunctionSignature struct {
    // Parameters is list of parameter types (in order).
    // MUST have at least 1 parameter.
    Parameters []arrow.DataType

    // ReturnType is the function's return type.
    // MUST NOT be nil.
    ReturnType arrow.DataType

    // Variadic indicates if last parameter accepts multiple values.
    // Example: CONCAT(string...) has Variadic=true
    Variadic bool
}
```

**Validation**:
- `Parameters` MUST NOT be nil or empty
- `ReturnType` MUST NOT be nil
- All `DataTypes` MUST be valid Arrow types

**Lifecycle**: Immutable, created when function is registered, reused for all invocations.

---

### 8. ScanFunc (Function Type)

**Purpose**: User-provided function that returns table data as `arrow.RecordReader`.

**Signature**:
```go
type ScanFunc func(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
```

**Responsibilities**:
- Create `arrow.RecordReader` that streams table data
- Respect context cancellation
- Apply filters, projections, limits from `opts`
- Return error if scan fails (connection lost, invalid filter, etc.)

**Validation**:
- Returned `RecordReader` schema MUST match table's `ArrowSchema()`
- MUST respect context cancellation (stop generating data if `ctx.Done()`)
- Caller MUST call `reader.Release()`

**Example**:
```go
func scanUsers(ctx context.Context, opts *ScanOptions) (array.RecordReader, error) {
    // Fetch data from database, file, API, etc.
    rows, err := db.QueryContext(ctx, "SELECT id, name, email FROM users")
    if err != nil {
        return nil, err
    }

    // Convert to Arrow RecordReader
    reader := convertRowsToArrowReader(rows, userSchema)
    return reader, nil
}
```

---

### 9. Authenticator (Interface)

**Purpose**: Validates bearer tokens and returns user identity.

**Interface Definition**:
```go
type Authenticator interface {
    // Authenticate validates a bearer token and returns user identity.
    // Returns error if token is invalid or expired.
    // Context allows timeout for auth backend calls.
    Authenticate(ctx context.Context, token string) (identity string, err error)
}
```

**Validation**:
- `Authenticate()` MUST return error for invalid tokens
- `identity` MUST be non-empty string if error is nil
- Implementations MUST be goroutine-safe
- SHOULD respect context timeout (don't block indefinitely)

**Built-in Implementations**:
- `BearerAuth(validateFunc)`: Simple token validation via user-provided function
- Users can implement custom `Authenticator` for JWT, OAuth, etc.

---

## Entity Relationships

```
Catalog (interface)
  └─> Schema (interface)
        ├─> Table (interface)
        │     └─> ScanFunc (executes scan)
        └─> ScalarFunction (interface)

ServerConfig (struct)
  ├─> Catalog (required)
  ├─> Authenticator (optional)
  └─> Allocator (optional)

ScanOptions (struct) ──> Table.Scan()

FunctionSignature (struct) <── ScalarFunction.Signature()
```

**Cardinality**:
- 1 `Catalog` → N `Schema` (1:N)
- 1 `Schema` → N `Table` (1:N)
- 1 `Schema` → N `ScalarFunction` (1:N)
- 1 `Table` → 1 `ScanFunc` (1:1)
- 1 `ServerConfig` → 1 `Catalog` (1:1)
- 1 `ServerConfig` → 0..1 `Authenticator` (1:0..1)

---

## Design Patterns

### Interface-Based Abstraction

**Rationale**: Interfaces allow both static (builder-generated) and dynamic (user-implemented) catalogs. Dynamic catalogs can:
- Reflect live database state (tables added/removed dynamically)
- Filter schemas/tables based on user permissions
- Connect to external metadata stores

**Tradeoff**: Slightly more complexity than concrete structs, but enables powerful extensibility (US-7 requirement).

### Reference Counting for Arrow Memory

**Rationale**: Arrow uses manual reference counting to manage memory. All Arrow objects (`RecordReader`, `Record`, `Array`) have `Release()` methods that decrement refcount.

**Rules**:
- Caller MUST call `Release()` after use
- Use `defer reader.Release()` immediately after creation
- Call `Retain()` before passing ownership to another goroutine

### Context Propagation

**Rationale**: All catalog and scan methods accept `context.Context` for:
- Cancellation (stop work when client disconnects)
- Deadline enforcement (prevent indefinite blocking)
- Value propagation (auth info, tracing IDs)

**Rules**:
- Check `ctx.Done()` in loops
- Use `ctx.Err()` to return cancellation error
- Don't create background contexts (violates cancellation)

---

## Next Steps

Phase 1 continues with:
1. **contracts/**: Go interface signatures matching these entities
2. **quickstart.md**: Example demonstrating static catalog with builder API
