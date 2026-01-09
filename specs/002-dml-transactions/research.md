# Research: DML Operations and Transaction Management

**Feature**: 002-dml-transactions
**Date**: 2025-11-28
**Status**: Complete

## Research Topics

### 1. Arrow Flight DML Operation Patterns

**Question**: How should DML operations (INSERT, UPDATE, DELETE) be implemented over Arrow Flight RPC?

**Decision**: Use custom JSON descriptor + MessagePack result pattern (current approach)

**Rationale**:
- Arrow Flight SQL defines `CommandStatementUpdate` with protobuf, but airport-go uses a simpler JSON/MessagePack approach
- JSON descriptors are human-readable and easier to debug
- MessagePack results are efficient and flexible for metadata
- Current approach already documented in existing DML contract and aligns with codebase conventions

**Alternatives Considered**:
1. **Full Arrow Flight SQL compliance** - Rejected: More complex, requires protobuf schema management, overkill for library scope
2. **Path-only descriptors** - Rejected: Insufficient for complex operations (UPDATE, DELETE) that need additional metadata

**Implementation Pattern**:
```go
// JSON descriptor for DML operations
{
    "operation": "insert|update|delete",
    "schema_name": "string",
    "table_name": "string",
    "row_ids": [int64],     // For UPDATE/DELETE
    "returning": true|false  // Optional
}

// MessagePack result
{
    "status": "success",
    "affected_rows": int64,
    "returning_data": []byte  // Serialized Arrow RecordBatch if returning=true
}
```

---

### 2. RETURNING Clause Implementation

**Question**: How should RETURNING clause data be communicated back to clients?

**Decision**: Inline serialized Arrow RecordBatch in PutResult app_metadata

**Rationale**:
- Most RETURNING queries return small result sets (inserted/updated rows)
- Inline approach avoids extra roundtrip (compared to separate DoGet)
- Consistent with MessagePack result pattern
- Simple client implementation (decode metadata, deserialize IPC)

**Alternatives Considered**:
1. **Separate DoGet with ticket** - Rejected for simplicity; can add later if needed for large results
2. **Streaming results in DoPut** - Rejected: Complicates protocol, not needed for typical use cases

**Implementation Pattern**:
```go
// When RETURNING clause requested
if descriptor.Returning {
    returningBatch := executeWithReturning(ctx, ...)

    var buf bytes.Buffer
    writer := ipc.NewWriter(&buf, ipc.WithSchema(returningBatch.Schema()))
    writer.Write(returningBatch)
    writer.Close()

    result := map[string]any{
        "status": "success",
        "affected_rows": affectedRows,
        "returning_data": buf.Bytes(),
    }
    return &flight.PutResult{AppMetadata: msgpack.Encode(result)}
}
```

---

### 3. Transaction Manager Interface Design

**Question**: What interface pattern best supports optional transaction coordination?

**Decision**: Simple Begin/Commit/Rollback interface with context-based ID propagation

**Rationale**:
- Follows patterns from pgx, database/sql, and GORM
- Interface is minimal - only coordination, not full ACID guarantees
- Context-based propagation aligns with gRPC patterns
- Idempotent commit/rollback enables safe deferred cleanup
- Optional by design (nil check pattern matches existing Auth pattern)

**Alternatives Considered**:
1. **Callback pattern only** (like pgx.BeginFunc) - Rejected: Doesn't fit request-scoped Flight handlers
2. **Transaction object passed through handlers** - Rejected: Clutters signatures, context is cleaner
3. **Transaction embedded in catalog** - Rejected: Violates separation of concerns

**Interface Design**:
```go
type TransactionManager interface {
    BeginTransaction(ctx context.Context) (txID string, err error)
    CommitTransaction(ctx context.Context, txID string) error
    RollbackTransaction(ctx context.Context, txID string) error
    GetTransactionStatus(ctx context.Context, txID string) (TransactionState, bool)
}

type TransactionState string
const (
    TransactionActive    TransactionState = "active"
    TransactionCommitted TransactionState = "committed"
    TransactionAborted   TransactionState = "aborted"
)
```

---

### 4. Transaction Context Propagation

**Question**: How should transaction IDs flow through the request lifecycle?

**Decision**: gRPC metadata header + context.Value propagation

**Rationale**:
- gRPC metadata (`x-transaction-id` header) is standard for cross-service context
- context.Value with unexported key prevents collisions
- Pattern matches existing auth context propagation in codebase
- Works naturally with gRPC interceptor patterns

**Alternatives Considered**:
1. **Command descriptor field** - Rejected as primary: Requires descriptor parsing before context setup
2. **Separate RPC parameter** - Rejected: Changes method signatures, inconsistent with Flight spec

**Implementation Pattern**:
```go
// Unexported key type prevents collisions
type txKey struct{}

func WithTransactionID(ctx context.Context, txID string) context.Context {
    return context.WithValue(ctx, txKey{}, txID)
}

func TransactionIDFromContext(ctx context.Context) (string, bool) {
    txID, ok := ctx.Value(txKey{}).(string)
    return txID, ok
}

// Extraction from gRPC metadata
func extractTransactionID(ctx context.Context) string {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok { return "" }
    if txIDs := md.Get("x-transaction-id"); len(txIDs) > 0 {
        return txIDs[0]
    }
    return ""
}
```

---

### 5. Table Capability Interfaces

**Question**: How should DML capability checking be structured?

**Decision**: Optional interfaces (InsertableTable, UpdatableTable, DeletableTable) via type assertion

**Rationale**:
- Follows Go's interface composition pattern
- Existing Table interface remains unchanged (backward compatible)
- Type assertion pattern (`if it, ok := table.(InsertableTable); ok`) is idiomatic
- Clear error messages when capability not supported
- Aligns with existing catalog patterns (DynamicSchemaTable)

**Alternatives Considered**:
1. **Single interface with all methods** - Rejected: Forces implementation of unused methods
2. **Capability flags on Table** - Rejected: Runtime checks instead of compile-time type safety
3. **Separate registry** - Rejected: Over-engineering, harder to use

**Interface Design**:
```go
// InsertableTable represents a table that supports INSERT operations.
type InsertableTable interface {
    Table
    Insert(ctx context.Context, rows array.RecordReader) (*DMLResult, error)
}

// UpdatableTable represents a table that supports UPDATE operations.
// Requires rowid for identifying rows to update.
type UpdatableTable interface {
    Table
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*DMLResult, error)
}

// DeletableTable represents a table that supports DELETE operations.
// Requires rowid for identifying rows to delete.
type DeletableTable interface {
    Table
    Delete(ctx context.Context, rowIDs []int64) (*DMLResult, error)
}

// DMLResult contains the outcome of a DML operation.
type DMLResult struct {
    AffectedRows  int64
    ReturningData array.RecordReader // nil if no RETURNING clause
}
```

---

### 6. Automatic Commit/Rollback Pattern

**Question**: How should handlers automatically manage transaction lifecycle?

**Decision**: Wrapper function with deferred rollback and explicit commit

**Rationale**:
- Deferred rollback handles both errors and panics safely
- Idempotent operations prevent double-commit/rollback issues
- Clear control flow: rollback on error, commit on success
- Centralized logic avoids duplication across handlers

**Implementation Pattern**:
```go
func (s *Server) withTransaction(ctx context.Context, fn func(context.Context) error) error {
    txID, hasTx := TransactionIDFromContext(ctx)
    if !hasTx || s.txManager == nil {
        return fn(ctx) // No transaction - execute directly
    }

    // Execute with automatic commit/rollback
    err := fn(ctx)

    if err != nil {
        if rbErr := s.txManager.RollbackTransaction(ctx, txID); rbErr != nil {
            s.logger.Error("rollback failed", "tx_id", txID, "error", rbErr)
        }
        return err
    }

    return s.txManager.CommitTransaction(ctx, txID)
}
```

---

### 7. Error Handling Strategy

**Question**: What gRPC status codes should be used for DML errors?

**Decision**: Map errors to specific gRPC codes for client actionability

| Error Condition | gRPC Code | Message Pattern |
|-----------------|-----------|-----------------|
| Table not found | `NotFound` | "table {schema}.{table} not found" |
| Schema not found | `NotFound` | "schema {schema} not found" |
| Table is read-only | `FailedPrecondition` | "table {name} does not support INSERT" |
| Table lacks rowid | `FailedPrecondition` | "table {name} requires rowid for UPDATE/DELETE" |
| Schema mismatch | `InvalidArgument` | "column mismatch: expected {expected}, got {actual}" |
| Invalid row IDs | `InvalidArgument` | "row IDs cannot be empty for UPDATE/DELETE" |
| Transaction invalid | `FailedPrecondition` | "transaction {txID} is not active" |
| No tx manager | `Unimplemented` | "transaction manager not configured" |
| Storage failure | `Internal` | "storage error: {details}" |

**Rationale**: Specific codes enable clients to handle errors appropriately (retry, report, fail fast).

---

## Summary of Decisions

| Topic | Decision | Key Rationale |
|-------|----------|---------------|
| DML Protocol | JSON descriptor + MessagePack result | Simplicity, debuggability |
| RETURNING | Inline serialized RecordBatch | Avoids extra roundtrip |
| Transaction Interface | Begin/Commit/Rollback with string IDs | Matches Go idioms |
| Context Propagation | gRPC metadata + context.Value | Standard gRPC pattern |
| Table Capabilities | Optional interfaces via type assertion | Backward compatible |
| Auto Commit/Rollback | Wrapper function with deferred cleanup | Panic-safe, centralized |
| Error Codes | Specific gRPC status codes | Client actionability |

## References

- Arrow Flight SQL specification: `format/FlightSql.proto`
- Go database/sql patterns: https://pkg.go.dev/database/sql
- pgx transaction handling: https://pkg.go.dev/github.com/jackc/pgx/v5
- gRPC metadata: https://grpc.io/docs/guides/metadata/
- Existing airport-go patterns: `catalog/table.go`, `flight/doexchange.go`
