# Data Model: DML Operations and Transaction Management

**Feature**: 002-dml-transactions
**Date**: 2025-11-28
**Status**: Draft

## Entity Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Catalog Package                                │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    implements    ┌──────────────────┐                │
│  │    Table     │◄─────────────────│  InsertableTable │                │
│  │  (existing)  │                  │  UpdatableTable  │                │
│  └──────────────┘                  │  DeletableTable  │                │
│         ▲                          └──────────────────┘                │
│         │                                                               │
│  ┌──────────────┐                  ┌──────────────────┐                │
│  │ StaticTable  │                  │    DMLResult     │                │
│  │  (existing)  │                  │  (new type)      │                │
│  └──────────────┘                  └──────────────────┘                │
│                                                                         │
│  ┌────────────────────┐            ┌──────────────────┐                │
│  │ TransactionManager │            │ TransactionState │                │
│  │  (new interface)   │            │   (new enum)     │                │
│  └────────────────────┘            └──────────────────┘                │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          Flight Package                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐              ┌──────────────────┐                │
│  │ InsertDescriptor │              │  DeleteAction    │                │
│  │  (existing)      │              │  (existing)      │                │
│  └──────────────────┘              └──────────────────┘                │
│                                                                         │
│  ┌──────────────────┐              ┌──────────────────┐                │
│  │ UpdateDescriptor │              │   DMLResponse    │                │
│  │  (existing)      │              │  (new type)      │                │
│  └──────────────────┘              └──────────────────┘                │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Catalog Package Entities

### InsertableTable Interface

**Purpose**: Represents a table that supports INSERT operations.

```go
// InsertableTable extends Table with INSERT capability.
// Tables implement this interface to accept new rows.
type InsertableTable interface {
    Table

    // Insert adds new rows to the table.
    // The rows RecordReader provides batches of data to insert.
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    Insert(ctx context.Context, rows array.RecordReader) (*DMLResult, error)
}
```

**Validation Rules**:
- Input RecordReader schema must match table schema
- Context cancellation must be respected
- Transaction context (if present) must be propagated to storage

**Relationships**:
- Extends `catalog.Table` interface
- Returns `*DMLResult`

---

### UpdatableTable Interface

**Purpose**: Represents a table that supports UPDATE operations on identified rows.

```go
// UpdatableTable extends Table with UPDATE capability.
// Tables must have a rowid mechanism to identify rows for update.
type UpdatableTable interface {
    Table

    // Update modifies existing rows identified by rowIDs.
    // The rows RecordReader provides replacement data for matched rows.
    // Row order in RecordReader must correspond to rowIDs order.
    // Returns DMLResult with affected row count and optional returning data.
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*DMLResult, error)
}
```

**Validation Rules**:
- rowIDs slice must not be empty
- rowIDs length must match total row count in RecordReader
- All rowIDs must exist in table (or error returned)
- RecordReader schema must match table schema (excluding rowid)

**Relationships**:
- Extends `catalog.Table` interface
- Returns `*DMLResult`

---

### DeletableTable Interface

**Purpose**: Represents a table that supports DELETE operations on identified rows.

```go
// DeletableTable extends Table with DELETE capability.
// Tables must have a rowid mechanism to identify rows for deletion.
type DeletableTable interface {
    Table

    // Delete removes rows identified by rowIDs.
    // Returns DMLResult with affected row count and optional returning data.
    Delete(ctx context.Context, rowIDs []int64) (*DMLResult, error)
}
```

**Validation Rules**:
- rowIDs slice must not be empty
- Non-existent rowIDs may be silently ignored or return error (implementation-defined)

**Relationships**:
- Extends `catalog.Table` interface
- Returns `*DMLResult`

---

### DMLResult Type

**Purpose**: Contains the outcome of a DML operation.

```go
// DMLResult holds the outcome of INSERT, UPDATE, or DELETE operations.
type DMLResult struct {
    // AffectedRows is the count of rows inserted, updated, or deleted.
    // For INSERT: number of rows successfully inserted.
    // For UPDATE: number of rows matched and modified.
    // For DELETE: number of rows removed.
    AffectedRows int64

    // ReturningData contains rows affected by the operation when
    // a RETURNING clause was specified. nil if no RETURNING requested.
    // Caller is responsible for releasing resources (RecordReader.Release).
    ReturningData array.RecordReader
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| AffectedRows | int64 | Yes | Count of affected rows |
| ReturningData | array.RecordReader | No | Returned rows (RETURNING clause) |

**State Transitions**: N/A (immutable result type)

---

### TransactionManager Interface

**Purpose**: Coordinates transactions across multiple DML operations.

```go
// TransactionManager coordinates transactions across operations.
// This interface is OPTIONAL - servers operate normally without it.
// Implementations handle persistence and transaction state management.
type TransactionManager interface {
    // BeginTransaction creates a new transaction and returns its unique ID.
    // The ID should be globally unique (UUID recommended).
    BeginTransaction(ctx context.Context) (txID string, err error)

    // CommitTransaction marks a transaction as successfully completed.
    // Called automatically by Flight handlers on operation success.
    // Idempotent - safe to call multiple times with same txID.
    CommitTransaction(ctx context.Context, txID string) error

    // RollbackTransaction aborts a transaction.
    // Called automatically by Flight handlers on operation failure.
    // Idempotent - safe to call multiple times with same txID.
    RollbackTransaction(ctx context.Context, txID string) error

    // GetTransactionStatus returns the current state of a transaction.
    // Returns (state, true) if transaction exists, ("", false) otherwise.
    GetTransactionStatus(ctx context.Context, txID string) (TransactionState, bool)
}
```

**Validation Rules**:
- BeginTransaction must generate unique IDs (no collisions)
- Commit/Rollback must be idempotent (safe to call multiple times)
- GetTransactionStatus must not modify state

**Relationships**:
- Used by `flight.Server` (optional dependency)
- Returns `TransactionState`

---

### TransactionState Type

**Purpose**: Represents the lifecycle stage of a transaction.

```go
// TransactionState represents the lifecycle stage of a transaction.
type TransactionState string

const (
    // TransactionActive indicates an open transaction awaiting operations.
    TransactionActive TransactionState = "active"

    // TransactionCommitted indicates a successfully completed transaction.
    TransactionCommitted TransactionState = "committed"

    // TransactionAborted indicates a rolled-back transaction.
    TransactionAborted TransactionState = "aborted"
)
```

**State Transition Diagram**:
```
                    ┌─────────┐
                    │  (new)  │
                    └────┬────┘
                         │ BeginTransaction()
                         ▼
                    ┌─────────┐
          ┌─────────│ Active  │─────────┐
          │         └─────────┘         │
          │ RollbackTransaction()       │ CommitTransaction()
          ▼                             ▼
    ┌───────────┐                 ┌───────────┐
    │  Aborted  │                 │ Committed │
    └───────────┘                 └───────────┘
```

---

## Flight Package Entities

### InsertDescriptor (existing, enhanced)

**Purpose**: Descriptor for INSERT operations sent via DoPut.

```go
// InsertDescriptor is the JSON-encoded command for INSERT operations.
type InsertDescriptor struct {
    Operation  string `json:"operation"`            // Always "insert"
    SchemaName string `json:"schema_name"`          // Target schema
    TableName  string `json:"table_name"`           // Target table
    Returning  bool   `json:"returning,omitempty"`  // Request RETURNING data
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Operation | string | Yes | Must be "insert" |
| SchemaName | string | Yes | Schema containing target table |
| TableName | string | Yes | Table to insert into |
| Returning | bool | No | If true, return inserted rows |

---

### UpdateDescriptor (existing, enhanced)

**Purpose**: Descriptor for UPDATE operations sent via DoPut.

```go
// UpdateDescriptor is the JSON-encoded command for UPDATE operations.
type UpdateDescriptor struct {
    Operation  string  `json:"operation"`           // Always "update"
    SchemaName string  `json:"schema_name"`         // Target schema
    TableName  string  `json:"table_name"`          // Target table
    RowIDs     []int64 `json:"row_ids"`             // Rows to update
    Returning  bool    `json:"returning,omitempty"` // Request RETURNING data
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Operation | string | Yes | Must be "update" |
| SchemaName | string | Yes | Schema containing target table |
| TableName | string | Yes | Table to update |
| RowIDs | []int64 | Yes | IDs of rows to modify |
| Returning | bool | No | If true, return updated rows |

---

### DeleteAction (existing, enhanced)

**Purpose**: Action body for DELETE operations sent via DoAction.

```go
// DeleteAction is the JSON-encoded body for DELETE operations.
type DeleteAction struct {
    SchemaName string  `json:"schema_name"`         // Target schema
    TableName  string  `json:"table_name"`          // Target table
    RowIDs     []int64 `json:"row_ids"`             // Rows to delete
    Returning  bool    `json:"returning,omitempty"` // Request RETURNING data
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| SchemaName | string | Yes | Schema containing target table |
| TableName | string | Yes | Table to delete from |
| RowIDs | []int64 | Yes | IDs of rows to remove |
| Returning | bool | No | If true, return deleted rows |

---

### DMLResponse Type

**Purpose**: Standard response format for all DML operations.

```go
// DMLResponse is the MessagePack-encoded result of DML operations.
type DMLResponse struct {
    Status        string `msgpack:"status"`                   // "success" or "error"
    AffectedRows  int64  `msgpack:"affected_rows"`            // Count of affected rows
    ReturningData []byte `msgpack:"returning_data,omitempty"` // Arrow IPC bytes
    ErrorMessage  string `msgpack:"error_message,omitempty"`  // Error details
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| Status | string | Yes | Operation outcome |
| AffectedRows | int64 | Yes | Rows affected |
| ReturningData | []byte | No | Serialized RecordBatch |
| ErrorMessage | string | No | Error details (when Status="error") |

---

## Transaction Context Types

### Context Key and Helpers

**Purpose**: Propagate transaction ID through request context.

```go
// Package txcontext provides transaction context utilities.
package txcontext

// txKey is the unexported context key for transaction ID.
type txKey struct{}

// WithTransactionID returns a new context with the transaction ID stored.
func WithTransactionID(ctx context.Context, txID string) context.Context {
    return context.WithValue(ctx, txKey{}, txID)
}

// TransactionIDFromContext retrieves the transaction ID if present.
// Returns ("", false) if no transaction ID is set.
func TransactionIDFromContext(ctx context.Context) (string, bool) {
    txID, ok := ctx.Value(txKey{}).(string)
    return txID, ok
}
```

---

## Capability Detection Pattern

**Purpose**: Determine table capabilities at runtime via type assertion.

```go
// Check if table supports INSERT
func canInsert(table catalog.Table) bool {
    _, ok := table.(catalog.InsertableTable)
    return ok
}

// Check if table supports UPDATE
func canUpdate(table catalog.Table) bool {
    _, ok := table.(catalog.UpdatableTable)
    return ok
}

// Check if table supports DELETE
func canDelete(table catalog.Table) bool {
    _, ok := table.(catalog.DeletableTable)
    return ok
}
```

---

## Error Types

| Error Condition | gRPC Code | Error Message Pattern |
|-----------------|-----------|----------------------|
| Schema not found | NotFound | "schema {name} not found" |
| Table not found | NotFound | "table {schema}.{table} not found" |
| INSERT not supported | FailedPrecondition | "table {name} does not support INSERT operations" |
| UPDATE not supported | FailedPrecondition | "table {name} does not support UPDATE operations (no rowid)" |
| DELETE not supported | FailedPrecondition | "table {name} does not support DELETE operations (no rowid)" |
| Empty row IDs | InvalidArgument | "row_ids cannot be empty for UPDATE/DELETE" |
| Schema mismatch | InvalidArgument | "schema mismatch: {details}" |
| Transaction not active | FailedPrecondition | "transaction {txID} is not active (state: {state})" |
| No transaction manager | Unimplemented | "transaction manager not configured" |

---

## File Locations

| Entity | Package | File |
|--------|---------|------|
| InsertableTable | catalog | `catalog/table.go` |
| UpdatableTable | catalog | `catalog/table.go` |
| DeletableTable | catalog | `catalog/table.go` |
| DMLResult | catalog | `catalog/types.go` |
| TransactionManager | catalog | `catalog/transaction.go` |
| TransactionState | catalog | `catalog/transaction.go` |
| InsertDescriptor | flight | `flight/dml_types.go` |
| UpdateDescriptor | flight | `flight/dml_types.go` |
| DeleteAction | flight | `flight/dml_types.go` |
| DMLResponse | flight | `flight/dml_types.go` |
| txcontext helpers | internal | `internal/txcontext/context.go` |
