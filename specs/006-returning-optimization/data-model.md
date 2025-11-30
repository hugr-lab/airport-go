# Data Model: DML RETURNING Clause Column Selection

**Date**: 2025-11-30
**Feature**: 006-returning-optimization

## New Types

### DMLOptions

Options struct for DML operations (INSERT, UPDATE, DELETE).

```go
// DMLOptions carries options for DML operations.
// Designed for extensibility - new fields can be added without breaking interfaces.
type DMLOptions struct {
    // Returning indicates whether a RETURNING clause was specified in the SQL statement.
    // When true, the implementation should populate DMLResult.ReturningData.
    // When false, no RETURNING data is expected (DMLResult.ReturningData should be nil).
    Returning bool

    // ReturningColumns specifies which columns to include in RETURNING results.
    // Only meaningful when Returning is true.
    //
    // Semantics:
    // - If Returning=false: ReturningColumns is ignored
    // - If Returning=true and ReturningColumns is nil/empty: return all columns (RETURNING *)
    // - If Returning=true and ReturningColumns is non-empty: return only specified columns
    //
    // Note: DuckDB Airport extension communicates only whether RETURNING is requested
    // (via return-chunks header), not which specific columns. The server populates
    // ReturningColumns from the input schema column names. Implementations can use
    // this for optimization.
    ReturningColumns []string
}
```

**Location**: `catalog/types.go`

**Design Rationale**:
- Struct (not individual parameters) enables future extensibility
- Pointer semantics allow nil-checking for default behavior
- Slice allows any number of columns including "all" (empty/nil)

## Modified Interfaces

### InsertableTable

```go
// InsertableTable extends Table with INSERT capability.
// Tables implement this interface to accept new rows via DoPut.
// Implementations MUST be goroutine-safe.
type InsertableTable interface {
    Table

    // Insert adds new rows to the table.
    // The rows RecordReader provides batches of data to insert.
    // The opts parameter provides options including RETURNING column selection.
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    // Caller MUST call rows.Release() after Insert returns.
    Insert(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

**Changes**:
- Added `opts *DMLOptions` parameter
- Updated godoc to describe opts usage

### UpdatableTable

```go
// UpdatableTable extends Table with UPDATE capability.
// Tables must have a rowid mechanism to identify rows for update.
// Implementations MUST be goroutine-safe.
type UpdatableTable interface {
    Table

    // Update modifies existing rows identified by rowIDs.
    // The rows RecordReader provides replacement data for matched rows.
    // Row order in RecordReader must correspond to rowIDs order.
    // The opts parameter provides options including RETURNING column selection.
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

**Changes**:
- Added `opts *DMLOptions` parameter
- Updated godoc to describe opts usage

### DeletableTable

```go
// DeletableTable extends Table with DELETE capability.
// Tables must have a rowid mechanism to identify rows for deletion.
// Implementations MUST be goroutine-safe.
type DeletableTable interface {
    Table

    // Delete removes rows identified by rowIDs.
    // The opts parameter provides options including RETURNING column selection.
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    Delete(ctx context.Context, rowIDs []int64, opts *DMLOptions) (*DMLResult, error)
}
```

**Changes**:
- Added `opts *DMLOptions` parameter
- Updated godoc to describe opts usage

## Existing Types (Unchanged)

### DMLResult

```go
// DMLResult holds the outcome of INSERT, UPDATE, or DELETE operations.
// Returned by InsertableTable.Insert, UpdatableTable.Update, and DeletableTable.Delete.
type DMLResult struct {
    // AffectedRows is the count of rows inserted, updated, or deleted.
    AffectedRows int64

    // ReturningData contains rows affected by the operation when
    // a RETURNING clause was specified. nil if no RETURNING requested.
    // Caller is responsible for releasing resources (RecordReader.Release).
    ReturningData array.RecordReader
}
```

**No changes** - existing structure is sufficient.

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        DuckDB Client                             │
│  INSERT INTO t (a,b,c) VALUES (...) RETURNING id                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DoExchange RPC (gRPC)                          │
│  Headers:                                                        │
│    airport-operation: "insert"                                   │
│    airport-flight-path: "schema/table"                           │
│    return-chunks: "1"                                            │
│  Data: Arrow RecordBatch with columns (a, b, c)                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              flight/doexchange_dml.go                            │
│  1. Parse headers, extract returnData = true                     │
│  2. Create DMLOptions{ReturningColumns: nil} (empty for now)    │
│  3. Call table.Insert(ctx, rows, opts)                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              Table Implementation                                │
│  1. Insert data                                                  │
│  2. If opts.ReturningColumns empty → return all available cols  │
│  3. If opts.ReturningColumns set → filter/optimize (optional)   │
│  4. Return DMLResult with ReturningData                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              flight/doexchange_dml.go                            │
│  1. Send ReturningData batches to client via stream             │
│  2. Send final metadata with total_changed count                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DuckDB Client                             │
│  Filters returned columns to match RETURNING clause (id only)   │
│  Returns result to user                                          │
└─────────────────────────────────────────────────────────────────┘
```

## Validation Rules

1. **DMLOptions.ReturningColumns**:
   - Empty/nil = return all available columns (implementation decides)
   - Non-empty = hint for optimization; implementations MAY honor or ignore
   - Invalid column names = implementation-specific behavior (error or ignore)

2. **DMLResult.ReturningData**:
   - nil when no RETURNING requested (`return-chunks: 0`)
   - RecordReader when RETURNING requested; schema determined by implementation
   - Caller MUST release after consuming

## Future Extensibility

`DMLOptions` struct can be extended with additional fields:

```go
type DMLOptions struct {
    ReturningColumns []string

    // Future additions (examples):
    // ConflictResolution string  // ON CONFLICT behavior
    // BatchSize          int     // Hint for batch processing
    // Timeout            time.Duration
}
```

Adding fields is non-breaking since existing code passing `&DMLOptions{ReturningColumns: cols}` continues to work.
