# Data Model: Batch Table Interface Signatures

**Feature**: 013-batch-table-signature
**Date**: 2025-12-30

## Interface Definitions

### UpdatableBatchTable (Updated)

```go
// UpdatableBatchTable extends Table with batch-oriented UPDATE capability.
// The Update method receives the complete input Record including the rowid column.
// Implementations extract rowid values from the rowid column in the Record.
// This interface is preferred over UpdatableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type UpdatableBatchTable interface {
    Table

    // Update modifies existing rows using data from the Record.
    // The rows Record contains both the rowid column (identifying rows to update)
    // and the new column values. Implementations MUST extract rowid values from
    // the rowid column (identified by name "rowid" or metadata key "is_rowid").
    // Use FindRowIDColumn(rows.Schema()) to locate the rowid column.
    // Implementations MUST return an error if any rowid value is null.
    // Row order in Record determines update order.
    // The opts parameter provides options including RETURNING clause information:
    //   - opts.Returning: true if RETURNING clause was specified
    //   - opts.ReturningColumns: column names to include in RETURNING results
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    // Caller MUST call rows.Release() after Update returns.
    Update(ctx context.Context, rows arrow.Record, opts *DMLOptions) (*DMLResult, error)
}
```

### DeletableBatchTable (Updated)

```go
// DeletableBatchTable extends Table with batch-oriented DELETE capability.
// The Delete method receives a Record containing the rowid column.
// Implementations extract rowid values from the rowid column in the Record.
// This interface is preferred over DeletableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type DeletableBatchTable interface {
    Table

    // Delete removes rows identified by rowid values in the Record.
    // The rows Record contains the rowid column (identified by name "rowid"
    // or metadata key "is_rowid") that identifies rows to delete.
    // Use FindRowIDColumn(rows.Schema()) to locate the rowid column.
    // Implementations MUST return an error if any rowid value is null.
    // The opts parameter provides options including RETURNING clause information:
    //   - opts.Returning: true if RETURNING clause was specified
    //   - opts.ReturningColumns: column names to include in RETURNING results
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    // Caller MUST call rows.Release() after Delete returns.
    Delete(ctx context.Context, rows arrow.Record, opts *DMLOptions) (*DMLResult, error)
}
```

## Key Types (Unchanged)

### DMLOptions

```go
// DMLOptions configures DML operation behavior.
type DMLOptions struct {
    // Returning indicates if RETURNING clause was specified.
    Returning bool

    // ReturningColumns lists column names to include in RETURNING results.
    // Empty means all columns.
    ReturningColumns []string
}
```

### DMLResult

```go
// DMLResult contains the result of a DML operation.
type DMLResult struct {
    // AffectedRows is the number of rows affected by the operation.
    AffectedRows int64

    // ReturningData contains data for RETURNING clause if requested.
    // May be nil if RETURNING was not requested.
    ReturningData array.RecordReader
}
```

## Type Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                         Table                                │
│  - Name() string                                            │
│  - Comment() string                                         │
│  - ArrowSchema(columns []string) *arrow.Schema              │
│  - Scan(ctx, opts) (array.RecordReader, error)              │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ extends
        ┌─────────────────────┴─────────────────────┐
        │                                           │
┌───────┴───────────────────┐     ┌─────────────────┴───────────┐
│   UpdatableBatchTable     │     │   DeletableBatchTable       │
│                           │     │                             │
│ Update(ctx, rows Record,  │     │ Delete(ctx, rows Record,    │
│        opts) (*Result, e) │     │        opts) (*Result, e)   │
└───────────────────────────┘     └─────────────────────────────┘
        │                                           │
        │ preferred over                            │ preferred over
        ▼                                           ▼
┌───────────────────────────┐     ┌─────────────────────────────┐
│   UpdatableTable (legacy) │     │   DeletableTable (legacy)   │
│                           │     │                             │
│ Update(ctx, rowIDs []int64│     │ Delete(ctx, rowIDs []int64, │
│   rows RecordReader, opts)│     │        opts) (*Result, e)   │
└───────────────────────────┘     └─────────────────────────────┘
```

## Signature Change Summary

| Interface | Method | Before | After |
|-----------|--------|--------|-------|
| UpdatableBatchTable | Update | `rows array.RecordReader` | `rows arrow.Record` |
| DeletableBatchTable | Delete | `rows array.RecordReader` | `rows arrow.Record` |

## Error Handling

New error condition added:

```go
// ErrNullRowID is returned when a null rowid value is encountered.
var ErrNullRowID = errors.New("null rowid value not allowed")
```

When implementations encounter a null rowid in the Record, they MUST return an error immediately without processing any rows.
