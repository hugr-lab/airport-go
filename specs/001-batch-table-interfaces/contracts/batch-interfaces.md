# API Contracts: Batch Table Interfaces

**Feature Branch**: `001-batch-table-interfaces`
**Date**: 2025-12-29

## Overview

This document defines the API contracts for the new batch table interfaces. Since this is a Go library (not a web service), contracts are defined as Go interface specifications.

---

## 1. UpdatableBatchTable Interface

### Definition

**Package**: `github.com/hugr-lab/airport-go/catalog`
**File**: `catalog/table.go`

```go
// UpdatableBatchTable extends Table with batch-oriented UPDATE capability.
// The Update method receives the complete input RecordReader including the rowid column.
// Implementations extract rowid values from the rowid column in the RecordReader.
// This interface is preferred over UpdatableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type UpdatableBatchTable interface {
    Table

    // Update modifies existing rows using data from the RecordReader.
    //
    // Parameters:
    //   - ctx: Context for cancellation and transaction coordination.
    //          May contain transaction ID from catalog.TransactionIDFromContext().
    //   - rows: RecordReader containing rowid column and updated column values.
    //           Caller is responsible for releasing after Update returns.
    //   - opts: DML options including RETURNING clause information.
    //           May be nil (use defaults).
    //
    // Returns:
    //   - DMLResult with AffectedRows count and optional ReturningData.
    //   - Error if update fails (e.g., invalid rowid, constraint violation).
    //
    // The rows RecordReader schema contains:
    //   - Updated column values (order may vary)
    //   - rowid column (identified by name "rowid" or metadata key "is_rowid")
    //
    // Use catalog.FindRowIDColumn(rows.Schema()) to locate the rowid column.
    //
    // Implementation Requirements:
    //   - MUST be goroutine-safe
    //   - MUST respect ctx.Done() for cancellation
    //   - MUST NOT release the rows RecordReader (caller responsibility)
    //   - SHOULD return ReturningData if opts.Returning is true
    Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### Contract Tests

```go
// TestUpdatableBatchTable_Update_Basic
// Given: A table implementing UpdatableBatchTable with 3 existing rows
// When: Update is called with RecordReader containing 2 rows (rowid=1, rowid=2)
// Then: 2 rows are updated, DMLResult.AffectedRows == 2

// TestUpdatableBatchTable_Update_Returning
// Given: A table implementing UpdatableBatchTable
// When: Update is called with opts.Returning = true
// Then: DMLResult.ReturningData contains updated rows

// TestUpdatableBatchTable_Update_NoRowidColumn
// Given: A table implementing UpdatableBatchTable
// When: Update is called with RecordReader missing rowid column
// Then: Error returned indicating missing rowid

// TestUpdatableBatchTable_Update_EmptyReader
// Given: A table implementing UpdatableBatchTable
// When: Update is called with empty RecordReader (no rows)
// Then: DMLResult.AffectedRows == 0, no error

// TestUpdatableBatchTable_Update_ContextCancellation
// Given: A table implementing UpdatableBatchTable
// When: Update is called with cancelled context
// Then: Error returned, operation aborted
```

---

## 2. DeletableBatchTable Interface

### Definition

**Package**: `github.com/hugr-lab/airport-go/catalog`
**File**: `catalog/table.go`

```go
// DeletableBatchTable extends Table with batch-oriented DELETE capability.
// The Delete method receives a RecordReader containing the rowid column.
// Implementations extract rowid values from the rowid column in the RecordReader.
// This interface is preferred over DeletableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type DeletableBatchTable interface {
    Table

    // Delete removes rows identified by rowid values in the RecordReader.
    //
    // Parameters:
    //   - ctx: Context for cancellation and transaction coordination.
    //          May contain transaction ID from catalog.TransactionIDFromContext().
    //   - rows: RecordReader containing the rowid column.
    //           May contain additional columns (ignored for deletion).
    //           Caller is responsible for releasing after Delete returns.
    //   - opts: DML options including RETURNING clause information.
    //           May be nil (use defaults).
    //
    // Returns:
    //   - DMLResult with AffectedRows count and optional ReturningData.
    //   - Error if deletion fails (e.g., constraint violation).
    //
    // The rows RecordReader schema contains:
    //   - rowid column (identified by name "rowid" or metadata key "is_rowid")
    //   - Optionally other columns (typically ignored)
    //
    // Use catalog.FindRowIDColumn(rows.Schema()) to locate the rowid column.
    //
    // Implementation Requirements:
    //   - MUST be goroutine-safe
    //   - MUST respect ctx.Done() for cancellation
    //   - MUST NOT release the rows RecordReader (caller responsibility)
    //   - SHOULD return ReturningData if opts.Returning is true
    Delete(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### Contract Tests

```go
// TestDeletableBatchTable_Delete_Basic
// Given: A table implementing DeletableBatchTable with 5 existing rows
// When: Delete is called with RecordReader containing 3 rowids
// Then: 3 rows are deleted, DMLResult.AffectedRows == 3

// TestDeletableBatchTable_Delete_Returning
// Given: A table implementing DeletableBatchTable
// When: Delete is called with opts.Returning = true
// Then: DMLResult.ReturningData contains deleted rows (before deletion)

// TestDeletableBatchTable_Delete_NoRowidColumn
// Given: A table implementing DeletableBatchTable
// When: Delete is called with RecordReader missing rowid column
// Then: Error returned indicating missing rowid

// TestDeletableBatchTable_Delete_EmptyReader
// Given: A table implementing DeletableBatchTable
// When: Delete is called with empty RecordReader (no rows)
// Then: DMLResult.AffectedRows == 0, no error

// TestDeletableBatchTable_Delete_ContextCancellation
// Given: A table implementing DeletableBatchTable
// When: Delete is called with cancelled context
// Then: Error returned, operation aborted
```

---

## 3. FindRowIDColumn Helper

### Definition

**Package**: `github.com/hugr-lab/airport-go/catalog`
**File**: `catalog/helpers.go`

```go
// FindRowIDColumn returns the index of the rowid column in the schema.
// Returns -1 if no rowid column is found.
//
// Rowid column is identified by:
//   - Column name "rowid" (case-sensitive), or
//   - Metadata key "is_rowid" with non-empty value
//
// Parameters:
//   - schema: Arrow schema to search for rowid column.
//             May be nil (returns -1).
//
// Returns:
//   - Column index (0-based) if found
//   - -1 if no rowid column found or schema is nil
//
// Example:
//
//     idx := catalog.FindRowIDColumn(reader.Schema())
//     if idx == -1 {
//         return errors.New("rowid column required")
//     }
//     rowidArray := batch.Column(idx)
func FindRowIDColumn(schema *arrow.Schema) int
```

### Contract Tests

```go
// TestFindRowIDColumn_ByName
// Given: Schema with column named "rowid"
// When: FindRowIDColumn is called
// Then: Returns index of "rowid" column

// TestFindRowIDColumn_ByMetadata
// Given: Schema with column having is_rowid metadata
// When: FindRowIDColumn is called
// Then: Returns index of that column

// TestFindRowIDColumn_NameTakesPrecedence
// Given: Schema with "rowid" column AND column with is_rowid metadata
// When: FindRowIDColumn is called
// Then: Returns index of "rowid" column (first match wins)

// TestFindRowIDColumn_NotFound
// Given: Schema with no rowid column
// When: FindRowIDColumn is called
// Then: Returns -1

// TestFindRowIDColumn_NilSchema
// Given: nil schema
// When: FindRowIDColumn is called
// Then: Returns -1
```

---

## 4. Handler Contracts

### handleDoExchangeUpdate Changes

**File**: `flight/doexchange_dml.go`

```go
// handleDoExchangeUpdate processes UPDATE operations via DoExchange.
//
// Interface Selection:
//   1. Check if table implements UpdatableBatchTable
//      - If yes: Call Update(ctx, inputReader, opts) directly
//   2. Fall back to UpdatableTable
//      - Extract rowIDs from rowid column
//      - Strip rowid column from batches
//      - Call Update(ctx, rowIDs, strippedReader, opts)
//   3. If neither implemented: Return FailedPrecondition error
//
// Behavior Unchanged:
//   - Transaction handling via withTransaction
//   - RETURNING data streaming
//   - Final metadata with total_changed
```

### handleDoExchangeDelete Changes

**File**: `flight/doexchange_dml.go`

```go
// handleDoExchangeDelete processes DELETE operations via DoExchange.
//
// Interface Selection:
//   1. Check if table implements DeletableBatchTable
//      - If yes: Call Delete(ctx, inputReader, opts) directly
//   2. Fall back to DeletableTable
//      - Extract rowIDs from first column
//      - Call Delete(ctx, rowIDs, opts)
//   3. If neither implemented: Return FailedPrecondition error
//
// Behavior Unchanged:
//   - Transaction handling via withTransaction
//   - RETURNING data streaming
//   - Final metadata with total_changed
```

---

## 5. Error Contracts

### Standard Errors

| Error | Code | Message | When |
|-------|------|---------|------|
| Missing rowid | InvalidArgument | "UPDATE requires rowid column in input schema" | RecordReader lacks rowid |
| Invalid rowid type | InvalidArgument | "rowid column must be Int64, Int32, or Uint64" | Wrong column type |
| Not updatable | FailedPrecondition | "table does not support UPDATE operations" | No update interface |
| Not deletable | FailedPrecondition | "table does not support DELETE operations" | No delete interface |

### Error Types

Implementation errors should use standard Go error patterns:

```go
// Recommended pattern for implementation errors
return nil, fmt.Errorf("update failed: %w", err)

// For typed errors
type UpdateError struct {
    RowID int64
    Cause error
}
func (e *UpdateError) Error() string {
    return fmt.Sprintf("failed to update row %d: %v", e.RowID, e.Cause)
}
func (e *UpdateError) Unwrap() error { return e.Cause }
```

---

## 6. Backward Compatibility Contracts

### Legacy Interface Support

```go
// GUARANTEE: Existing UpdatableTable implementations continue to work.
// The handler will:
//   1. Check UpdatableBatchTable first (preferred)
//   2. Fall back to UpdatableTable if batch not implemented
//   3. Extract rowIDs and strip column as before

// GUARANTEE: Existing DeletableTable implementations continue to work.
// The handler will:
//   1. Check DeletableBatchTable first (preferred)
//   2. Fall back to DeletableTable if batch not implemented
//   3. Extract rowIDs as before
```

### Interface Priority

```go
// Contract: Batch interface is always preferred when both are implemented.
//
// If table implements both UpdatableBatchTable and UpdatableTable:
//   → UpdatableBatchTable.Update() is called
//   → UpdatableTable.Update() is never called
//
// If table implements both DeletableBatchTable and DeletableTable:
//   → DeletableBatchTable.Delete() is called
//   → DeletableTable.Delete() is never called
```

---

## 7. Thread Safety Contracts

```go
// ALL interface implementations MUST be goroutine-safe.
//
// The server may call methods concurrently from different goroutines.
// Implementations must use appropriate synchronization.
//
// Example safe implementation:
type ThreadSafeTable struct {
    mu   sync.RWMutex
    data map[int64]Row
}

func (t *ThreadSafeTable) Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()
    // Safe to modify t.data
}
```
