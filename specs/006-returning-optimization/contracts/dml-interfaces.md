# DML Interface Contracts

**Date**: 2025-11-30
**Feature**: 006-returning-optimization

## Interface Changes Summary

| Interface | Method | Change |
|-----------|--------|--------|
| InsertableTable | Insert | Add `opts *DMLOptions` parameter |
| UpdatableTable | Update | Add `opts *DMLOptions` parameter |
| DeletableTable | Delete | Add `opts *DMLOptions` parameter |

## New Type: DMLOptions

**Package**: `catalog`
**File**: `catalog/types.go`

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
    // The server populates ReturningColumns from the input schema column names.
    ReturningColumns []string
}
```

## Modified Interface: InsertableTable

**Package**: `catalog`
**File**: `catalog/table.go`

### Before

```go
type InsertableTable interface {
    Table
    Insert(ctx context.Context, rows array.RecordReader) (*DMLResult, error)
}
```

### After

```go
type InsertableTable interface {
    Table
    Insert(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### Contract

- **Preconditions**:
  - `ctx` must be non-nil
  - `rows` must be a valid RecordReader (may be empty)
  - `opts` may be nil (treat as empty options)

- **Postconditions**:
  - Returns `DMLResult` with `AffectedRows` >= 0
  - If `return-chunks=1`, `ReturningData` contains affected rows
  - If `opts.ReturningColumns` is non-empty, implementation MAY filter columns

- **Error Conditions**:
  - Context cancelled → return ctx.Err()
  - Schema mismatch → return error with details
  - Storage failure → return error

## Modified Interface: UpdatableTable

**Package**: `catalog`
**File**: `catalog/table.go`

### Before

```go
type UpdatableTable interface {
    Table
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*DMLResult, error)
}
```

### After

```go
type UpdatableTable interface {
    Table
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### Contract

- **Preconditions**:
  - `ctx` must be non-nil
  - `rowIDs` contains valid row identifiers (may be empty)
  - `rows` schema matches columns being updated
  - `opts` may be nil

- **Postconditions**:
  - Returns `DMLResult` with `AffectedRows` = len(updated rows)
  - If `return-chunks=1`, `ReturningData` contains updated row values
  - Rows not found are silently skipped (no error)

- **Error Conditions**:
  - Context cancelled → return ctx.Err()
  - Invalid rowID type → return error
  - Storage failure → return error

## Modified Interface: DeletableTable

**Package**: `catalog`
**File**: `catalog/table.go`

### Before

```go
type DeletableTable interface {
    Table
    Delete(ctx context.Context, rowIDs []int64) (*DMLResult, error)
}
```

### After

```go
type DeletableTable interface {
    Table
    Delete(ctx context.Context, rowIDs []int64, opts *DMLOptions) (*DMLResult, error)
}
```

### Contract

- **Preconditions**:
  - `ctx` must be non-nil
  - `rowIDs` contains valid row identifiers (may be empty)
  - `opts` may be nil

- **Postconditions**:
  - Returns `DMLResult` with `AffectedRows` = len(deleted rows)
  - If `return-chunks=1`, `ReturningData` contains deleted row values
  - Rows not found are silently skipped (no error)

- **Error Conditions**:
  - Context cancelled → return ctx.Err()
  - Invalid rowID → return error
  - Storage failure → return error

## Implementation Requirements

### Server (flight/doexchange_dml.go)

1. Create `DMLOptions` with empty `ReturningColumns` (protocol doesn't provide)
2. Pass `opts` to table method calls
3. Handle `DMLResult.ReturningData` unchanged

```go
// Example for INSERT handler
opts := &catalog.DMLOptions{
    Returning:        returnData, // from return-chunks header
    ReturningColumns: nil,        // populated from input schema if returning
}

// Populate column names from input schema when RETURNING is requested
if returnData {
    for i := 0; i < inputSchema.NumFields(); i++ {
        opts.ReturningColumns = append(opts.ReturningColumns, inputSchema.Field(i).Name)
    }
}

result, err := insertableTable.Insert(ctx, rows, opts)
```

### Table Implementations

1. Accept `opts *DMLOptions` parameter
2. Handle nil opts (treat as empty)
3. Optionally use `ReturningColumns` for optimization

```go
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*catalog.DMLResult, error) {
    // opts may be nil - handle gracefully
    result := &catalog.DMLResult{}

    // ... insert logic ...
    result.AffectedRows = affected

    // Only populate ReturningData if RETURNING clause was specified
    if opts != nil && opts.Returning {
        var returningCols []string
        if len(opts.ReturningColumns) > 0 {
            returningCols = opts.ReturningColumns
            // Optional: optimize query to fetch only these columns
        } else {
            // RETURNING * - return all columns
            returningCols = t.AllColumnNames()
        }
        result.ReturningData = t.buildReturningReader(insertedRows, returningCols)
    }

    return result, nil
}
```

## Files Requiring Updates

| File | Type | Changes |
|------|------|---------|
| `catalog/types.go` | New type | Add `DMLOptions` struct |
| `catalog/table.go` | Interface | Update 3 interface signatures |
| `flight/doexchange_dml.go` | Handler | Create opts, pass to methods |
| `examples/dml/main.go` | Example | Update method signatures |
| `tests/integration/dml_test.go` | Test | Update method signatures |

## Breaking Change Migration

This is a breaking change. All implementations must update:

```go
// Before
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader) (*catalog.DMLResult, error)

// After
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error)
```

Implementations that don't use RETURNING columns can ignore the `opts` parameter.
