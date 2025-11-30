# Migration Guide: DML Interface Changes

**Date**: 2025-11-30
**Feature**: 006-returning-optimization
**Impact**: Breaking change to DML interface signatures

## Overview

This release adds a `DMLOptions` parameter to all DML interface methods (`Insert`, `Update`, `Delete`). This enables future extensibility for RETURNING column selection and other DML options.

## What Changed

| Interface | Old Signature | New Signature |
|-----------|--------------|---------------|
| InsertableTable | `Insert(ctx, rows)` | `Insert(ctx, rows, opts)` |
| UpdatableTable | `Update(ctx, rowIDs, rows)` | `Update(ctx, rowIDs, rows, opts)` |
| DeletableTable | `Delete(ctx, rowIDs)` | `Delete(ctx, rowIDs, opts)` |

## Migration Steps

### Step 1: Update Interface Implementations

Add the `opts *catalog.DMLOptions` parameter to your DML method signatures.

**Before:**
```go
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader) (*catalog.DMLResult, error) {
    // ... implementation
}

func (t *MyTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*catalog.DMLResult, error) {
    // ... implementation
}

func (t *MyTable) Delete(ctx context.Context, rowIDs []int64) (*catalog.DMLResult, error) {
    // ... implementation
}
```

**After:**
```go
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // ... implementation (opts can be ignored if not needed)
}

func (t *MyTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // ... implementation (opts can be ignored if not needed)
}

func (t *MyTable) Delete(ctx context.Context, rowIDs []int64, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // ... implementation (opts can be ignored if not needed)
}
```

### Step 2: Handle nil opts

The `opts` parameter may be `nil`. Handle gracefully:

```go
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // Safe to ignore opts if you don't need RETURNING column filtering
    // opts may be nil - that's okay

    // Optional: use ReturningColumns for optimization
    if opts != nil && len(opts.ReturningColumns) > 0 {
        // Optimize: only fetch these columns for RETURNING
        columns := opts.ReturningColumns
        _ = columns // use in your query
    }

    // ... rest of implementation
}
```

### Step 3: Verify Compilation

Run `go build ./...` to ensure all implementations are updated.

## Example: Complete Migration

### Before (v0.1.x)

```go
package main

import (
    "context"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/hugr-lab/airport-go/catalog"
)

type MyTable struct {
    // ...
}

func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader) (*catalog.DMLResult, error) {
    var affected int64
    for rows.Next() {
        batch := rows.RecordBatch()
        affected += batch.NumRows()
        // ... insert batch
    }
    return &catalog.DMLResult{AffectedRows: affected}, nil
}
```

### After (v0.2.x)

```go
package main

import (
    "context"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/hugr-lab/airport-go/catalog"
)

type MyTable struct {
    // ...
}

func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    var affected int64
    for rows.Next() {
        batch := rows.RecordBatch()
        affected += batch.NumRows()
        // ... insert batch
    }
    return &catalog.DMLResult{AffectedRows: affected}, nil
}
```

Note: The only change is adding `opts *catalog.DMLOptions` to the signature. The implementation can remain unchanged.

## Using ReturningColumns (Optional)

If you want to optimize RETURNING data:

```go
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // Determine which columns to include in RETURNING
    var returningCols []string
    if opts != nil && len(opts.ReturningColumns) > 0 {
        returningCols = opts.ReturningColumns
    } else {
        // Default: all columns
        returningCols = t.AllColumnNames()
    }

    // ... insert logic ...

    // Build RETURNING data with only requested columns
    returningData := t.buildReturningData(insertedRows, returningCols)

    return &catalog.DMLResult{
        AffectedRows:  int64(len(insertedRows)),
        ReturningData: returningData,
    }, nil
}
```

## FAQ

### Q: Do I have to use opts.ReturningColumns?

No. You can ignore it completely. DuckDB currently filters columns client-side anyway. This field is for future protocol extensions and server-side optimization.

### Q: What if opts is nil?

That's expected and valid. Treat nil as "use defaults" (return all columns).

### Q: Will there be more fields in DMLOptions later?

Possibly. The struct design allows adding fields without breaking your implementation. New fields will have sensible defaults.

### Q: Is this a breaking change?

Yes. All DML interface implementations must update their method signatures. This is acceptable for a pre-1.0 library (v0.x).

## Checklist

- [ ] Update `Insert` signature to include `opts *catalog.DMLOptions`
- [ ] Update `Update` signature to include `opts *catalog.DMLOptions`
- [ ] Update `Delete` signature to include `opts *catalog.DMLOptions`
- [ ] Run `go build ./...` to verify compilation
- [ ] Run tests to verify functionality
