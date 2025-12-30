# Quickstart: Batch Table Interface Migration

**Feature**: 013-batch-table-signature
**Date**: 2025-12-30

## Overview

This guide shows how to migrate from the old `array.RecordReader` signatures to the new `arrow.Record` signatures for `UpdatableBatchTable` and `DeletableBatchTable`.

## Before (Old Signature)

```go
// UpdatableBatchTable with RecordReader
func (t *MyTable) Update(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // Had to call rows.Next() and handle iterator semantics
    for rows.Next() {
        batch := rows.RecordBatch()
        // Process batch...
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return &catalog.DMLResult{AffectedRows: count}, nil
}

// DeletableBatchTable with RecordReader
func (t *MyTable) Delete(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    for rows.Next() {
        batch := rows.RecordBatch()
        // Process batch...
    }
    // ...
}
```

## After (New Signature)

```go
// UpdatableBatchTable with Record - simpler!
func (t *MyTable) Update(ctx context.Context, rows arrow.Record, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // Direct access to the record - no iterator needed
    schema := rows.Schema()

    // Find rowid column
    rowidColIdx := catalog.FindRowIDColumn(schema)
    if rowidColIdx == -1 {
        return nil, errors.New("rowid column not found")
    }

    // Check for null rowids (required by spec)
    rowidCol := rows.Column(rowidColIdx)
    if rowidCol.NullN() > 0 {
        return nil, catalog.ErrNullRowID
    }

    // Process rows directly
    for i := 0; i < int(rows.NumRows()); i++ {
        rowid := rows.Column(rowidColIdx).(*array.Int64).Value(i)
        // Update row with rowid...
    }

    return &catalog.DMLResult{AffectedRows: rows.NumRows()}, nil
}

// DeletableBatchTable with Record - simpler!
func (t *MyTable) Delete(ctx context.Context, rows arrow.Record, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // Find rowid column
    rowidColIdx := catalog.FindRowIDColumn(rows.Schema())
    if rowidColIdx == -1 {
        rowidColIdx = 0 // Default to first column for DELETE
    }

    // Check for null rowids
    rowidCol := rows.Column(rowidColIdx)
    if rowidCol.NullN() > 0 {
        return nil, catalog.ErrNullRowID
    }

    // Collect rowids to delete
    rowidArr := rowidCol.(*array.Int64)
    for i := 0; i < int(rows.NumRows()); i++ {
        rowid := rowidArr.Value(i)
        // Delete row with rowid...
    }

    return &catalog.DMLResult{AffectedRows: rows.NumRows()}, nil
}
```

## Key Differences

| Aspect | Old (RecordReader) | New (Record) |
|--------|-------------------|--------------|
| Data access | Iterator (`Next()`) | Direct access |
| Batch handling | Loop over batches | Single batch |
| Schema access | `rows.Schema()` | `rows.Schema()` (same) |
| Column access | `batch.Column(i)` | `rows.Column(i)` |
| Row count | Per-batch iteration | `rows.NumRows()` |
| Error checking | `rows.Err()` at end | Not needed |

## Migration Steps

1. **Update method signature**: Change `rows array.RecordReader` to `rows arrow.Record`

2. **Remove iterator loop**: Replace `for rows.Next() { batch := rows.RecordBatch(); ... }` with direct access to `rows`

3. **Remove error check**: Remove `rows.Err()` check (not needed for Record)

4. **Add null rowid check**: Add check for null rowids and return `catalog.ErrNullRowID`

5. **Update imports**: Ensure `github.com/apache/arrow-go/v18/arrow` is imported

## Memory Management

Memory management remains the same:
- Caller (handler) retains the Record before passing
- Your implementation processes the Record
- Caller releases the Record after your method returns

You do NOT need to call `rows.Release()` in your implementation.

## Testing

Your integration tests should continue to work. If you have unit tests that mock the interface, update them to pass `arrow.Record` instead of `array.RecordReader`.

```go
// Before
reader, _ := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
result, err := table.Update(ctx, reader, opts)

// After
result, err := table.Update(ctx, batch, opts)
```
