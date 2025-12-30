# Research: Simplify Batch Table Interface Signatures

**Feature**: 013-batch-table-signature
**Date**: 2025-12-30

## Overview

This research document analyzes the proposed change from `array.RecordReader` to `arrow.Record` in the batch table interfaces.

## Research Topics

### 1. Arrow Record vs RecordReader Semantics

**Decision**: Use `arrow.Record` directly instead of `array.RecordReader`

**Rationale**:
- `array.RecordReader` is an iterator interface with `Next()` method for streaming multiple batches
- `arrow.Record` (alias for `arrow.RecordBatch`) represents a single batch of columnar data
- Current handler implementation (`newUpdateProcessor`, `newDeleteProcessor`) always creates a RecordReader from a single batch:
  ```go
  batchReader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
  ```
- This wrapping adds unnecessary overhead and complexity
- Passing `arrow.Record` directly simplifies both the interface and implementation

**Alternatives Considered**:
1. **Keep RecordReader**: Rejected - adds complexity for no benefit since handlers always process single batches
2. **Use `[]arrow.Record` slice**: Rejected - handlers receive one batch at a time from the stream; slice would require buffering
3. **Use `arrow.RecordBatch` type directly**: `arrow.Record` is the same type (alias) and is more commonly used in Go Arrow APIs

### 2. Memory Management

**Decision**: Caller releases the Record after method returns (same pattern as current RecordReader)

**Rationale**:
- Arrow Go uses reference counting for memory management
- The handler already retains/releases batches correctly
- No change to memory management pattern is needed

**Key Points**:
- Handler calls `record.Retain()` before passing to method
- Table implementation processes the record
- Handler calls `record.Release()` after method returns
- Same pattern used for INSERT operations

### 3. Null RowID Handling

**Decision**: Return error immediately on first null rowid (per clarification session)

**Rationale**:
- Null rowids indicate corrupted or invalid data from DuckDB protocol layer
- Failing fast prevents partial updates/deletes with undefined behavior
- Consistent with strict error handling principle from constitution

**Implementation**:
- Add null check when extracting rowid values from the Record
- Return typed error with clear message

### 4. Backward Compatibility

**Decision**: Legacy interfaces remain unchanged

**Rationale**:
- `UpdatableTable` with `Update(ctx, rowIDs []int64, rows RecordReader)` continues to work
- `DeletableTable` with `Delete(ctx, rowIDs []int64)` continues to work
- Handler strategy pattern (`newUpdateProcessor`, `newDeleteProcessor`) already handles interface selection
- Users can migrate at their own pace

### 5. Handler Simplification

**Current Implementation** (flight/doexchange_dml.go:809-822):
```go
if batchTable, ok := table.(catalog.UpdatableBatchTable); ok {
    return func(ctx context.Context, batch arrow.RecordBatch, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
        batchReader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
        if err != nil {
            return nil, err
        }
        // ... call batchTable.Update(txCtx, batchReader, opts)
    }, true
}
```

**New Implementation**:
```go
if batchTable, ok := table.(catalog.UpdatableBatchTable); ok {
    return func(ctx context.Context, batch arrow.RecordBatch, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
        // ... call batchTable.Update(txCtx, batch, opts) directly
    }, true
}
```

**Benefits**:
- Removes RecordReader construction (3 lines of boilerplate per handler)
- Removes potential error handling for RecordReader creation
- Clearer code intent

## Conclusions

No unknowns or blockers identified. The change is straightforward:

1. **Interface change**: `array.RecordReader` → `arrow.Record` in method signatures
2. **Handler simplification**: Remove RecordReader wrapping
3. **Test updates**: Update table implementations in tests to use new signature
4. **Example updates**: Update DML example to demonstrate new signature
5. **Doc updates**: Update api-guide.md with new signature documentation

All research complete. Proceed to Phase 1: Design & Contracts.
