# Research: Batch Table Interfaces for Update and Delete

**Feature Branch**: `001-batch-table-interfaces`
**Date**: 2025-12-29

## Overview

This document captures research findings for implementing the new `UpdatableBatchTable` and `DeletableBatchTable` interfaces that simplify DML operations by embedding the rowid column in the RecordReader parameter.

---

## 1. Current Interface Analysis

### Current UpdatableTable Interface

```go
type UpdatableTable interface {
    Table
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

**Pain Points:**
- Requires separate `rowIDs []int64` array that must be synchronized with `rows` RecordReader
- Handler extracts rowIDs from input, strips the column, then passes both separately
- Implementation must correlate rowIDs[i] with the i-th record from the RecordReader
- Extra memory allocation for the extracted rowIDs slice

### Current DeletableTable Interface

```go
type DeletableTable interface {
    Table
    Delete(ctx context.Context, rowIDs []int64, opts *DMLOptions) (*DMLResult, error)
}
```

**Pain Points:**
- Receives only `rowIDs []int64`, not a RecordReader
- Inconsistent with UpdatableTable and InsertableTable patterns
- Handler must extract rowIDs from Arrow array and convert to slice
- For RETURNING with DELETE, implementation may need the original row data

---

## 2. Handler Processing Analysis

### handleDoExchangeUpdate Current Flow

1. Create RecordReader from stream
2. Find rowid column index in schema
3. For each batch:
   - Extract rowIDs from batch column → `[]int64`
   - Strip rowid column from batch → new RecordReader
   - Call `UpdatableTable.Update(ctx, rowIDs, strippedReader, opts)`

**Overhead:**
- `extractRowIDs()` allocates new slice per batch
- `stripRowIDColumn()` creates new schema and records without rowid

### handleDoExchangeDelete Current Flow

1. Create RecordReader from stream
2. For each batch:
   - Extract rowIDs from first column → `[]int64`
   - Call `DeletableTable.Delete(ctx, rowIDs, opts)`

**Overhead:**
- `extractRowIDs()` allocates new slice per batch
- Original batch data discarded after extraction

---

## 3. Design Decision: New Interface Signatures

### Decision: UpdatableBatchTable Interface

```go
// UpdatableBatchTable extends Table with batch-oriented UPDATE capability.
// The Update method receives the complete input RecordReader including the rowid column.
// Implementations extract rowid values from the rowid column in the RecordReader.
// This interface is preferred over UpdatableTable when implemented.
type UpdatableBatchTable interface {
    Table
    Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

**Rationale:**
- RecordReader already contains rowid column from client
- Eliminates rowID extraction/stripping overhead in handler
- Implementation has full control over how to process rowids
- Matches InsertableTable pattern (single RecordReader parameter)
- Consistent API surface across all DML operations

**Alternatives Considered:**
- Keep separate rowIDs parameter but as Arrow Array instead of slice
  - Rejected: Still requires type assertion, not cleaner than current
- Pass rowid column separately as Arrow Array
  - Rejected: Adds complexity, doesn't simplify implementation

### Decision: DeletableBatchTable Interface

```go
// DeletableBatchTable extends Table with batch-oriented DELETE capability.
// The Delete method receives a RecordReader containing at least the rowid column.
// Implementations extract rowid values from the RecordReader.
// This interface is preferred over DeletableTable when implemented.
type DeletableBatchTable interface {
    Table
    Delete(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

**Rationale:**
- Consistent with UpdatableBatchTable pattern
- For DELETE with RETURNING, implementation may need access to full row data
- Allows future extension where DELETE input includes additional columns
- Matches batch processing pattern used throughout Arrow Go

**Alternatives Considered:**
- Use single Arrow Array for rowids
  - Rejected: Less flexible, breaks RecordReader consistency
- Keep slice but wrap in struct
  - Rejected: Adds wrapper type without benefit

---

## 4. Design Decision: Interface Detection Strategy

### Decision: Type Assertion Priority

```go
// Check for batch interface first
if batchTable, ok := table.(catalog.UpdatableBatchTable); ok {
    // Use batch interface - pass RecordReader as-is
    return batchTable.Update(ctx, inputReader, opts)
}
// Fall back to legacy interface
if legacyTable, ok := table.(catalog.UpdatableTable); ok {
    // Extract rowIDs, strip column, use legacy interface
    return callLegacyUpdate(ctx, legacyTable, inputReader, opts)
}
```

**Rationale:**
- Batch interface is preferred (simpler, more efficient)
- Tables implementing both get the benefit of the new interface
- Legacy tables continue to work without changes
- Clear upgrade path for existing implementations

**Alternatives Considered:**
- Separate method names (UpdateBatch vs Update)
  - Rejected: Confusing, breaks naming conventions
- Configuration flag to select interface
  - Rejected: Unnecessary complexity

---

## 5. Design Decision: Wrapper Functions for Legacy Support

### Decision: Internal Wrapper in Handler

The wrapper logic will be internal to the handler, not exposed as public API:

```go
// callLegacyUpdate adapts batch input to legacy UpdatableTable interface
func (s *Server) callLegacyUpdate(ctx context.Context, table catalog.UpdatableTable,
    inputReader array.RecordReader, rowidColIdx int, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    // Existing extraction and stripping logic
    rowIDs, err := extractRowIDs(batch.Column(rowidColIdx))
    if err != nil {
        return nil, err
    }
    strippedRecords := stripRowIDColumn(records, rowidColIdx)
    strippedReader, _ := array.NewRecordReader(strippedRecords[0].Schema(), strippedRecords)
    return table.Update(ctx, rowIDs, strippedReader, opts)
}
```

**Rationale:**
- Keeps backward compatibility internal to flight package
- No new public API surface for legacy support
- Handler code remains the single source of truth for protocol handling
- Easy to remove when legacy interfaces are eventually deprecated

**Alternatives Considered:**
- Public adapter function in catalog package
  - Rejected: Exposes internal protocol details, adds public API surface
- Wrapper type implementing UpdatableBatchTable
  - Rejected: Unnecessary indirection, harder to maintain

---

## 6. Design Decision: Rowid Column Identification

### Decision: Existing Detection Logic

Keep the existing rowid detection logic:

```go
// Find rowid column by name or metadata
rowidColIdx := -1
for i := 0; i < schema.NumFields(); i++ {
    field := schema.Field(i)
    if field.Name == "rowid" {
        rowidColIdx = i
        break
    }
    if md := field.Metadata; md.Len() > 0 {
        if idx := md.FindKey("is_rowid"); idx >= 0 && md.Values()[idx] != "" {
            rowidColIdx = i
            break
        }
    }
}
```

**Rationale:**
- Already implemented and tested in current handlers
- Supports both name-based ("rowid") and metadata-based (`is_rowid`) identification
- Matches DuckDB Airport extension behavior
- No changes needed to protocol or client expectations

---

## 7. Design Decision: Helper Function Location

### Decision: Add to catalog package

```go
// catalog/helpers.go
package catalog

// FindRowIDColumn returns the index of the rowid column in the schema.
// Returns -1 if no rowid column is found.
// Rowid column is identified by:
//   - Column name "rowid", or
//   - Metadata key "is_rowid" with non-empty value
func FindRowIDColumn(schema *arrow.Schema) int {
    for i := 0; i < schema.NumFields(); i++ {
        field := schema.Field(i)
        if field.Name == "rowid" {
            return i
        }
        if md := field.Metadata; md.Len() > 0 {
            if idx := md.FindKey("is_rowid"); idx >= 0 && md.Values()[idx] != "" {
                return i
            }
        }
    }
    return -1
}
```

**Rationale:**
- Public helper useful for batch interface implementers
- Centralizes rowid detection logic
- Can be used by both handler and table implementations
- Documented behavior for API users

---

## 8. Testing Strategy

### Unit Tests

- `TestFindRowIDColumn` - column detection by name and metadata
- `TestUpdatableBatchTable_Update` - mock table with batch interface
- `TestDeletableBatchTable_Delete` - mock table with batch interface

### Integration Tests

- `TestBatchUpdate_ViaLegacyInterface` - legacy table still works
- `TestBatchUpdate_ViaBatchInterface` - new interface works
- `TestBatchUpdate_InterfacePreference` - batch preferred when both implemented
- `TestBatchDelete_*` - parallel tests for DELETE operations
- `TestBatchUpdate_WithReturning` - RETURNING clause works with batch interface

---

## 9. Documentation Updates

### Files to Update

1. `docs/api-guide.md` - Add batch interface documentation
2. `protocol_implementation_golang.qmd` - Update DML section
3. `examples/dml/main.go` - Add batch interface example
4. `README.md` - Mention new interfaces in features

### Migration Guide Content

```markdown
## Migrating to Batch Interfaces

The new `UpdatableBatchTable` and `DeletableBatchTable` interfaces simplify
UPDATE and DELETE implementations:

**Before (legacy):**
```go
func (t *MyTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error) {
    // Correlate rowIDs[i] with i-th record
}
```

**After (batch):**
```go
func (t *MyTable) Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error) {
    // Extract rowid from each record directly
    rowidIdx := catalog.FindRowIDColumn(rows.Schema())
    // Process records with rowid in-place
}
```
```

---

## 10. Summary of Decisions

| Topic | Decision | Key Rationale |
|-------|----------|---------------|
| UpdatableBatchTable signature | `Update(ctx, rows, opts)` | Matches InsertableTable; RecordReader includes rowid |
| DeletableBatchTable signature | `Delete(ctx, rows, opts)` | Consistent with batch pattern; enables RETURNING access |
| Interface detection | Type assertion, batch first | Clear preference, backward compatible |
| Wrapper location | Internal to flight handler | No new public API for legacy support |
| Rowid detection | Existing name/metadata logic | Already proven, matches protocol |
| Helper function | `catalog.FindRowIDColumn()` | Public utility for implementers |

---

## 11. Risk Assessment

| Risk | Mitigation |
|------|------------|
| Breaking existing implementations | Legacy interfaces remain, tested for compatibility |
| Performance regression | Batch interface avoids extraction overhead |
| Confusion about which interface to use | Clear documentation with migration guide |
| Memory management with RecordReader | Document Release() requirements in godoc |
