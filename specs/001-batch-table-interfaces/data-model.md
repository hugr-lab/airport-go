# Data Model: Batch Table Interfaces

**Feature Branch**: `001-batch-table-interfaces`
**Date**: 2025-12-29

## Overview

This document defines the data model for the new batch table interfaces, including interface definitions, type relationships, and state transitions.

---

## 1. Interface Hierarchy

```
catalog.Table (base)
├── catalog.InsertableTable
│   └── Insert(ctx, rows, opts) → existing
├── catalog.UpdatableTable
│   └── Update(ctx, rowIDs, rows, opts) → legacy
├── catalog.UpdatableBatchTable     ← NEW
│   └── Update(ctx, rows, opts)
├── catalog.DeletableTable
│   └── Delete(ctx, rowIDs, opts) → legacy
└── catalog.DeletableBatchTable     ← NEW
    └── Delete(ctx, rows, opts)
```

---

## 2. New Interface Definitions

### UpdatableBatchTable

```go
// UpdatableBatchTable extends Table with batch-oriented UPDATE capability.
// The Update method receives the complete input RecordReader including the rowid column.
// Implementations extract rowid values from the rowid column in the RecordReader.
// This interface is preferred over UpdatableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type UpdatableBatchTable interface {
    Table

    // Update modifies existing rows using data from the RecordReader.
    // The rows RecordReader contains both the rowid column (identifying rows to update)
    // and the new column values. Implementations MUST extract rowid values from
    // the rowid column (identified by name "rowid" or metadata key "is_rowid").
    // Row order in RecordReader determines update order.
    // The opts parameter provides options including RETURNING clause information:
    //   - opts.Returning: true if RETURNING clause was specified
    //   - opts.ReturningColumns: column names to include in RETURNING results
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    // Caller MUST call rows.Release() after Update returns.
    Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### DeletableBatchTable

```go
// DeletableBatchTable extends Table with batch-oriented DELETE capability.
// The Delete method receives a RecordReader containing the rowid column.
// Implementations extract rowid values from the rowid column in the RecordReader.
// This interface is preferred over DeletableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type DeletableBatchTable interface {
    Table

    // Delete removes rows identified by rowid values in the RecordReader.
    // The rows RecordReader contains the rowid column (identified by name "rowid"
    // or metadata key "is_rowid") that identifies rows to delete.
    // The opts parameter provides options including RETURNING clause information:
    //   - opts.Returning: true if RETURNING clause was specified
    //   - opts.ReturningColumns: column names to include in RETURNING results
    // Returns DMLResult with affected row count and optional returning data.
    // Context may contain transaction ID for coordinated operations.
    // Caller MUST call rows.Release() after Delete returns.
    Delete(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

---

## 3. Helper Function

### FindRowIDColumn

```go
// FindRowIDColumn returns the index of the rowid column in the schema.
// Returns -1 if no rowid column is found.
// Rowid column is identified by:
//   - Column name "rowid", or
//   - Metadata key "is_rowid" with non-empty value
func FindRowIDColumn(schema *arrow.Schema) int
```

**Location**: `catalog/helpers.go`

---

## 4. Existing Types (Unchanged)

### DMLOptions

```go
type DMLOptions struct {
    // Returning indicates if RETURNING clause was specified.
    Returning bool

    // ReturningColumns lists column names to include in RETURNING results.
    ReturningColumns []string
}
```

### DMLResult

```go
type DMLResult struct {
    // AffectedRows is the count of rows modified.
    AffectedRows int64

    // ReturningData contains RETURNING clause data if requested.
    ReturningData array.RecordReader
}
```

---

## 5. Schema Structures

### Update Input Schema (from DuckDB)

The UPDATE operation receives records with this structure:

| Column | Type | Description |
|--------|------|-------------|
| col1, col2, ... | various | Updated column values |
| rowid | INT64 | Row identifier (with `is_rowid` metadata or named "rowid") |

**Note**: Column order may vary; rowid can be at any position.

### Delete Input Schema (from DuckDB)

The DELETE operation receives records with this structure:

| Column | Type | Description |
|--------|------|-------------|
| rowid | INT64 | Row identifier |

**Note**: May include additional columns for complex deletes.

### Rowid Column Identification

A column is identified as the rowid column if:
1. Column name equals "rowid" (case-sensitive), OR
2. Column has metadata key "is_rowid" with non-empty value

Metadata example:
```go
arrow.NewMetadata([]string{"is_rowid"}, []string{"true"})
```

---

## 6. Interface Detection Flow

```
┌─────────────────────────────────────┐
│ Table received from Schema.Table() │
└─────────────────────────────────────┘
                  │
                  ▼
    ┌─────────────────────────────┐
    │ Is UpdatableBatchTable?     │
    │ (type assertion)            │
    └─────────────────────────────┘
           │yes            │no
           ▼               ▼
┌──────────────────┐ ┌─────────────────────────┐
│ Call batch       │ │ Is UpdatableTable?      │
│ Update(ctx,rows) │ │ (type assertion)        │
└──────────────────┘ └─────────────────────────┘
                            │yes           │no
                            ▼              ▼
               ┌───────────────────┐ ┌──────────────┐
               │ Extract rowIDs    │ │ Return error │
               │ Strip column      │ │ "not update- │
               │ Call legacy       │ │  able"       │
               │ Update(rowIDs,..) │ └──────────────┘
               └───────────────────┘
```

---

## 7. State Transitions

### Handler Processing State

```
┌──────────────────────────────────────────────────────────────────┐
│ State: Receiving Input                                           │
│ - Stream connected, reading RecordReader                         │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ State: Interface Detection                                       │
│ - Check UpdatableBatchTable or DeletableBatchTable first         │
│ - Fall back to legacy interface if batch not implemented         │
└──────────────────────────────────────────────────────────────────┘
                              │
           ┌──────────────────┼──────────────────┐
           │ Batch Interface  │ Legacy Interface │
           ▼                  ▼                  ▼
┌──────────────────┐ ┌───────────────────────────────┐
│ State: Direct    │ │ State: Extraction             │
│ Processing       │ │ - Extract rowIDs from column  │
│ - Pass full      │ │ - Strip rowid from records    │
│   RecordReader   │ │ - Create stripped reader      │
└──────────────────┘ └───────────────────────────────┘
           │                  │
           └────────┬─────────┘
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│ State: Executing DML                                             │
│ - Call interface method                                          │
│ - Within transaction if TransactionManager configured            │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ State: Processing RETURNING                                      │
│ - If RETURNING requested, stream result batches                  │
│ - Transform schema if needed                                     │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ State: Complete                                                  │
│ - Send final metadata with total_changed count                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 8. Validation Rules

### UpdatableBatchTable.Update

| Rule | Description | Error |
|------|-------------|-------|
| VR-001 | RecordReader must contain rowid column | "UPDATE requires rowid column" |
| VR-002 | Rowid values must be Int64, Int32, or Uint64 | "rowid must be integer type" |
| VR-003 | Context must not be nil | panic (Go convention) |
| VR-004 | opts may be nil (use defaults) | N/A |

### DeletableBatchTable.Delete

| Rule | Description | Error |
|------|-------------|-------|
| VR-001 | RecordReader must contain rowid column | "DELETE requires rowid column" |
| VR-002 | Rowid values must be Int64, Int32, or Uint64 | "rowid must be integer type" |
| VR-003 | Context must not be nil | panic (Go convention) |
| VR-004 | opts may be nil (use defaults) | N/A |

---

## 9. Memory Management

### RecordReader Lifecycle

```go
// Caller (handler) responsibility:
inputReader, _ := flight.NewRecordReader(stream)
defer inputReader.Release()  // Always release when done

// Implementation responsibility:
func (t *MyTable) Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error) {
    // Do NOT release rows - caller will do it
    for rows.Next() {
        batch := rows.RecordBatch()
        // Process batch
        // Do NOT release batch - RecordReader manages it
    }
    return result, nil
}
```

### RETURNING Data Lifecycle

```go
// Implementation provides ReturningData:
result := &DMLResult{
    AffectedRows:  n,
    ReturningData: myReader,  // Implementation creates this
}
return result, nil

// Handler consumes and releases ReturningData:
for result.ReturningData.Next() {
    batch := result.ReturningData.RecordBatch()
    // Send to client
    // Handler manages batch lifetime
}
// Handler does NOT release ReturningData - it's consumed
```

---

## 10. Compatibility Matrix

| Table Implementation | UPDATE Result | DELETE Result |
|---------------------|---------------|---------------|
| Only UpdatableBatchTable | ✅ Batch path | ❌ Not deletable |
| Only UpdatableTable | ✅ Legacy path | ❌ Not deletable |
| Both interfaces | ✅ Batch path (preferred) | ❌ Not deletable |
| Only DeletableBatchTable | ❌ Not updatable | ✅ Batch path |
| Only DeletableTable | ❌ Not updatable | ✅ Legacy path |
| Both interfaces | ❌ Not updatable | ✅ Batch path (preferred) |
| All four interfaces | ✅ Batch paths | ✅ Batch paths |
