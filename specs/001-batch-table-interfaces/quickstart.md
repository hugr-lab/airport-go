# Quickstart: Batch Table Interfaces

**Feature Branch**: `001-batch-table-interfaces`
**Date**: 2025-12-29

## Overview

This guide shows how to implement and use the new `UpdatableBatchTable` and `DeletableBatchTable` interfaces for simpler UPDATE and DELETE operations.

---

## Why Use Batch Interfaces?

The batch interfaces simplify DML implementations:

| Aspect | Legacy Interface | Batch Interface |
|--------|-----------------|-----------------|
| Update signature | `Update(ctx, rowIDs, rows, opts)` | `Update(ctx, rows, opts)` |
| Delete signature | `Delete(ctx, rowIDs, opts)` | `Delete(ctx, rows, opts)` |
| Row ID handling | Separate `[]int64` parameter | Embedded in RecordReader |
| Implementation | Must correlate rowIDs[i] with row i | Extract rowID from each record |

---

## Implementing UpdatableBatchTable

### Basic Implementation

```go
package main

import (
    "context"
    "fmt"
    "sync"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"

    "github.com/hugr-lab/airport-go/catalog"
)

// BatchUpdatableTable implements UpdatableBatchTable
type BatchUpdatableTable struct {
    mu     sync.RWMutex
    name   string
    schema *arrow.Schema
    data   map[int64]map[string]any // rowid -> column -> value
}

func (t *BatchUpdatableTable) Name() string    { return t.name }
func (t *BatchUpdatableTable) Comment() string { return "Batch-updatable table" }
func (t *BatchUpdatableTable) ArrowSchema(columns []string) *arrow.Schema {
    return catalog.ProjectSchema(t.schema, columns)
}

func (t *BatchUpdatableTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // ... standard scan implementation
}

// Update implements UpdatableBatchTable
func (t *BatchUpdatableTable) Update(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    // Find the rowid column
    rowidIdx := catalog.FindRowIDColumn(rows.Schema())
    if rowidIdx == -1 {
        return nil, fmt.Errorf("rowid column not found")
    }

    var affectedRows int64
    var returningBatches []arrow.Record

    // Process each batch
    for rows.Next() {
        batch := rows.Record()

        // Get rowid column
        rowidCol := batch.Column(rowidIdx).(*array.Int64)

        // Update each row
        for i := 0; i < int(batch.NumRows()); i++ {
            if rowidCol.IsNull(i) {
                continue
            }
            rowID := rowidCol.Value(i)

            // Get existing row
            row, exists := t.data[rowID]
            if !exists {
                continue // Row doesn't exist, skip
            }

            // Update columns (except rowid)
            for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
                if colIdx == rowidIdx {
                    continue // Skip rowid column
                }
                colName := batch.Schema().Field(colIdx).Name
                row[colName] = getValueAt(batch.Column(colIdx), i)
            }

            affectedRows++
        }

        // Collect RETURNING data if requested
        if opts != nil && opts.Returning {
            returningBatches = append(returningBatches, batch)
            batch.Retain()
        }
    }

    result := &catalog.DMLResult{AffectedRows: affectedRows}

    if opts != nil && opts.Returning && len(returningBatches) > 0 {
        result.ReturningData, _ = array.NewRecordReader(
            t.ArrowSchema(opts.ReturningColumns),
            returningBatches,
        )
    }

    return result, nil
}
```

---

## Implementing DeletableBatchTable

### Basic Implementation

```go
// BatchDeletableTable implements DeletableBatchTable
type BatchDeletableTable struct {
    mu     sync.RWMutex
    name   string
    schema *arrow.Schema
    data   map[int64]map[string]any
}

// ... Name, Comment, ArrowSchema, Scan same as above

// Delete implements DeletableBatchTable
func (t *BatchDeletableTable) Delete(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    // Find the rowid column
    rowidIdx := catalog.FindRowIDColumn(rows.Schema())
    if rowidIdx == -1 {
        return nil, fmt.Errorf("rowid column not found")
    }

    var affectedRows int64
    var deletedRows []map[string]any // For RETURNING

    // Process each batch
    for rows.Next() {
        batch := rows.Record()

        // Get rowid column
        rowidCol := batch.Column(rowidIdx).(*array.Int64)

        // Delete each row
        for i := 0; i < int(batch.NumRows()); i++ {
            if rowidCol.IsNull(i) {
                continue
            }
            rowID := rowidCol.Value(i)

            // Get row before deletion (for RETURNING)
            if row, exists := t.data[rowID]; exists {
                if opts != nil && opts.Returning {
                    deletedRows = append(deletedRows, row)
                }
                delete(t.data, rowID)
                affectedRows++
            }
        }
    }

    result := &catalog.DMLResult{AffectedRows: affectedRows}

    if opts != nil && opts.Returning && len(deletedRows) > 0 {
        result.ReturningData = buildRecordReader(t.schema, deletedRows)
    }

    return result, nil
}
```

---

## Using FindRowIDColumn Helper

```go
import "github.com/hugr-lab/airport-go/catalog"

// Find rowid in schema
rowidIdx := catalog.FindRowIDColumn(reader.Schema())
if rowidIdx == -1 {
    return nil, errors.New("rowid column required for UPDATE/DELETE")
}

// Access rowid values from batch
batch := reader.Record()
rowidCol := batch.Column(rowidIdx)

// Type-safe extraction
switch col := rowidCol.(type) {
case *array.Int64:
    for i := 0; i < col.Len(); i++ {
        rowID := col.Value(i)
        // Process row
    }
case *array.Int32:
    for i := 0; i < col.Len(); i++ {
        rowID := int64(col.Value(i))
        // Process row
    }
}
```

---

## Migration from Legacy Interfaces

### Before (Legacy UpdatableTable)

```go
type LegacyTable struct{}

func (t *LegacyTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    var affected int64
    rowIdx := 0

    for rows.Next() {
        batch := rows.Record()
        for i := 0; i < int(batch.NumRows()); i++ {
            rowID := rowIDs[rowIdx] // Must correlate manually
            rowIdx++
            // Update row with rowID
            affected++
        }
    }

    return &catalog.DMLResult{AffectedRows: affected}, nil
}
```

### After (Batch UpdatableBatchTable)

```go
type ModernTable struct{}

func (t *ModernTable) Update(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    var affected int64
    rowidIdx := catalog.FindRowIDColumn(rows.Schema())

    for rows.Next() {
        batch := rows.Record()
        rowidCol := batch.Column(rowidIdx).(*array.Int64)

        for i := 0; i < int(batch.NumRows()); i++ {
            rowID := rowidCol.Value(i) // Directly from batch
            // Update row with rowID
            affected++
        }
    }

    return &catalog.DMLResult{AffectedRows: affected}, nil
}
```

---

## Complete Example

```go
package main

import (
    "context"
    "log"
    "net"
    "sync"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "google.golang.org/grpc"

    "github.com/hugr-lab/airport-go"
    "github.com/hugr-lab/airport-go/catalog"
)

// UsersTable implements both UpdatableBatchTable and DeletableBatchTable
type UsersTable struct {
    mu     sync.RWMutex
    schema *arrow.Schema
    data   map[int64]*User
}

type User struct {
    ID   int64
    Name string
}

func NewUsersTable() *UsersTable {
    rowidMeta := arrow.NewMetadata([]string{"is_rowid"}, []string{"true"})
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "rowid", Type: arrow.PrimitiveTypes.Int64, Metadata: rowidMeta},
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
    }, nil)

    return &UsersTable{
        schema: schema,
        data: map[int64]*User{
            1: {ID: 1, Name: "Alice"},
            2: {ID: 2, Name: "Bob"},
            3: {ID: 3, Name: "Charlie"},
        },
    }
}

func (t *UsersTable) Name() string    { return "users" }
func (t *UsersTable) Comment() string { return "Users with batch DML support" }
func (t *UsersTable) ArrowSchema(columns []string) *arrow.Schema {
    return catalog.ProjectSchema(t.schema, columns)
}

func (t *UsersTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // ... implementation
}

// Update implements UpdatableBatchTable
func (t *UsersTable) Update(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    rowidIdx := catalog.FindRowIDColumn(rows.Schema())
    if rowidIdx == -1 {
        return nil, fmt.Errorf("rowid column required")
    }

    var affected int64
    for rows.Next() {
        batch := rows.Record()
        rowidCol := batch.Column(rowidIdx).(*array.Int64)

        for i := 0; i < int(batch.NumRows()); i++ {
            rowID := rowidCol.Value(i)
            if user, ok := t.data[rowID]; ok {
                // Update user fields from batch columns
                for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
                    if colIdx == rowidIdx {
                        continue
                    }
                    colName := batch.Schema().Field(colIdx).Name
                    if colName == "name" {
                        user.Name = batch.Column(colIdx).(*array.String).Value(i)
                    }
                }
                affected++
            }
        }
    }

    return &catalog.DMLResult{AffectedRows: affected}, nil
}

// Delete implements DeletableBatchTable
func (t *UsersTable) Delete(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    rowidIdx := catalog.FindRowIDColumn(rows.Schema())
    if rowidIdx == -1 {
        return nil, fmt.Errorf("rowid column required")
    }

    var affected int64
    for rows.Next() {
        batch := rows.Record()
        rowidCol := batch.Column(rowidIdx).(*array.Int64)

        for i := 0; i < int(batch.NumRows()); i++ {
            rowID := rowidCol.Value(i)
            if _, ok := t.data[rowID]; ok {
                delete(t.data, rowID)
                affected++
            }
        }
    }

    return &catalog.DMLResult{AffectedRows: affected}, nil
}

func main() {
    table := NewUsersTable()

    cat, _ := airport.NewCatalogBuilder().
        Schema("demo").
        Table(table).
        Build()

    grpcServer := grpc.NewServer()
    airport.NewServer(grpcServer, airport.ServerConfig{Catalog: cat})

    lis, _ := net.Listen("tcp", ":50051")
    log.Println("Server with batch DML support listening on :50051")
    grpcServer.Serve(lis)
}
```

---

## Testing Your Implementation

```sql
-- Connect from DuckDB
ATTACH '' AS demo (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

-- Test UPDATE
UPDATE demo.demo.users SET name = 'Alicia' WHERE id = 1;

-- Test DELETE
DELETE FROM demo.demo.users WHERE id = 2;

-- Test RETURNING
INSERT INTO demo.demo.users (id, name) VALUES (4, 'Diana') RETURNING *;
UPDATE demo.demo.users SET name = 'Eve' WHERE id = 3 RETURNING name;
DELETE FROM demo.demo.users WHERE id = 4 RETURNING *;
```

---

## Key Points

1. **Use `catalog.FindRowIDColumn()`** to locate the rowid column in the RecordReader schema
2. **Both interfaces accept RecordReader** - consistent with InsertableTable pattern
3. **Batch interface is preferred** when both legacy and batch are implemented
4. **Thread-safety required** - use appropriate locking
5. **Don't release the RecordReader** - the caller (handler) manages its lifecycle
