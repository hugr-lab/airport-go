package airport_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	airport "github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// =============================================================================
// Batch Interface Integration Tests
// =============================================================================
// These tests verify that tables implementing UpdatableBatchTable and
// DeletableBatchTable work correctly with DuckDB SQL operations.
// The batch interfaces receive rowid embedded in the RecordReader rather
// than as a separate []int64 slice.

// TestDMLBatchUpdate tests UPDATE operations using UpdatableBatchTable interface.
func TestDMLBatchUpdate(t *testing.T) {
	table := newBatchDMLTable(dmlSchemaWithRowID())
	cat := batchDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("UpdateSingleRow", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
		})

		// Update via DuckDB SQL - uses UpdatableBatchTable.Update
		_, err := db.Exec(fmt.Sprintf(
			"UPDATE %s.batch_schema.users SET name = 'Alice Updated' WHERE id = 1",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}

		// Verify the batch interface was used
		if !table.WasBatchUpdateUsed() {
			t.Error("expected batch Update interface to be used")
		}

		// Verify via SELECT
		var name string
		err = db.QueryRow(fmt.Sprintf("SELECT name FROM %s.batch_schema.users WHERE id = 1", attachName)).Scan(&name)
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}

		if name != "Alice Updated" {
			t.Errorf("expected 'Alice Updated', got '%s'", name)
		}
	})

	t.Run("UpdateMultipleRows", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// Update multiple rows
		_, err := db.Exec(fmt.Sprintf(
			"UPDATE %s.batch_schema.users SET email = 'updated@example.com' WHERE id IN (1, 2)",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}

		// Verify the batch interface was used
		if !table.WasBatchUpdateUsed() {
			t.Error("expected batch Update interface to be used")
		}

		// Verify updates
		var count int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.batch_schema.users WHERE email = 'updated@example.com'",
			attachName,
		)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 2 {
			t.Errorf("expected 2 updated rows, got %d", count)
		}
	})

	t.Run("UpdateWithReturning", func(t *testing.T) {
		table.Clear()
		table.EnableReturning()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
		})

		// UPDATE with RETURNING
		rows, err := db.Query(fmt.Sprintf(
			"UPDATE %s.batch_schema.users SET name = 'Updated' WHERE id IN (1, 2) RETURNING id",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE RETURNING failed: %v", err)
		}

		var returnedCount int
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedCount++
		}
		rows.Close()

		if returnedCount != 2 {
			t.Errorf("expected 2 returned rows, got %d", returnedCount)
		}

		// Verify the batch interface was used
		if !table.WasBatchUpdateUsed() {
			t.Error("expected batch Update interface to be used")
		}
	})
}

// TestDMLBatchDelete tests DELETE operations using DeletableBatchTable interface.
func TestDMLBatchDelete(t *testing.T) {
	table := newBatchDMLTable(dmlSchemaWithRowID())
	cat := batchDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("DeleteSingleRow", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// Delete via DuckDB SQL - uses DeletableBatchTable.Delete
		_, err := db.Exec(fmt.Sprintf(
			"DELETE FROM %s.batch_schema.users WHERE id = 2",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify the batch interface was used
		if !table.WasBatchDeleteUsed() {
			t.Error("expected batch Delete interface to be used")
		}

		// Verify count
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.batch_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 2 {
			t.Errorf("expected 2 rows remaining, got %d", count)
		}
	})

	t.Run("DeleteMultipleRows", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// Delete multiple rows
		_, err := db.Exec(fmt.Sprintf(
			"DELETE FROM %s.batch_schema.users WHERE id IN (1, 3)",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify the batch interface was used
		if !table.WasBatchDeleteUsed() {
			t.Error("expected batch Delete interface to be used")
		}

		// Verify only Bob remains
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.batch_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 1 {
			t.Errorf("expected 1 row remaining, got %d", count)
		}
	})

	t.Run("DeleteWithReturning", func(t *testing.T) {
		table.Clear()
		table.EnableReturning()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// DELETE with RETURNING
		rows, err := db.Query(fmt.Sprintf(
			"DELETE FROM %s.batch_schema.users WHERE id IN (1, 2) RETURNING id",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE RETURNING failed: %v", err)
		}

		var returnedCount int
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedCount++
		}
		rows.Close()

		if returnedCount != 2 {
			t.Errorf("expected 2 returned rows, got %d", returnedCount)
		}

		// Verify the batch interface was used
		if !table.WasBatchDeleteUsed() {
			t.Error("expected batch Delete interface to be used")
		}
	})
}

// TestDMLBatchFullCRUD tests a complete CRUD cycle using batch interfaces.
func TestDMLBatchFullCRUD(t *testing.T) {
	table := newBatchDMLTable(dmlSchemaWithRowID())
	cat := batchDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// 1. INSERT - Create initial data (uses InsertableTable, not batch)
	_, err := db.Exec(fmt.Sprintf(
		"INSERT INTO %s.batch_schema.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
		attachName,
	))
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// 2. Verify INSERT with SELECT
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.batch_schema.users", attachName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT failed: %v", err)
	}
	if count != 2 {
		t.Errorf("after INSERT: expected 2 rows, got %d", count)
	}

	// 3. UPDATE - Modify data (uses UpdatableBatchTable)
	_, err = db.Exec(fmt.Sprintf(
		"UPDATE %s.batch_schema.users SET name = 'Alice Updated', email = 'alice.new@example.com' WHERE id = 1",
		attachName,
	))
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify batch interface was used
	if !table.WasBatchUpdateUsed() {
		t.Error("expected batch Update interface to be used")
	}

	// 4. Verify UPDATE with SELECT
	var name, email string
	err = db.QueryRow(fmt.Sprintf("SELECT name, email FROM %s.batch_schema.users WHERE id = 1", attachName)).Scan(&name, &email)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "Alice Updated" {
		t.Errorf("after UPDATE: expected name='Alice Updated', got '%s'", name)
	}
	if email != "alice.new@example.com" {
		t.Errorf("after UPDATE: expected email='alice.new@example.com', got '%s'", email)
	}

	// 5. DELETE - Remove data (uses DeletableBatchTable)
	_, err = db.Exec(fmt.Sprintf(
		"DELETE FROM %s.batch_schema.users WHERE id = 2",
		attachName,
	))
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Verify batch interface was used
	if !table.WasBatchDeleteUsed() {
		t.Error("expected batch Delete interface to be used")
	}

	// 6. Verify DELETE with SELECT
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.batch_schema.users", attachName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT failed: %v", err)
	}
	if count != 1 {
		t.Errorf("after DELETE: expected 1 row, got %d", count)
	}
}

// =============================================================================
// Batch Interface Test Infrastructure
// =============================================================================

// batchDMLTable implements UpdatableBatchTable and DeletableBatchTable interfaces.
// This tests the new batch interfaces where rowid is embedded in RecordReader.
type batchDMLTable struct {
	tableName       string
	schema          *arrow.Schema
	alloc           memory.Allocator
	mu              sync.RWMutex
	data            [][]any // Each row: [rowid, id, name, email]
	nextRowID       int64
	enableReturning bool

	// Track which interface was used
	batchUpdateUsed bool
	batchDeleteUsed bool
}

func newBatchDMLTable(schema *arrow.Schema) *batchDMLTable {
	return &batchDMLTable{
		tableName: "users",
		schema:    schema,
		alloc:     memory.DefaultAllocator,
		data:      make([][]any, 0),
		nextRowID: 1,
	}
}

func (t *batchDMLTable) EnableReturning() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enableReturning = true
}

func (t *batchDMLTable) WasBatchUpdateUsed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.batchUpdateUsed
}

func (t *batchDMLTable) WasBatchDeleteUsed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.batchDeleteUsed
}

func (t *batchDMLTable) Name() string    { return t.tableName }
func (t *batchDMLTable) Comment() string { return "Batch interface DML table" }
func (t *batchDMLTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *batchDMLTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.data) == 0 {
		return array.NewRecordReader(t.schema, nil)
	}

	record := buildTestRecord(t.schema, t.convertData())
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

func (t *batchDMLTable) convertData() [][]interface{} {
	result := make([][]interface{}, len(t.data))
	for i, row := range t.data {
		result[i] = make([]interface{}, len(row))
		copy(result[i], row)
	}
	return result
}

// Insert implements catalog.InsertableTable (same as legacy)
func (t *batchDMLTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	start := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	inputSchema := rows.Schema()
	var insertedRows [][]any
	var totalRows int64

	for rows.Next() {
		batch := rows.RecordBatch()
		for rowIdx := int64(0); rowIdx < batch.NumRows(); rowIdx++ {
			row := make([]any, batch.NumCols()+1)
			row[0] = t.nextRowID

			for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
				col := batch.Column(colIdx)
				val := extractValue(col, int(rowIdx))
				if val == nil && inputSchema.Field(colIdx).Name == "id" {
					row[colIdx+1] = t.nextRowID
				} else {
					row[colIdx+1] = val
				}
			}
			t.data = append(t.data, row)

			if t.enableReturning {
				insertedRows = append(insertedRows, row)
			}

			t.nextRowID++
			totalRows++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	elapsed := time.Since(start)
	fmt.Printf("[DEBUG] INSERT (batch table) completed: %d rows, duration=%v\n", totalRows, elapsed)

	result := &catalog.DMLResult{AffectedRows: totalRows}

	if t.enableReturning && opts != nil && opts.Returning && len(insertedRows) > 0 {
		returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
		returningReader, err := t.buildReturningReader(returningSchema, insertedRows)
		if err != nil {
			return nil, fmt.Errorf("failed to build RETURNING data: %w", err)
		}
		result.ReturningData = returningReader
	}

	return result, nil
}

// Update implements catalog.UpdatableBatchTable - rowid is embedded in RecordReader
func (t *batchDMLTable) Update(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	start := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	t.batchUpdateUsed = true

	inputSchema := rows.Schema()

	// Find rowid column using catalog helper
	rowidColIdx := catalog.FindRowIDColumn(inputSchema)
	if rowidColIdx == -1 {
		return nil, fmt.Errorf("rowid column not found in input schema")
	}

	// Build mapping from input column index to table column index (skip rowid)
	colMapping := make(map[int]int) // input col idx -> table col idx
	for i := 0; i < inputSchema.NumFields(); i++ {
		if i == rowidColIdx {
			continue // Skip rowid column
		}
		inputColName := inputSchema.Field(i).Name
		for j := 1; j < t.schema.NumFields(); j++ {
			if t.schema.Field(j).Name == inputColName {
				colMapping[i] = j
				break
			}
		}
	}

	// Process updates
	type colUpdate struct {
		colIdx int
		value  any
	}
	updates := make(map[int64][]colUpdate)

	for rows.Next() {
		batch := rows.RecordBatch()
		rowidCol := batch.Column(rowidColIdx).(*array.Int64)

		for batchRowIdx := 0; batchRowIdx < int(batch.NumRows()); batchRowIdx++ {
			rowID := rowidCol.Value(batchRowIdx)

			var colUpdates []colUpdate
			for inputColIdx, tableColIdx := range colMapping {
				col := batch.Column(inputColIdx)
				colUpdates = append(colUpdates, colUpdate{
					colIdx: tableColIdx,
					value:  extractValue(col, batchRowIdx),
				})
			}
			updates[rowID] = colUpdates
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Apply updates
	affected := int64(0)
	var affectedRows [][]any
	for i := range t.data {
		rowID := t.data[i][0].(int64)
		if colUpdates, ok := updates[rowID]; ok {
			for _, cu := range colUpdates {
				t.data[i][cu.colIdx] = cu.value
			}
			if t.enableReturning && opts != nil && opts.Returning {
				rowCopy := make([]any, len(t.data[i]))
				copy(rowCopy, t.data[i])
				affectedRows = append(affectedRows, rowCopy)
			}
			affected++
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("[DEBUG] UPDATE (batch interface) completed: %d rows affected, duration=%v\n", affected, elapsed)

	result := &catalog.DMLResult{AffectedRows: affected}

	if t.enableReturning && opts != nil && opts.Returning && len(affectedRows) > 0 {
		returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
		returningReader, err := t.buildReturningReader(returningSchema, affectedRows)
		if err != nil {
			return nil, fmt.Errorf("failed to build RETURNING data: %w", err)
		}
		result.ReturningData = returningReader
	}

	return result, nil
}

// Delete implements catalog.DeletableBatchTable - rowid is embedded in RecordReader
func (t *batchDMLTable) Delete(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	start := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	t.batchDeleteUsed = true

	// Collect rowIDs to delete from the RecordReader
	deleteSet := make(map[int64]bool)

	for rows.Next() {
		batch := rows.RecordBatch()
		// Find rowid column - should be the first (and only) column for DELETE
		rowidColIdx := catalog.FindRowIDColumn(batch.Schema())
		if rowidColIdx == -1 {
			// Default to first column if not found by name/metadata
			rowidColIdx = 0
		}

		rowidCol := batch.Column(rowidColIdx).(*array.Int64)
		for i := 0; i < int(batch.NumRows()); i++ {
			deleteSet[rowidCol.Value(i)] = true
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Filter out deleted rows
	newData := make([][]any, 0, len(t.data))
	var deletedRows [][]any
	deleted := int64(0)
	for _, row := range t.data {
		rowID := row[0].(int64)
		if !deleteSet[rowID] {
			newData = append(newData, row)
		} else {
			if t.enableReturning && opts != nil && opts.Returning {
				rowCopy := make([]any, len(row))
				copy(rowCopy, row)
				deletedRows = append(deletedRows, rowCopy)
			}
			deleted++
		}
	}
	t.data = newData

	elapsed := time.Since(start)
	fmt.Printf("[DEBUG] DELETE (batch interface) completed: %d rows deleted, duration=%v\n", deleted, elapsed)

	result := &catalog.DMLResult{AffectedRows: deleted}

	if t.enableReturning && opts != nil && opts.Returning && len(deletedRows) > 0 {
		returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
		returningReader, err := t.buildReturningReader(returningSchema, deletedRows)
		if err != nil {
			return nil, fmt.Errorf("failed to build RETURNING data: %w", err)
		}
		result.ReturningData = returningReader
	}

	return result, nil
}

func (t *batchDMLTable) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = make([][]any, 0)
	t.nextRowID = 1
	t.batchUpdateUsed = false
	t.batchDeleteUsed = false
}

func (t *batchDMLTable) RowCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.data)
}

func (t *batchDMLTable) SeedData(rows [][]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, rows...)
	maxRowID := int64(0)
	for _, row := range t.data {
		if rowID := row[0].(int64); rowID > maxRowID {
			maxRowID = rowID
		}
	}
	t.nextRowID = maxRowID + 1
}

func (t *batchDMLTable) buildReturningReader(inputSchema *arrow.Schema, rows [][]any) (array.RecordReader, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	numCols := inputSchema.NumFields()
	builders := make([]array.Builder, numCols)
	for i := 0; i < numCols; i++ {
		field := inputSchema.Field(i)
		builders[i] = array.NewBuilder(t.alloc, field.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	colMapping := make([]int, numCols)
	for i := 0; i < numCols; i++ {
		colName := inputSchema.Field(i).Name
		for j := 0; j < t.schema.NumFields(); j++ {
			if t.schema.Field(j).Name == colName {
				colMapping[i] = j
				break
			}
		}
	}

	for _, row := range rows {
		for i, builder := range builders {
			tableColIdx := colMapping[i]
			value := row[tableColIdx]

			switch b := builder.(type) {
			case *array.Int64Builder:
				if value == nil {
					b.AppendNull()
				} else {
					b.Append(value.(int64))
				}
			case *array.StringBuilder:
				if value == nil {
					b.AppendNull()
				} else {
					b.Append(value.(string))
				}
			}
		}
	}

	arrays := make([]arrow.Array, numCols)
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	record := array.NewRecordBatch(inputSchema, arrays, int64(len(rows)))
	for _, arr := range arrays {
		arr.Release()
	}

	return array.NewRecordReader(inputSchema, []arrow.RecordBatch{record})
}

// batchDMLCatalog creates a catalog with a batch-interface DML table.
func batchDMLCatalog(t *testing.T, table catalog.Table) catalog.Catalog {
	t.Helper()

	cat, err := airport.NewCatalogBuilder().
		Schema("batch_schema").
		Comment("Schema for batch interface DML testing").
		Table(table).
		Build()

	if err != nil {
		t.Fatalf("failed to build batch DML catalog: %v", err)
	}
	return cat
}
