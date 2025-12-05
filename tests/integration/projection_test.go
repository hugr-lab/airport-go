package airport_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	airport "github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// =============================================================================
// Column Projection Tests
// =============================================================================
// These tests verify that column projection (SELECT specific columns) is
// properly handled by tables and table functions.

// TestTableColumnProjection tests that tables return only requested columns.
func TestTableColumnProjection(t *testing.T) {
	// Create table that tracks which columns were requested
	table := newProjectionTestTable()
	cat := projectionTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("SelectAllColumns", func(t *testing.T) {
		table.Reset()

		rows, err := db.Query(fmt.Sprintf(
			"SELECT * FROM %s.projection_schema.data",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT * failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 4 {
			t.Errorf("expected 4 columns, got %d: %v", len(cols), cols)
		}

		var count int
		for rows.Next() {
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}

		// Verify scan was called
		t.Logf("SelectAllColumns: scanCount=%d, requestedCols=%v", table.GetScanCount(), table.GetRequestedCols())
		if table.GetScanCount() == 0 {
			t.Errorf("Scan was never called")
		}
	})

	t.Run("SelectSingleColumn", func(t *testing.T) {
		table.Reset()

		rows, err := db.Query(fmt.Sprintf(
			"SELECT name FROM %s.projection_schema.data",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT name failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 1 {
			t.Errorf("expected 1 column, got %d: %v", len(cols), cols)
		}
		if cols[0] != "name" {
			t.Errorf("expected column 'name', got '%s'", cols[0])
		}

		// Verify data
		var names []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			names = append(names, name)
		}

		expected := []string{"Alice", "Bob", "Charlie"}
		if len(names) != len(expected) {
			t.Errorf("expected %d rows, got %d", len(expected), len(names))
		}
		for i, name := range names {
			if name != expected[i] {
				t.Errorf("row %d: expected '%s', got '%s'", i, expected[i], name)
			}
		}

		// Verify column projection was passed to Scan
		t.Logf("SelectSingleColumn: scanCount=%d, requestedCols=%v", table.GetScanCount(), table.GetRequestedCols())
		if table.GetScanCount() == 0 {
			t.Fatalf("Scan was never called")
		}
		// We expect only "name" column to be requested for projection pushdown
		wantCols := []string{"name"}
		if !slices.Equal(table.GetRequestedCols(), wantCols) {
			t.Errorf("Column projection not working: got %v, want %v", table.GetRequestedCols(), wantCols)
		}
	})

	t.Run("SelectMultipleColumns", func(t *testing.T) {
		table.Reset()

		rows, err := db.Query(fmt.Sprintf(
			"SELECT id, email FROM %s.projection_schema.data",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT id, email failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 2 {
			t.Errorf("expected 2 columns, got %d: %v", len(cols), cols)
		}

		// Verify column order
		if cols[0] != "id" || cols[1] != "email" {
			t.Errorf("expected columns [id, email], got %v", cols)
		}

		// Verify data
		var count int
		for rows.Next() {
			var id int64
			var email string
			if err := rows.Scan(&id, &email); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}

		// Verify column projection was passed to Scan
		t.Logf("SelectMultipleColumns: scanCount=%d, requestedCols=%v", table.GetScanCount(), table.GetRequestedCols())
		if table.GetScanCount() == 0 {
			t.Fatalf("Scan was never called")
		}
		// We expect only "id" and "email" columns to be requested
		wantCols := []string{"id", "email"}
		if !slices.Equal(table.GetRequestedCols(), wantCols) {
			t.Errorf("Column projection not working: got %v, want %v", table.GetRequestedCols(), wantCols)
		}
	})

	t.Run("SelectColumnsReversedOrder", func(t *testing.T) {
		table.Reset()

		rows, err := db.Query(fmt.Sprintf(
			"SELECT email, name, id FROM %s.projection_schema.data",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT email, name, id failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 3 {
			t.Errorf("expected 3 columns, got %d: %v", len(cols), cols)
		}

		// DuckDB may reorder columns based on its optimization
		// Just verify we get the right data
		var count int
		for rows.Next() {
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}
	})
}

// TestTableFunctionColumnProjection tests that table functions return only requested columns.
func TestTableFunctionColumnProjection(t *testing.T) {
	cat := tableFunctionProjectionCatalog(t)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("SelectAllColumnsFromTableFunction", func(t *testing.T) {
		// GENERATE_RANGE creates dynamic columns: col1, col2, col3, col4
		rows, err := db.Query(fmt.Sprintf(
			"SELECT * FROM %s.func_schema.GENERATE_RANGE(1, 3, 4)",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT * FROM GENERATE_RANGE failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 4 {
			t.Errorf("expected 4 columns, got %d: %v", len(cols), cols)
		}

		var count int
		for rows.Next() {
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 rows (1,2,3), got %d", count)
		}
	})

	t.Run("SelectSpecificColumnsFromTableFunction", func(t *testing.T) {
		// Select only col1 and col3 from a 4-column result
		rows, err := db.Query(fmt.Sprintf(
			"SELECT col1, col3 FROM %s.func_schema.GENERATE_RANGE(1, 3, 4)",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT col1, col3 FROM GENERATE_RANGE failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 2 {
			t.Errorf("expected 2 columns, got %d: %v", len(cols), cols)
		}

		// Verify data: col1 = row * 1, col3 = row * 3
		type row struct {
			col1 int64
			col3 int64
		}
		var results []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.col1, &r.col3); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			results = append(results, r)
		}

		expected := []row{
			{col1: 1, col3: 3}, // row 1: 1*1, 1*3
			{col1: 2, col3: 6}, // row 2: 2*1, 2*3
			{col1: 3, col3: 9}, // row 3: 3*1, 3*3
		}

		if len(results) != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), len(results))
		}

		for i, r := range results {
			if r.col1 != expected[i].col1 || r.col3 != expected[i].col3 {
				t.Errorf("row %d: expected {%d, %d}, got {%d, %d}",
					i, expected[i].col1, expected[i].col3, r.col1, r.col3)
			}
		}
	})

	t.Run("SelectSingleColumnFromTableFunction", func(t *testing.T) {
		rows, err := db.Query(fmt.Sprintf(
			"SELECT col2 FROM %s.func_schema.GENERATE_RANGE(1, 5, 3)",
			attachName,
		))
		if err != nil {
			t.Fatalf("SELECT col2 FROM GENERATE_RANGE failed: %v", err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		if len(cols) != 1 {
			t.Errorf("expected 1 column, got %d: %v", len(cols), cols)
		}

		// col2 = row * 2
		var values []int64
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			values = append(values, v)
		}

		expected := []int64{2, 4, 6, 8, 10} // rows 1-5, each * 2
		if len(values) != len(expected) {
			t.Fatalf("expected %d values, got %d", len(expected), len(values))
		}

		for i, v := range values {
			if v != expected[i] {
				t.Errorf("row %d: expected %d, got %d", i, expected[i], v)
			}
		}
	})
}

// =============================================================================
// Test Infrastructure
// =============================================================================

// projectionTestTable tracks column requests for testing projection pushdown.
type projectionTestTable struct {
	schema *arrow.Schema
	data   [][]any

	mu            sync.Mutex
	requestedCols []string
	scanCount     int
}

func newProjectionTestTable() *projectionTestTable {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	data := [][]any{
		{int64(1), "Alice", "alice@example.com", int64(100)},
		{int64(2), "Bob", "bob@example.com", int64(200)},
		{int64(3), "Charlie", "charlie@example.com", int64(300)},
	}

	return &projectionTestTable{schema: schema, data: data}
}

func (t *projectionTestTable) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requestedCols = nil
	t.scanCount = 0
}

func (t *projectionTestTable) GetScanCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.scanCount
}

func (t *projectionTestTable) GetRequestedCols() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requestedCols
}

func (t *projectionTestTable) Name() string    { return "data" }
func (t *projectionTestTable) Comment() string { return "Test table for column projection" }
func (t *projectionTestTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *projectionTestTable) Scan(_ context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.Lock()
	t.scanCount++

	// Track requested columns for test verification
	if opts != nil && len(opts.Columns) > 0 {
		t.requestedCols = opts.Columns
	} else {
		t.requestedCols = []string{"*"}
	}
	t.mu.Unlock()

	// Note: DuckDB expects full schema - it handles projection client-side.
	// The column projection info is a hint for optimization (e.g., skip fetching
	// columns from a database), but the returned schema must match ArrowSchema().
	builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
	defer builder.Release()

	for _, row := range t.data {
		builder.Field(0).(*array.Int64Builder).Append(row[0].(int64))
		builder.Field(1).(*array.StringBuilder).Append(row[1].(string))
		builder.Field(2).(*array.StringBuilder).Append(row[2].(string))
		builder.Field(3).(*array.Int64Builder).Append(row[3].(int64))
	}

	record := builder.NewRecordBatch()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

func projectionTestCatalog(t *testing.T, table *projectionTestTable) catalog.Catalog {
	t.Helper()
	cat, err := airport.NewCatalogBuilder().
		Schema("projection_schema").
		Table(table).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}
	return cat
}

// =============================================================================
// Table Function with Projection Support
// =============================================================================

type generateRangeWithProjection struct{}

func (f *generateRangeWithProjection) Name() string { return "GENERATE_RANGE" }
func (f *generateRangeWithProjection) Comment() string {
	return "Generate range with column projection"
}

func (f *generateRangeWithProjection) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.PrimitiveTypes.Int64, // start
			arrow.PrimitiveTypes.Int64, // stop
			arrow.PrimitiveTypes.Int64, // column_count
		},
		ReturnType: nil,
	}
}

func (f *generateRangeWithProjection) SchemaForParameters(_ context.Context, params []any) (*arrow.Schema, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("expected 3 params, got %d", len(params))
	}

	columnCount := toInt64Value(params[2])
	if columnCount < 1 || columnCount > 10 {
		return nil, fmt.Errorf("column_count must be 1-10, got %d", columnCount)
	}

	fields := make([]arrow.Field, columnCount)
	for i := int64(0); i < columnCount; i++ {
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col%d", i+1),
			Type: arrow.PrimitiveTypes.Int64,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (f *generateRangeWithProjection) Execute(ctx context.Context, params []any, _ *catalog.ScanOptions) (array.RecordReader, error) {
	start := toInt64Value(params[0])
	stop := toInt64Value(params[1])
	columnCount := toInt64Value(params[2])

	fullSchema, err := f.SchemaForParameters(ctx, params)
	if err != nil {
		return nil, err
	}

	// Note: DuckDB expects full schema - it handles projection client-side.
	// The column projection info is a hint for optimization (e.g., skip fetching
	// columns from a database), but the returned schema must match SchemaForParameters().
	builder := array.NewRecordBuilder(memory.DefaultAllocator, fullSchema)
	defer builder.Release()

	for i := start; i <= stop; i++ {
		for col := int64(0); col < columnCount; col++ {
			// Value = row * (column_number)
			value := i * (col + 1)
			builder.Field(int(col)).(*array.Int64Builder).Append(value)
		}
	}

	record := builder.NewRecordBatch()
	return array.NewRecordReader(fullSchema, []arrow.RecordBatch{record})
}

func tableFunctionProjectionCatalog(t *testing.T) catalog.Catalog {
	t.Helper()
	cat, err := airport.NewCatalogBuilder().
		Schema("func_schema").
		TableFunc(&generateRangeWithProjection{}).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}
	return cat
}

func toInt64Value(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	case int:
		return int64(val)
	default:
		return 0
	}
}
