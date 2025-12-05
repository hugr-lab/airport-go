package airport_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// TestTableFunctions tests normal table-returning functions (not in/out).
// These use table_function_flight_info action followed by DoGet.
func TestTableFunctions(t *testing.T) {
	cat := catalogWithTableFunctions(t)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("GenerateSeries", func(t *testing.T) {
		// Test basic table function that generates a series of integers
		query := fmt.Sprintf("SELECT * FROM %s.test_schema.GENERATE_SERIES(1, 5)", attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Table function call failed: %v", err)
		}
		defer rows.Close()

		expected := []int64{1, 2, 3, 4, 5}
		idx := 0
		for rows.Next() {
			var value int64
			if err := rows.Scan(&value); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			if idx >= len(expected) {
				t.Fatalf("Got more rows than expected")
			}
			if value != expected[idx] {
				t.Errorf("Row %d: expected %d, got %d", idx, expected[idx], value)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("DynamicSchema", func(t *testing.T) {
		// Test table function with dynamic schema based on parameters
		// GENERATE_RANGE(start, stop, column_count) creates N columns
		query := fmt.Sprintf("SELECT * FROM %s.test_schema.GENERATE_RANGE(1, 3, 4)", attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Table function call failed: %v", err)
		}
		defer rows.Close()

		// Should get 3 rows with 4 columns each
		rowCount := 0
		for rows.Next() {
			var col1, col2, col3, col4 int64
			if err := rows.Scan(&col1, &col2, &col3, &col4); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			rowCount++

			// Each column should have the same value (the row number)
			expectedVal := int64(rowCount)
			if col1 != expectedVal || col2 != expectedVal || col3 != expectedVal || col4 != expectedVal {
				t.Errorf("Row %d: expected all columns = %d, got [%d, %d, %d, %d]",
					rowCount, expectedVal, col1, col2, col3, col4)
			}
		}

		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
		}
	})
}

// TestTableFunctionsInOut tests table functions that accept row sets as input.
// These use DoExchange bidirectional streaming.
func TestTableFunctionsInOut(t *testing.T) {
	cat := catalogWithTableFunctionsInOut(t)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("PassThrough", func(t *testing.T) {
		// Test in/out function that passes rows through unchanged
		query := fmt.Sprintf(`
			SELECT * FROM %s.test_schema.FILTER_ROWS(
				(SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name))
			)
		`, attachName)

		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Table in/out function failed: %v", err)
		}
		defer rows.Close()

		expected := []struct {
			id   int64
			name string
		}{
			{1, "a"},
			{2, "b"},
			{3, "c"},
		}

		idx := 0
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}

			if idx >= len(expected) {
				t.Fatalf("Got more rows than expected")
			}

			if id != expected[idx].id || name != expected[idx].name {
				t.Errorf("Row %d: expected (%d, %s), got (%d, %s)",
					idx, expected[idx].id, expected[idx].name, id, name)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("WithScalarParameter", func(t *testing.T) {
		// Test in/out function with scalar parameters
		query := fmt.Sprintf(`
			SELECT * FROM %s.test_schema.MULTIPLY_COLUMN(
				'id',
				10,
				(SELECT * FROM (VALUES (1), (2), (3)) AS t(id))
			)
		`, attachName)

		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Table in/out function with parameters failed: %v", err)
		}
		defer rows.Close()

		expected := []int64{10, 20, 30}
		idx := 0
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}

			if idx >= len(expected) {
				t.Fatalf("Got more rows than expected")
			}

			if id != expected[idx] {
				t.Errorf("Row %d: expected %d, got %d", idx, expected[idx], id)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})
}

// TestTableFunctionErrors tests error handling for table functions.
func TestTableFunctionErrors(t *testing.T) {
	cat := catalogWithTableFunctions(t)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("InvalidParameters", func(t *testing.T) {
		// Test with invalid parameter count
		query := fmt.Sprintf("SELECT * FROM %s.test_schema.GENERATE_SERIES(1)", attachName)
		_, err := db.Query(query)
		if err == nil {
			t.Error("Expected error for invalid parameter count")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("InvalidParameterType", func(t *testing.T) {
		// Test with invalid parameter type
		query := fmt.Sprintf("SELECT * FROM %s.test_schema.GENERATE_SERIES('a', 'b')", attachName)
		_, err := db.Query(query)
		if err == nil {
			t.Error("Expected error for invalid parameter type")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("FunctionNotFound", func(t *testing.T) {
		// Test calling non-existent function
		query := fmt.Sprintf("SELECT * FROM %s.test_schema.NONEXISTENT()", attachName)
		_, err := db.Query(query)
		if err == nil {
			t.Error("Expected error for non-existent function")
		}
		t.Logf("Got expected error: %v", err)
	})
}

// Helper functions to create test catalogs

func catalogWithTableFunctions(t *testing.T) catalog.Catalog {
	t.Helper()
	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		TableFunc(&generateSeriesFunc{}).
		TableFunc(&generateRangeFunc{}).
		Build()
	if err != nil {
		t.Fatalf("failed to build catalog: %v", err)
	}
	return cat
}

func catalogWithTableFunctionsInOut(t *testing.T) catalog.Catalog {
	t.Helper()
	// Create a catalog with in/out table functions and a versioned table
	versionedData := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
	}, nil)

	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		TableFuncInOut(&filterRowsFunc{}).
		TableFuncInOut(&multiplyColumnFunc{}).
		SimpleTable(airport.SimpleTableDef{
			Name:   "versioned_data",
			Schema: versionedData,
			ScanFunc: func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
				builder := array.NewRecordBuilder(memory.DefaultAllocator, versionedData)
				defer builder.Release()

				// Generate some test data
				builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

				now := arrow.Timestamp(time.Now().UnixMicro())
				builder.Field(1).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{now, now, now}, nil)

				record := builder.NewRecordBatch()
				defer record.Release()

				return array.NewRecordReader(versionedData, []arrow.RecordBatch{record})
			},
		}).
		Build()
	if err != nil {
		t.Fatalf("failed to build catalog: %v", err)
	}
	return cat
}

// Test function implementations

// filterRowsFunc is a simple in/out function that passes rows through
type filterRowsFunc struct{}

func (f *filterRowsFunc) Name() string {
	return "FILTER_ROWS"
}

func (f *filterRowsFunc) Comment() string {
	return "Pass-through filter for testing in/out functions"
}

func (f *filterRowsFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.Null, // Table input parameter (type doesn't matter, marked by is_table_type metadata)
		},
		ReturnType: nil, // Table function
	}
}

func (f *filterRowsFunc) SchemaForParameters(ctx context.Context, params []any, inputSchema *arrow.Schema) (*arrow.Schema, error) {
	return inputSchema, nil
}

func (f *filterRowsFunc) Execute(ctx context.Context, params []any, input array.RecordReader, opts *catalog.ScanOptions) (array.RecordReader, error) {
	// For in/out table functions via DoExchange, we can return the input reader directly
	// since it's already a streaming reader. The data will flow through as it arrives.
	// We just need to ensure the reader stays valid for the caller.
	return input, nil
}

// multiplyColumnFunc multiplies a column by a scalar value
type multiplyColumnFunc struct{}

func (f *multiplyColumnFunc) Name() string {
	return "MULTIPLY_COLUMN"
}

func (f *multiplyColumnFunc) Comment() string {
	return "Multiplies a column by a scalar value"
}

func (f *multiplyColumnFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.BinaryTypes.String,   // column name
			arrow.PrimitiveTypes.Int64, // multiplier
			arrow.Null,                 // Table input parameter (marked by is_table_type metadata)
		},
		ReturnType: nil,
	}
}

func (f *multiplyColumnFunc) SchemaForParameters(ctx context.Context, params []any, inputSchema *arrow.Schema) (*arrow.Schema, error) {
	return inputSchema, nil
}

func (f *multiplyColumnFunc) Execute(ctx context.Context, params []any, input array.RecordReader, opts *catalog.ScanOptions) (array.RecordReader, error) {
	// Note: Last parameter in signature is the table input (arrow.Null with is_table_type metadata)
	// It's not included in params array - only scalar parameters are passed
	if len(params) != 2 {
		return nil, fmt.Errorf("MULTIPLY_COLUMN requires 2 scalar parameters, got %d", len(params))
	}

	// Extract parameters
	columnName, ok := params[0].(string)
	if !ok {
		return nil, fmt.Errorf("column name must be string")
	}

	var multiplier int64
	switch v := params[1].(type) {
	case int64:
		multiplier = v
	case float64:
		multiplier = int64(v)
	default:
		return nil, fmt.Errorf("multiplier must be number")
	}

	// Find column index
	schema := input.Schema()
	colIdx := -1
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == columnName {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	// Create a streaming reader that transforms data on-the-fly
	// We wrap the input reader to process batches as they arrive
	return &multiplyColumnReader{
		input:      input,
		schema:     schema,
		colIdx:     colIdx,
		multiplier: multiplier,
	}, nil
}

// multiplyColumnReader is a streaming RecordReader that multiplies a column
type multiplyColumnReader struct {
	input      array.RecordReader
	schema     *arrow.Schema
	colIdx     int
	multiplier int64
	current    arrow.RecordBatch
}

func (r *multiplyColumnReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *multiplyColumnReader) Next() bool {
	if !r.input.Next() {
		return false
	}

	// Process the batch
	batch := r.input.RecordBatch()
	col := batch.Column(r.colIdx)

	// Handle both Int32 and Int64 columns - output same type as input
	var newCol arrow.Array

	switch typedCol := col.(type) {
	case *array.Int64:
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < typedCol.Len(); i++ {
			if typedCol.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(typedCol.Value(i) * r.multiplier)
			}
		}
		newCol = builder.NewInt64Array()
	case *array.Int32:
		builder := array.NewInt32Builder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < typedCol.Len(); i++ {
			if typedCol.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32(int64(typedCol.Value(i)) * r.multiplier))
			}
		}
		newCol = builder.NewInt32Array()
	default:
		// For other types, just pass through
		r.current = batch
		batch.Retain()
		return true
	}

	defer newCol.Release()

	// Release previous batch if any
	if r.current != nil {
		r.current.Release()
	}

	// Create new batch with all columns, replacing only the multiplied column
	numCols := int(batch.NumCols())
	cols := make([]arrow.Array, numCols)
	for i := 0; i < numCols; i++ {
		if i == r.colIdx {
			cols[i] = newCol
			newCol.Retain()
		} else {
			cols[i] = batch.Column(i)
			cols[i].Retain()
		}
	}
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()

	r.current = array.NewRecordBatch(r.schema, cols, batch.NumRows())
	return true
}

func (r *multiplyColumnReader) RecordBatch() arrow.RecordBatch {
	return r.current
}

func (r *multiplyColumnReader) Record() arrow.RecordBatch {
	return r.current
}

func (r *multiplyColumnReader) Err() error {
	return r.input.Err()
}

func (r *multiplyColumnReader) Release() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.input.Release()
}

func (r *multiplyColumnReader) Retain() {
	// Not implemented for this simple reader
}

// generateSeriesFunc generates a series of integers (normal table function)
type generateSeriesFunc struct{}

func (f *generateSeriesFunc) Name() string {
	return "GENERATE_SERIES"
}

func (f *generateSeriesFunc) Comment() string {
	return "Generates a series of integers from start to stop"
}

func (f *generateSeriesFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.PrimitiveTypes.Int64, // start
			arrow.PrimitiveTypes.Int64, // stop
		},
		ReturnType: nil,
	}
}

func (f *generateSeriesFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
	return arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil), nil
}

func (f *generateSeriesFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("GENERATE_SERIES requires 2 parameters")
	}

	start, stop, err := extractInt64Range(params)
	if err != nil {
		return nil, err
	}

	schema, _ := f.SchemaForParameters(ctx, params)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for i := start; i <= stop; i++ {
		builder.Field(0).(*array.Int64Builder).Append(i)
	}

	record := builder.NewRecordBatch()
	defer record.Release()

	return array.NewRecordReader(schema, []arrow.RecordBatch{record})
}

// generateRangeFunc generates rows with dynamic column count
type generateRangeFunc struct{}

func (f *generateRangeFunc) Name() string {
	return "GENERATE_RANGE"
}

func (f *generateRangeFunc) Comment() string {
	return "Generates rows with N columns, schema depends on column_count parameter"
}

func (f *generateRangeFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.PrimitiveTypes.Int64, // start
			arrow.PrimitiveTypes.Int64, // stop
			arrow.PrimitiveTypes.Int64, // column_count
		},
		ReturnType: nil,
	}
}

func (f *generateRangeFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("GENERATE_RANGE requires 3 parameters")
	}

	columnCount, err := toInt64(params[2])
	if err != nil {
		return nil, fmt.Errorf("column_count: %w", err)
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

func (f *generateRangeFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("GENERATE_RANGE requires 3 parameters")
	}

	start, err := toInt64(params[0])
	if err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	stop, err := toInt64(params[1])
	if err != nil {
		return nil, fmt.Errorf("stop: %w", err)
	}

	columnCount, err := toInt64(params[2])
	if err != nil {
		return nil, fmt.Errorf("column_count: %w", err)
	}

	schema, err := f.SchemaForParameters(ctx, params)
	if err != nil {
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for i := start; i <= stop; i++ {
		for col := int64(0); col < columnCount; col++ {
			builder.Field(int(col)).(*array.Int64Builder).Append(i)
		}
	}

	record := builder.NewRecordBatch()
	defer record.Release()

	return array.NewRecordReader(schema, []arrow.RecordBatch{record})
}

// Helper functions

func extractInt64Range(params []any) (start, stop int64, err error) {
	if len(params) != 2 {
		return 0, 0, fmt.Errorf("expected 2 parameters, got %d", len(params))
	}

	start, err = toInt64(params[0])
	if err != nil {
		return 0, 0, fmt.Errorf("start: %w", err)
	}

	stop, err = toInt64(params[1])
	if err != nil {
		return 0, 0, fmt.Errorf("stop: %w", err)
	}

	return start, stop, nil
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}
