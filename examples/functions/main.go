// Package main demonstrates scalar and table function implementation with column projection.
//
// To test with DuckDB CLI:
//
//	duckdb
//	INSTALL airport FROM community;
//	LOAD airport;
//	ATTACH 'grpc://localhost:50051' AS demo (TYPE airport);
//
//	-- Scalar function:
//	SELECT MULTIPLY(value, 10) FROM demo.functions_demo.users;
//
//	-- Table function with all columns:
//	SELECT * FROM demo.functions_demo.GENERATE_SERIES(1, 5);
//
//	-- Table function with dynamic schema:
//	SELECT * FROM demo.functions_demo.GENERATE_RANGE(1, 3, 4);  -- 4 columns
//
//	-- Column projection (server returns only requested columns):
//	SELECT name FROM demo.functions_demo.users;
//	SELECT col1, col3 FROM demo.functions_demo.GENERATE_RANGE(1, 5, 4);
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Create catalog with functions
	cat, err := createCatalogWithFunctions()
	if err != nil {
		log.Fatalf("Failed to create catalog: %v", err)
	}

	// Create and start server with debug logging
	debugLevel := slog.LevelDebug
	config := airport.ServerConfig{
		Catalog:  cat,
		Address:  "localhost:50051",
		LogLevel: &debugLevel,
	}

	grpcServer := grpc.NewServer(airport.ServerOptions(config)...)

	if err := airport.NewServer(grpcServer, config); err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Airport Flight server with functions listening on %s", lis.Addr())
	log.Printf("Connect with: ATTACH 'grpc://localhost:50051' AS my_server (TYPE airport)")
	log.Println("\nAvailable functions:")
	log.Println("  - Scalar: MULTIPLY(x INT64, factor INT64) -> INT64")
	log.Println("  - Table:  GENERATE_SERIES(start INT64, stop INT64, [step INT64]) -> (value INT64)")
	log.Println("  - Table:  GENERATE_RANGE(start, stop, columns) -> dynamic schema")
	log.Println("\nColumn projection examples:")
	log.Println("  SELECT name FROM demo.functions_demo.users;")
	log.Println("  SELECT col1, col3 FROM demo.functions_demo.GENERATE_RANGE(1, 5, 4);")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func createCatalogWithFunctions() (catalog.Catalog, error) {
	// Create simple data table with projection support
	usersTable := NewUsersTableWithProjection()

	return airport.NewCatalogBuilder().
		Schema("functions_demo").
		Comment("Schema demonstrating scalar and table functions with column projection").
		Table(usersTable).
		ScalarFunc(&multiplyFunc{}).
		TableFunc(&generateSeriesFunc{}).
		TableFunc(&generateRangeFunc{}).
		Build()
}

// =============================================================================
// Table with Column Projection Support
// =============================================================================

// UsersTableWithProjection demonstrates column projection pushdown.
// When a query requests specific columns, only those columns are returned.
type UsersTableWithProjection struct {
	schema *arrow.Schema
	data   [][]any
}

func NewUsersTableWithProjection() *UsersTableWithProjection {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	data := [][]any{
		{int64(1), "Alice", int64(100)},
		{int64(2), "Bob", int64(200)},
		{int64(3), "Charlie", int64(300)},
	}

	return &UsersTableWithProjection{schema: schema, data: data}
}

func (t *UsersTableWithProjection) Name() string               { return "users" }
func (t *UsersTableWithProjection) Comment() string            { return "Sample user data with column projection support" }
func (t *UsersTableWithProjection) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *UsersTableWithProjection) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	// Handle column projection
	outputSchema, columnIndices := t.projectColumns(opts)

	fmt.Printf("[UsersTable] Scan with columns=%v\n", getColumnNames(opts))

	// Build record with only requested columns
	record := t.buildProjectedRecord(outputSchema, columnIndices)
	return array.NewRecordReader(outputSchema, []arrow.RecordBatch{record})
}

func (t *UsersTableWithProjection) projectColumns(opts *catalog.ScanOptions) (*arrow.Schema, []int) {
	if opts == nil || len(opts.Columns) == 0 {
		// Return all columns
		indices := make([]int, t.schema.NumFields())
		for i := range indices {
			indices[i] = i
		}
		return t.schema, indices
	}

	// Build column name to index map
	colIndex := make(map[string]int)
	for i := 0; i < t.schema.NumFields(); i++ {
		colIndex[t.schema.Field(i).Name] = i
	}

	// Select only requested columns
	var fields []arrow.Field
	var indices []int
	for _, col := range opts.Columns {
		if idx, ok := colIndex[col]; ok {
			fields = append(fields, t.schema.Field(idx))
			indices = append(indices, idx)
		}
	}

	if len(fields) == 0 {
		// Fallback to all if no columns matched
		indices = make([]int, t.schema.NumFields())
		for i := range indices {
			indices[i] = i
		}
		return t.schema, indices
	}

	return arrow.NewSchema(fields, nil), indices
}

func (t *UsersTableWithProjection) buildProjectedRecord(schema *arrow.Schema, columnIndices []int) arrow.RecordBatch {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, row := range t.data {
		for outIdx, srcIdx := range columnIndices {
			switch v := row[srcIdx].(type) {
			case int64:
				builder.Field(outIdx).(*array.Int64Builder).Append(v)
			case string:
				builder.Field(outIdx).(*array.StringBuilder).Append(v)
			}
		}
	}

	return builder.NewRecordBatch()
}

// =============================================================================
// Scalar Function: MULTIPLY
// =============================================================================

// multiplyFunc multiplies an integer column by a constant factor.
type multiplyFunc struct{}

func (f *multiplyFunc) Name() string {
	return "MULTIPLY"
}

func (f *multiplyFunc) Comment() string {
	return "Multiplies input value by a constant factor"
}

func (f *multiplyFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.PrimitiveTypes.Int64, // input value
			arrow.PrimitiveTypes.Int64, // multiplication factor
		},
		ReturnType: arrow.PrimitiveTypes.Int64,
		Variadic:   false,
	}
}

func (f *multiplyFunc) Execute(_ context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	if input.NumCols() != 2 {
		return nil, fmt.Errorf("MULTIPLY expects 2 columns, got %d", input.NumCols())
	}

	valueCol := input.Column(0).(*array.Int64)
	factorCol := input.Column(1).(*array.Int64)

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	// Vectorized multiplication
	for i := 0; i < valueCol.Len(); i++ {
		if valueCol.IsNull(i) || factorCol.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(valueCol.Value(i) * factorCol.Value(i))
		}
	}

	return builder.NewInt64Array(), nil
}

// =============================================================================
// Table Function: GENERATE_SERIES
// =============================================================================

// generateSeriesFunc generates a series of integers from start to stop.
type generateSeriesFunc struct{}

func (f *generateSeriesFunc) Name() string {
	return "GENERATE_SERIES"
}

func (f *generateSeriesFunc) Comment() string {
	return "Generates a series of integers from start to stop with optional step"
}

func (f *generateSeriesFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.PrimitiveTypes.Int64, // start
			arrow.PrimitiveTypes.Int64, // stop
			arrow.PrimitiveTypes.Int64, // step (optional via variadic)
		},
		ReturnType: nil, // Table function, schema from SchemaForParameters
		Variadic:   true,
	}
}

func (f *generateSeriesFunc) SchemaForParameters(_ context.Context, _ []any) (*arrow.Schema, error) {
	return arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil), nil
}

func (f *generateSeriesFunc) Execute(_ context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
	if len(params) < 2 || len(params) > 3 {
		return nil, fmt.Errorf("GENERATE_SERIES requires 2 or 3 parameters, got %d", len(params))
	}

	start, err := toInt64(params[0])
	if err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	stop, err := toInt64(params[1])
	if err != nil {
		return nil, fmt.Errorf("stop: %w", err)
	}

	step := int64(1)
	if len(params) == 3 {
		step, err = toInt64(params[2])
		if err != nil {
			return nil, fmt.Errorf("step: %w", err)
		}
	}

	fmt.Printf("[GENERATE_SERIES] start=%d, stop=%d, step=%d\n", start, stop, step)

	// Generate series
	var values []int64
	for i := start; (step > 0 && i <= stop) || (step < 0 && i >= stop); i += step {
		values = append(values, i)
	}

	// Build Arrow record
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.AppendValues(values, nil)

	valueArray := builder.NewInt64Array()
	defer valueArray.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	record := array.NewRecordBatch(schema, []arrow.Array{valueArray}, int64(valueArray.Len()))

	return array.NewRecordReader(schema, []arrow.RecordBatch{record})
}

// =============================================================================
// Table Function: GENERATE_RANGE with Column Projection
// =============================================================================

// generateRangeFunc generates a table with dynamic schema and column projection support.
type generateRangeFunc struct{}

func (f *generateRangeFunc) Name() string {
	return "GENERATE_RANGE"
}

func (f *generateRangeFunc) Comment() string {
	return "Generates rows with dynamic number of columns (start, stop, column_count). Supports column projection."
}

func (f *generateRangeFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{
			arrow.PrimitiveTypes.Int64, // start
			arrow.PrimitiveTypes.Int64, // stop
			arrow.PrimitiveTypes.Int64, // column_count
		},
		ReturnType: nil, // Table function with dynamic schema
		Variadic:   false,
	}
}

func (f *generateRangeFunc) SchemaForParameters(_ context.Context, params []any) (*arrow.Schema, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("GENERATE_RANGE requires 3 parameters, got %d", len(params))
	}

	columnCount, err := toInt64(params[2])
	if err != nil {
		return nil, fmt.Errorf("column_count: %w", err)
	}

	if columnCount < 1 || columnCount > 10 {
		return nil, fmt.Errorf("column_count must be between 1 and 10, got %v", columnCount)
	}

	// Build dynamic schema
	fields := make([]arrow.Field, int(columnCount))
	for i := 0; i < int(columnCount); i++ {
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col%d", i+1),
			Type: arrow.PrimitiveTypes.Int64,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (f *generateRangeFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("GENERATE_RANGE requires 3 parameters, got %d", len(params))
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

	// Get full schema
	fullSchema, err := f.SchemaForParameters(ctx, params)
	if err != nil {
		return nil, err
	}

	// Handle column projection
	outputSchema, columnIndices := projectSchemaColumns(fullSchema, opts)

	fmt.Printf("[GENERATE_RANGE] start=%d, stop=%d, cols=%d, projected=%v\n",
		start, stop, columnCount, getColumnNames(opts))

	// Build record with projected columns
	builder := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer builder.Release()

	// Generate rows
	for i := start; i <= stop; i++ {
		for outIdx, srcColIdx := range columnIndices {
			// Each column value = row_index * (col + 1)
			value := i * int64(srcColIdx+1)
			builder.Field(outIdx).(*array.Int64Builder).Append(value)
		}
	}

	record := builder.NewRecordBatch()
	return array.NewRecordReader(outputSchema, []arrow.RecordBatch{record})
}

// =============================================================================
// Helper Functions
// =============================================================================

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("parameter must be number, got %T", v)
	}
}

func projectSchemaColumns(schema *arrow.Schema, opts *catalog.ScanOptions) (*arrow.Schema, []int) {
	if opts == nil || len(opts.Columns) == 0 {
		// Return all columns
		indices := make([]int, schema.NumFields())
		for i := range indices {
			indices[i] = i
		}
		return schema, indices
	}

	// Build column name to index map
	colIndex := make(map[string]int)
	for i := 0; i < schema.NumFields(); i++ {
		colIndex[schema.Field(i).Name] = i
	}

	// Select only requested columns
	var fields []arrow.Field
	var indices []int
	for _, col := range opts.Columns {
		if idx, ok := colIndex[col]; ok {
			fields = append(fields, schema.Field(idx))
			indices = append(indices, idx)
		}
	}

	if len(fields) == 0 {
		// Fallback to all if no columns matched
		indices = make([]int, schema.NumFields())
		for i := range indices {
			indices[i] = i
		}
		return schema, indices
	}

	return arrow.NewSchema(fields, nil), indices
}

func getColumnNames(opts *catalog.ScanOptions) []string {
	if opts == nil || len(opts.Columns) == 0 {
		return []string{"*"}
	}
	return opts.Columns
}
