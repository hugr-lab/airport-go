// Package main demonstrates scalar and table function implementation.
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
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

	// Create and start server
	config := airport.ServerConfig{
		Catalog: cat,
		Address: "localhost:50051",
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

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func createCatalogWithFunctions() (catalog.Catalog, error) {
	// Create simple data table
	usersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	usersData := [][]interface{}{
		{int64(1), "Alice", int64(100)},
		{int64(2), "Bob", int64(200)},
		{int64(3), "Charlie", int64(300)},
	}

	return airport.NewCatalogBuilder().
		Schema("functions_demo").
		Comment("Schema demonstrating scalar and table functions").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "Sample user data",
			Schema:   usersSchema,
			ScanFunc: makeScanFunc(usersSchema, usersData),
		}).
		ScalarFunc(&multiplyFunc{}).
		TableFunc(&generateSeriesFunc{}).
		TableFunc(&generateRangeFunc{}).
		Build()
}

// makeScanFunc creates a scan function from static data.
func makeScanFunc(schema *arrow.Schema, data [][]interface{}) func(context.Context, *catalog.ScanOptions) (array.RecordReader, error) {
	return func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		record := buildRecord(schema, data)
		return array.NewRecordReader(schema, []arrow.Record{record})
	}
}

// buildRecord creates an Arrow record from test data.
func buildRecord(schema *arrow.Schema, data [][]interface{}) arrow.Record {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, row := range data {
		for i, val := range row {
			switch v := val.(type) {
			case int64:
				builder.Field(i).(*array.Int64Builder).Append(v)
			case string:
				builder.Field(i).(*array.StringBuilder).Append(v)
			}
		}
	}

	return builder.NewRecord()
}

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

func (f *multiplyFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
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

	resultArray := builder.NewInt64Array()
	defer resultArray.Release()

	resultSchema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	return array.NewRecord(resultSchema, []arrow.Array{resultArray}, int64(resultArray.Len())), nil
}

// generateSeriesFunc generates a series of integers from start to stop.
// Schema is fixed, but range depends on parameters.
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

func (f *generateSeriesFunc) SchemaForParameters(ctx context.Context, params []interface{}) (*arrow.Schema, error) {
	// Schema is always the same - single Int64 column
	return arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil), nil
}

func (f *generateSeriesFunc) Execute(ctx context.Context, params []interface{}, opts *catalog.ScanOptions) (array.RecordReader, error) {
	if len(params) < 2 || len(params) > 3 {
		return nil, fmt.Errorf("GENERATE_SERIES requires 2 or 3 parameters, got %d", len(params))
	}

	// Extract parameters
	start, ok := params[0].(float64) // JSON numbers decode as float64
	if !ok {
		return nil, fmt.Errorf("start parameter must be number, got %T", params[0])
	}

	stop, ok := params[1].(float64)
	if !ok {
		return nil, fmt.Errorf("stop parameter must be number, got %T", params[1])
	}

	step := float64(1)
	if len(params) == 3 {
		step, ok = params[2].(float64)
		if !ok {
			return nil, fmt.Errorf("step parameter must be number, got %T", params[2])
		}
	}

	// Generate series
	var values []int64
	for i := int64(start); (step > 0 && i <= int64(stop)) || (step < 0 && i >= int64(stop)); i += int64(step) {
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

	record := array.NewRecord(schema, []arrow.Array{valueArray}, int64(valueArray.Len()))

	return array.NewRecordReader(schema, []arrow.Record{record})
}

// generateRangeFunc generates a table with dynamic schema based on column count parameter.
// This demonstrates how table functions can return different schemas based on input.
type generateRangeFunc struct{}

func (f *generateRangeFunc) Name() string {
	return "GENERATE_RANGE"
}

func (f *generateRangeFunc) Comment() string {
	return "Generates rows with dynamic number of columns (start, stop, column_count)"
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

func (f *generateRangeFunc) SchemaForParameters(ctx context.Context, params []interface{}) (*arrow.Schema, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("GENERATE_RANGE requires 3 parameters, got %d", len(params))
	}

	// Extract column count parameter
	columnCount, ok := params[2].(float64)
	if !ok {
		return nil, fmt.Errorf("column_count parameter must be number, got %T", params[2])
	}

	if columnCount < 1 || columnCount > 10 {
		return nil, fmt.Errorf("column_count must be between 1 and 10, got %v", columnCount)
	}

	// Build dynamic schema with requested number of columns
	fields := make([]arrow.Field, int(columnCount))
	for i := 0; i < int(columnCount); i++ {
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col%d", i+1),
			Type: arrow.PrimitiveTypes.Int64,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (f *generateRangeFunc) Execute(ctx context.Context, params []interface{}, opts *catalog.ScanOptions) (array.RecordReader, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("GENERATE_RANGE requires 3 parameters, got %d", len(params))
	}

	// Extract parameters
	start, ok := params[0].(float64)
	if !ok {
		return nil, fmt.Errorf("start parameter must be number, got %T", params[0])
	}

	stop, ok := params[1].(float64)
	if !ok {
		return nil, fmt.Errorf("stop parameter must be number, got %T", params[1])
	}

	columnCount, ok := params[2].(float64)
	if !ok {
		return nil, fmt.Errorf("column_count parameter must be number, got %T", params[2])
	}

	// Get schema
	schema, err := f.SchemaForParameters(ctx, params)
	if err != nil {
		return nil, err
	}

	// Build record with dynamic columns
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	// Generate rows
	for i := int64(start); i <= int64(stop); i++ {
		for col := 0; col < int(columnCount); col++ {
			// Each column gets row_index * (col + 1)
			builder.Field(col).(*array.Int64Builder).Append(i * int64(col+1))
		}
	}

	record := builder.NewRecord()

	return array.NewRecordReader(schema, []arrow.Record{record})
}
