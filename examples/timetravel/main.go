// Package main demonstrates an Airport Flight server with time travel support.
// Time travel allows querying data at specific points in time using DuckDB's AT syntax.
//
// To test with DuckDB CLI:
//
//	duckdb
//	INSTALL airport FROM community;
//	LOAD airport;
//	ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
//
//	-- Query current data:
//	SELECT * FROM demo.test.users;
//
//	-- Query data at specific versions:
//	SELECT * FROM demo.test.users AT (VERSION => 1);  -- Only Alice
//	SELECT * FROM demo.test.users AT (VERSION => 2);  -- Alice + Bob
//	SELECT * FROM demo.test.users AT (VERSION => 3);  -- All three users
//
//	-- Query with column selection (projection pushdown):
//	SELECT name FROM demo.test.users AT (VERSION => 2);
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Create time travel table
	table := NewTimeTravelUsersTable()

	// Build catalog
	cat, err := airport.NewCatalogBuilder().
		Schema("test").
		Table(table).
		Build()
	if err != nil {
		log.Fatalf("Failed to build catalog: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register Airport handlers
	debugLevel := slog.LevelDebug
	err = airport.NewServer(grpcServer, airport.ServerConfig{
		Catalog:  cat,
		LogLevel: &debugLevel,
	})
	if err != nil {
		log.Fatalf("Failed to register Airport server: %v", err)
	}

	// Start serving
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Airport Time Travel server listening on :50051")
	log.Println("Example catalog contains:")
	log.Println("  - Schema: test")
	log.Println("    - Table: users (time travel enabled)")
	log.Println("")
	log.Println("Data versions:")
	log.Println("  VERSION 1: Alice only")
	log.Println("  VERSION 2: Alice + Bob")
	log.Println("  VERSION 3: Alice + Bob + Charlie (current)")
	log.Println("")
	log.Println("Test with DuckDB CLI:")
	log.Println("  ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');")
	log.Println("  SELECT * FROM demo.test.users;                    -- Current (v3)")
	log.Println("  SELECT * FROM demo.test.users AT (VERSION => 1);  -- v1: Alice")
	log.Println("  SELECT * FROM demo.test.users AT (VERSION => 2);  -- v2: Alice+Bob")
	log.Println("")
	log.Println("Column projection (returns only requested columns):")
	log.Println("  SELECT name FROM demo.test.users;")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// =============================================================================
// Time Travel Table Implementation
// =============================================================================

// TimeTravelUsersTable demonstrates a table with version-based time travel.
// It implements catalog.Table and catalog.DynamicSchemaTable for AT syntax support.
type TimeTravelUsersTable struct {
	schema *arrow.Schema
	alloc  memory.Allocator

	// Versioned data: version -> rows
	versions map[int64][][]any
}

func NewTimeTravelUsersTable() *TimeTravelUsersTable {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// Create versioned history
	versions := map[int64][][]any{
		1: {
			{int64(1), "Alice", "alice@example.com"},
		},
		2: {
			{int64(1), "Alice", "alice@example.com"},
			{int64(2), "Bob", "bob@example.com"},
		},
		3: {
			{int64(1), "Alice", "alice@example.com"},
			{int64(2), "Bob", "bob@example.com"},
			{int64(3), "Charlie", "charlie@example.com"},
		},
	}

	return &TimeTravelUsersTable{
		schema:   schema,
		alloc:    memory.DefaultAllocator,
		versions: versions,
	}
}

// Table interface implementation

func (t *TimeTravelUsersTable) Name() string { return "users" }
func (t *TimeTravelUsersTable) Comment() string {
	return "Users table with time travel support (versions 1-3)"
}

func (t *TimeTravelUsersTable) ArrowSchema(columns []string) *arrow.Schema {
	return t.schema
}

func (t *TimeTravelUsersTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	// Determine which version to query
	version := int64(3) // Default to current (latest)
	if opts != nil && opts.TimePoint != nil {
		if opts.TimePoint.Unit == "version" {
			v, err := strconv.ParseInt(opts.TimePoint.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid version: %s", opts.TimePoint.Value)
			}
			version = v
		}
	}

	data, ok := t.versions[version]
	if !ok {
		return nil, fmt.Errorf("version %d not found (available: 1-3)", version)
	}

	fmt.Printf("[TimeTravelTable] Query version=%d, columns=%v\n", version, getColumnNames(opts))

	// Build record with projected columns
	record := t.buildRecord(t.schema, data, opts.Columns)
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

// DynamicSchemaTable interface implementation
// This enables schema introspection for time-travel queries

//nolint:unparam // error return required by interface
func (t *TimeTravelUsersTable) SchemaForRequest(_ context.Context, req *catalog.SchemaRequest) (*arrow.Schema, error) {
	// here you can adjust schema based on req.Parameters or req.TimePoint if needed
	return t.schema, nil
}

// Helper methods
func (t *TimeTravelUsersTable) buildRecord(schema *arrow.Schema, data [][]any, columns []string) arrow.RecordBatch {
	builder := array.NewRecordBuilder(t.alloc, schema)
	defer builder.Release()

	for i, field := range builder.Fields() {
		if columns != nil && !slices.Contains(columns, schema.Field(i).Name) {
			// fill empty values for unselected columns
			field.AppendEmptyValues(len(data))
			continue
		}
		for _, row := range data {
			switch v := row[i].(type) {
			case int64:
				field.(*array.Int64Builder).Append(v)
			case string:
				field.(*array.StringBuilder).Append(v)
			}
		}
	}

	return builder.NewRecordBatch()
}

func getColumnNames(opts *catalog.ScanOptions) []string {
	if opts == nil || len(opts.Columns) == 0 {
		return []string{"*"}
	}
	return opts.Columns
}
