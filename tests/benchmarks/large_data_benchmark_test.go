// Package benchmarks provides performance benchmarks for the airport-go Flight server.
// This file contains real-world benchmarks with large datasets (10M rows).
package benchmarks

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"

	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	// Default row count for large data benchmarks
	defaultLargeRowCount = 10_000_000
	// Batch size for generating data
	batchSize = 100_000
)

// =============================================================================
// Large Data Benchmark: Real-world performance testing
// =============================================================================

// BenchmarkLargeDataTransfer benchmarks transferring 10M rows through Flight protocol.
// This measures end-to-end data transfer performance.
func BenchmarkLargeDataTransfer(b *testing.B) {
	rowCount := defaultLargeRowCount

	// Create a table that generates data on-the-fly
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "category", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	table := &largeDataTable{
		name:     "large_data",
		schema:   schema,
		rowCount: rowCount,
	}

	cat, _ := airport.NewCatalogBuilder().
		Schema("bench").
		Table(table).
		Build()

	server := newLargeBenchServer(b, cat)
	defer server.stop()

	db := openDuckDB(b)
	defer db.Close()

	attachServer(b, db, server.address)

	// Warm-up run
	warmupQuery := "SELECT COUNT(*) FROM bench_cat.bench.large_data"
	row := db.QueryRow(warmupQuery)
	var count int64
	if err := row.Scan(&count); err != nil {
		b.Fatalf("Warmup failed: %v", err)
	}
	if count != int64(rowCount) {
		b.Fatalf("Expected %d rows, got %d", rowCount, count)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM bench_cat.bench.large_data")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		transferredRows := int64(0)
		for rows.Next() {
			var id int64
			var value float64
			var category int32
			var name string
			if err := rows.Scan(&id, &value, &category, &name); err != nil {
				rows.Close()
				b.Fatalf("Scan failed: %v", err)
			}
			transferredRows++
		}
		rows.Close()

		if transferredRows != int64(rowCount) {
			b.Fatalf("Expected %d rows, got %d", rowCount, transferredRows)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(rowCount), "rows/op")
	b.ReportMetric(float64(rowCount)/b.Elapsed().Seconds()*float64(b.N), "rows/sec")
}

// BenchmarkLargeDataAggregation benchmarks aggregating 10M rows.
// This measures query performance with server-side aggregation potential.
func BenchmarkLargeDataAggregation(b *testing.B) {
	rowCount := defaultLargeRowCount

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "category", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	table := &largeDataTable{
		name:     "large_data",
		schema:   schema,
		rowCount: rowCount,
	}

	cat, _ := airport.NewCatalogBuilder().
		Schema("bench").
		Table(table).
		Build()

	server := newLargeBenchServer(b, cat)
	defer server.stop()

	db := openDuckDB(b)
	defer db.Close()

	attachServer(b, db, server.address)

	// Test different aggregation queries
	queries := []struct {
		name  string
		query string
	}{
		{"count", "SELECT COUNT(*) FROM bench_cat.bench.large_data"},
		{"sum", "SELECT SUM(value) FROM bench_cat.bench.large_data"},
		{"avg", "SELECT AVG(value) FROM bench_cat.bench.large_data"},
		{"group_by", "SELECT category, COUNT(*), SUM(value), AVG(value) FROM bench_cat.bench.large_data GROUP BY category"},
		{"min_max", "SELECT MIN(value), MAX(value), MIN(id), MAX(id) FROM bench_cat.bench.large_data"},
	}

	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			// Warm-up
			_, err := db.Exec(q.query)
			if err != nil {
				b.Fatalf("Warmup failed: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.Query(q.query)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
				for rows.Next() {
					// Consume results
				}
				rows.Close()
			}

			b.StopTimer()
			b.ReportMetric(float64(rowCount), "rows_processed/op")
		})
	}
}

// BenchmarkCTASLargeData benchmarks CREATE TABLE AS SELECT with 10M rows.
// This tests the dynamic catalog's ability to handle large data insertions.
func BenchmarkCTASLargeData(b *testing.B) {
	rowCount := defaultLargeRowCount

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "category", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	// Create source table
	sourceTable := &largeDataTable{
		name:     "source_data",
		schema:   schema,
		rowCount: rowCount,
	}

	// Create dynamic catalog for CTAS target
	dynCatalog := newBenchDynamicCatalog()
	dynCatalog.createSchema("bench")

	// Add source table to a static schema
	cat, _ := airport.NewCatalogBuilder().
		Schema("source").
		Table(sourceTable).
		Build()

	// Create combined catalog
	combinedCat := &combinedCatalog{
		static:  cat,
		dynamic: dynCatalog,
	}

	server := newLargeBenchServer(b, combinedCat)
	defer server.stop()

	db := openDuckDB(b)
	defer db.Close()

	attachServer(b, db, server.address)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("target_%d", i)

		// Create table as select
		start := time.Now()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE bench_cat.bench.%s AS SELECT * FROM bench_cat.source.source_data",
			tableName,
		))
		elapsed := time.Since(start)

		if err != nil {
			b.Fatalf("CTAS failed: %v", err)
		}

		b.ReportMetric(float64(rowCount)/elapsed.Seconds(), "rows/sec")

		// Verify row count
		row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM bench_cat.bench.%s", tableName))
		var count int64
		if err := row.Scan(&count); err != nil {
			b.Fatalf("Count failed: %v", err)
		}
		if count != int64(rowCount) {
			b.Fatalf("Expected %d rows, got %d", rowCount, count)
		}

		// Clean up for next iteration
		dynCatalog.dropTable("bench", tableName)
	}

	b.StopTimer()
	b.ReportMetric(float64(rowCount), "rows/op")
}

// BenchmarkColumnProjection benchmarks reading only specific columns from 10M rows.
func BenchmarkColumnProjection(b *testing.B) {
	rowCount := defaultLargeRowCount

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "category", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "extra1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "extra2", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "extra3", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	table := &largeDataTable{
		name:     "wide_table",
		schema:   schema,
		rowCount: rowCount,
	}

	cat, _ := airport.NewCatalogBuilder().
		Schema("bench").
		Table(table).
		Build()

	server := newLargeBenchServer(b, cat)
	defer server.stop()

	db := openDuckDB(b)
	defer db.Close()

	attachServer(b, db, server.address)

	projections := []struct {
		name    string
		columns string
	}{
		{"single_column", "id"},
		{"two_columns", "id, value"},
		{"three_columns", "id, value, category"},
		{"all_columns", "*"},
	}

	for _, p := range projections {
		b.Run(p.name, func(b *testing.B) {
			query := fmt.Sprintf("SELECT %s FROM bench_cat.bench.wide_table", p.columns)

			// Warm-up
			rows, _ := db.Query(query)
			for rows.Next() {
			}
			rows.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}

				count := 0
				for rows.Next() {
					count++
				}
				rows.Close()

				if count != rowCount {
					b.Fatalf("Expected %d rows, got %d", rowCount, count)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(rowCount), "rows/op")
		})
	}
}

// =============================================================================
// Helper types and functions for large data benchmarks
// =============================================================================

// largeDataTable generates large amounts of test data on-the-fly.
type largeDataTable struct {
	name     string
	schema   *arrow.Schema
	rowCount int
}

func (t *largeDataTable) Name() string    { return t.name }
func (t *largeDataTable) Comment() string { return fmt.Sprintf("Large data table with %d rows", t.rowCount) }

func (t *largeDataTable) ArrowSchema(cols []string) *arrow.Schema {
	if len(cols) == 0 {
		return t.schema
	}
	return catalog.ProjectSchema(t.schema, cols)
}

func (t *largeDataTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	// Server requires full schema - DuckDB handles column projection client-side
	// Create a streaming reader that generates data in batches
	return &largeDataReader{
		schema:     t.schema,
		rowCount:   t.rowCount,
		batchSize:  batchSize,
		currentRow: 0,
		allocator:  memory.DefaultAllocator,
		ctx:        ctx,
	}, nil
}

// largeDataReader generates data in batches to avoid memory issues.
type largeDataReader struct {
	schema     *arrow.Schema
	rowCount   int
	batchSize  int
	currentRow int
	allocator  memory.Allocator
	ctx        context.Context
	current    arrow.RecordBatch
}

func (r *largeDataReader) Schema() *arrow.Schema           { return r.schema }
func (r *largeDataReader) Record() arrow.RecordBatch       { return r.current }
func (r *largeDataReader) RecordBatch() arrow.RecordBatch  { return r.current }
func (r *largeDataReader) Err() error                 { return nil }
func (r *largeDataReader) Retain()                    {}
func (r *largeDataReader) Release() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *largeDataReader) Next() bool {
	// Check context cancellation
	select {
	case <-r.ctx.Done():
		return false
	default:
	}

	if r.currentRow >= r.rowCount {
		return false
	}

	// Release previous record
	if r.current != nil {
		r.current.Release()
	}

	// Calculate batch size
	remaining := r.rowCount - r.currentRow
	currentBatchSize := r.batchSize
	if remaining < currentBatchSize {
		currentBatchSize = remaining
	}

	// Build record with all columns (full schema)
	builder := array.NewRecordBuilder(r.allocator, r.schema)
	defer builder.Release()

	// Generate data for each column in the schema
	for fieldIdx, field := range r.schema.Fields() {
		startRow := r.currentRow
		switch field.Name {
		case "id":
			b := builder.Field(fieldIdx).(*array.Int64Builder)
			for i := 0; i < currentBatchSize; i++ {
				b.Append(int64(startRow + i))
			}
		case "value":
			b := builder.Field(fieldIdx).(*array.Float64Builder)
			for i := 0; i < currentBatchSize; i++ {
				b.Append(float64(startRow+i) * 1.5)
			}
		case "category":
			b := builder.Field(fieldIdx).(*array.Int32Builder)
			for i := 0; i < currentBatchSize; i++ {
				b.Append(int32((startRow + i) % 100))
			}
		case "name":
			b := builder.Field(fieldIdx).(*array.StringBuilder)
			for i := 0; i < currentBatchSize; i++ {
				b.Append(fmt.Sprintf("item_%d", startRow+i))
			}
		case "extra1", "extra2", "extra3":
			b := builder.Field(fieldIdx).(*array.StringBuilder)
			for i := 0; i < currentBatchSize; i++ {
				if (startRow+i)%10 == 0 {
					b.AppendNull()
				} else {
					b.Append(fmt.Sprintf("%s_value_%d", field.Name, startRow+i))
				}
			}
		}
	}

	r.current = builder.NewRecordBatch()
	r.currentRow += currentBatchSize
	return true
}

// newLargeBenchServer creates a server for large data benchmarks.
func newLargeBenchServer(b *testing.B, cat catalog.Catalog) *benchServer {
	b.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to create listener: %v", err)
	}

	// Configure server with minimal logging and larger message size
	warnLevel := slog.LevelWarn
	config := airport.ServerConfig{
		Catalog:        cat,
		Address:        lis.Addr().String(),
		LogLevel:       &warnLevel,
		MaxMessageSize: 64 * 1024 * 1024, // 64MB for large batches
	}

	opts := airport.ServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	if err := airport.NewServer(grpcServer, config); err != nil {
		b.Fatalf("Failed to register server: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return &benchServer{
		grpcServer: grpcServer,
		listener:   lis,
		address:    lis.Addr().String(),
	}
}

// =============================================================================
// Dynamic catalog for CTAS benchmarks
// =============================================================================

type benchDynamicCatalog struct {
	mu      sync.RWMutex
	schemas map[string]*benchDynamicSchema
}

func newBenchDynamicCatalog() *benchDynamicCatalog {
	return &benchDynamicCatalog{
		schemas: make(map[string]*benchDynamicSchema),
	}
}

func (c *benchDynamicCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	schemas := make([]catalog.Schema, 0, len(c.schemas))
	for _, s := range c.schemas {
		schemas = append(schemas, s)
	}
	return schemas, nil
}

func (c *benchDynamicCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if s, ok := c.schemas[name]; ok {
		return s, nil
	}
	return nil, nil
}

func (c *benchDynamicCatalog) createSchema(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[name] = &benchDynamicSchema{
		name:   name,
		tables: make(map[string]*benchDynamicTable),
	}
}

func (c *benchDynamicCatalog) dropTable(schemaName, tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.schemas[schemaName]; ok {
		delete(s.tables, tableName)
	}
}

func (c *benchDynamicCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.schemas[name]; exists {
		return nil, catalog.ErrAlreadyExists
	}
	schema := &benchDynamicSchema{
		name:   name,
		tables: make(map[string]*benchDynamicTable),
	}
	c.schemas[name] = schema
	return schema, nil
}

func (c *benchDynamicCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, name)
	return nil
}

type benchDynamicSchema struct {
	mu     sync.RWMutex
	name   string
	tables map[string]*benchDynamicTable
}

func (s *benchDynamicSchema) Name() string    { return s.name }
func (s *benchDynamicSchema) Comment() string { return "" }

func (s *benchDynamicSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tables := make([]catalog.Table, 0, len(s.tables))
	for _, t := range s.tables {
		tables = append(tables, t)
	}
	return tables, nil
}

func (s *benchDynamicSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.tables[name]; ok {
		return t, nil
	}
	return nil, nil
}

func (s *benchDynamicSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}
func (s *benchDynamicSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}
func (s *benchDynamicSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
	return nil, nil
}

func (s *benchDynamicSchema) CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts catalog.CreateTableOptions) (catalog.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	table := &benchDynamicTable{
		name:   name,
		schema: schema,
	}
	s.tables[name] = table
	return table, nil
}

func (s *benchDynamicSchema) DropTable(ctx context.Context, name string, opts catalog.DropTableOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, name)
	return nil
}

func (s *benchDynamicSchema) RenameTable(ctx context.Context, oldName, newName string, opts catalog.RenameTableOptions) error {
	return nil
}

type benchDynamicTable struct {
	mu      sync.RWMutex
	name    string
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (t *benchDynamicTable) Name() string    { return t.name }
func (t *benchDynamicTable) Comment() string { return "" }

func (t *benchDynamicTable) ArrowSchema(cols []string) *arrow.Schema {
	if len(cols) == 0 {
		return t.schema
	}
	return catalog.ProjectSchema(t.schema, cols)
}

func (t *benchDynamicTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	records := make([]arrow.RecordBatch, len(t.records))
	for i, rec := range t.records {
		rec.Retain()
		records[i] = rec
	}
	return array.NewRecordReader(t.schema, records)
}

func (t *benchDynamicTable) Insert(ctx context.Context, rows array.RecordReader) (*catalog.DMLResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var count int64
	for rows.Next() {
		rec := rows.RecordBatch()
		rec.Retain()
		t.records = append(t.records, rec)
		count += rec.NumRows()
	}
	return &catalog.DMLResult{AffectedRows: count}, nil
}

// combinedCatalog combines static and dynamic catalogs.
type combinedCatalog struct {
	static  catalog.Catalog
	dynamic *benchDynamicCatalog
}

func (c *combinedCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	staticSchemas, _ := c.static.Schemas(ctx)
	dynamicSchemas, _ := c.dynamic.Schemas(ctx)
	return append(staticSchemas, dynamicSchemas...), nil
}

func (c *combinedCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	if s, _ := c.static.Schema(ctx, name); s != nil {
		return s, nil
	}
	return c.dynamic.Schema(ctx, name)
}

func (c *combinedCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
	return c.dynamic.CreateSchema(ctx, name, opts)
}

func (c *combinedCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
	return c.dynamic.DropSchema(ctx, name, opts)
}
