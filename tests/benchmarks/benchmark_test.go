// Package benchmarks provides performance benchmarks for the airport-go Flight server.
// These benchmarks use DuckDB's Airport extension as the Flight client to measure
// end-to-end performance through the Flight protocol.
package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/serialize"

	_ "github.com/duckdb/duckdb-go/v2"
)

// benchServer wraps a Flight server for benchmarking.
type benchServer struct {
	grpcServer *grpc.Server
	listener   net.Listener
	address    string
}

// newBenchServer creates and starts a benchmark Flight server.
func newBenchServer(b *testing.B, cat catalog.Catalog) *benchServer {
	b.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to create listener: %v", err)
	}

	// Configure server with minimal logging for benchmarks
	warnLevel := slog.LevelWarn
	config := airport.ServerConfig{
		Catalog:  cat,
		Address:  lis.Addr().String(),
		LogLevel: &warnLevel,
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

func (s *benchServer) stop() {
	s.grpcServer.GracefulStop()
	s.listener.Close()
}

// openDuckDB opens a DuckDB connection with the Airport extension loaded.
func openDuckDB(b *testing.B) *sql.DB {
	b.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("DuckDB not available: %v", err)
	}

	_, err = db.Exec("INSTALL airport FROM community")
	if err != nil {
		b.Fatalf("Airport extension not available: %v", err)
	}

	_, err = db.Exec("LOAD airport")
	if err != nil {
		b.Fatalf("Failed to load Airport extension: %v", err)
	}

	return db
}

// attachServer attaches a Flight server to DuckDB as a catalog.
func attachServer(b *testing.B, db *sql.DB, address, name string) {
	b.Helper()

	query := fmt.Sprintf("ATTACH '' AS %s (TYPE airport, LOCATION 'grpc://%s')", name, address)
	_, err := db.Exec(query)
	if err != nil {
		b.Fatalf("Failed to attach server: %v", err)
	}
}

// BenchmarkCatalogSerialization benchmarks catalog metadata serialization.
// This tests the internal serialization performance.
func BenchmarkCatalogSerialization(b *testing.B) {
	schemas := make([]catalog.Schema, 0, 10)
	for i := 0; i < 10; i++ {
		tables := make([]catalog.Table, 0, 5)
		for j := 0; j < 5; j++ {
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
				{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			}, nil)

			table := &benchTable{
				name:   fmt.Sprintf("table_%c", 'a'+j),
				schema: arrowSchema,
			}
			tables = append(tables, table)
		}

		schema := &benchSchema{
			name:   fmt.Sprintf("schema_%d", i),
			tables: tables,
		}
		schemas = append(schemas, schema)
	}

	cat := &benchCatalog{schemas: schemas}
	ctx := context.Background()
	allocator := memory.DefaultAllocator

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data, err := serialize.SerializeCatalog(ctx, cat, allocator)
		if err != nil {
			b.Fatalf("Serialization failed: %v", err)
		}
		_ = data
	}

	b.StopTimer()
	data, _ := serialize.SerializeCatalog(ctx, cat, allocator)
	b.ReportMetric(float64(len(data)), "bytes")
}

// BenchmarkTableScan benchmarks scanning a table through the Flight protocol.
// This measures end-to-end query performance using DuckDB as client.
func BenchmarkTableScan(b *testing.B) {
	rowCounts := []int{100, 1000, 10000}

	for _, rows := range rowCounts {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			}, nil)

			rowCount := rows
			scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
				builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
				defer builder.Release()

				ids := make([]int64, rowCount)
				values := make([]float64, rowCount)
				for i := 0; i < rowCount; i++ {
					ids[i] = int64(i)
					values[i] = float64(i) * 1.5
				}

				builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
				builder.Field(1).(*array.Float64Builder).AppendValues(values, nil)

				record := builder.NewRecordBatch()
				defer record.Release()

				return array.NewRecordReader(schema, []arrow.RecordBatch{record})
			}

			cat, _ := airport.NewCatalogBuilder().
				Schema("bench").
				SimpleTable(airport.SimpleTableDef{
					Name:     "data",
					Schema:   schema,
					ScanFunc: scanFunc,
				}).
				Build()

			server := newBenchServer(b, cat)
			defer server.stop()

			db := openDuckDB(b)
			defer db.Close()

			attachServer(b, db, server.address, "bench_cat")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.Query("SELECT * FROM bench_cat.bench.data")
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
			b.ReportMetric(float64(rowCount), "rows/scan")
		})
	}
}

// BenchmarkCatalogBuilder benchmarks building catalogs of various sizes.
func BenchmarkCatalogBuilder(b *testing.B) {
	tableCounts := []int{1, 10, 100}

	for _, tableCount := range tableCounts {
		b.Run(fmt.Sprintf("tables_%d", tableCount), func(b *testing.B) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			}, nil)

			scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
				builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
				defer builder.Release()
				record := builder.NewRecordBatch()
				defer record.Release()
				return array.NewRecordReader(schema, []arrow.RecordBatch{record})
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				builder := airport.NewCatalogBuilder().Schema("test")

				for j := 0; j < tableCount; j++ {
					builder.SimpleTable(airport.SimpleTableDef{
						Name:     fmt.Sprintf("table_%d", j),
						Schema:   schema,
						ScanFunc: scanFunc,
					})
				}

				cat, err := builder.Build()
				if err != nil {
					b.Fatalf("Build failed: %v", err)
				}
				_ = cat
			}

			b.StopTimer()
			b.ReportMetric(float64(tableCount), "tables")
		})
	}
}

// BenchmarkRecordBuilding benchmarks Arrow record construction.
func BenchmarkRecordBuilding(b *testing.B) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int_col", Type: arrow.PrimitiveTypes.Int64},
		{Name: "str_col", Type: arrow.BinaryTypes.String},
		{Name: "float_col", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	allocator := memory.DefaultAllocator

	intData := make([]int64, 1000)
	strData := make([]string, 1000)
	floatData := make([]float64, 1000)

	for i := 0; i < 1000; i++ {
		intData[i] = int64(i)
		strData[i] = fmt.Sprintf("value_%c", 'a'+(i%26))
		floatData[i] = float64(i) * 1.1
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		builder := array.NewRecordBuilder(allocator, schema)

		builder.Field(0).(*array.Int64Builder).AppendValues(intData, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues(strData, nil)
		builder.Field(2).(*array.Float64Builder).AppendValues(floatData, nil)

		record := builder.NewRecordBatch()
		record.Release()
		builder.Release()
	}

	b.StopTimer()
	b.ReportMetric(1000, "rows/record")
}

// BenchmarkConcurrentScans benchmarks concurrent table scans through Flight.
func BenchmarkConcurrentScans(b *testing.B) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()

		ids := make([]int64, 100)
		for i := 0; i < 100; i++ {
			ids[i] = int64(i)
		}
		builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	}

	cat, _ := airport.NewCatalogBuilder().
		Schema("bench").
		SimpleTable(airport.SimpleTableDef{
			Name:     "data",
			Schema:   schema,
			ScanFunc: scanFunc,
		}).
		Build()

	server := newBenchServer(b, cat)
	defer server.stop()

	db := openDuckDB(b)
	defer db.Close()

	attachServer(b, db, server.address, "bench_cat")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rows, err := db.Query("SELECT * FROM bench_cat.bench.data")
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			for rows.Next() {
				var id int64
				rows.Scan(&id)
			}
			rows.Close()
		}
	})
}

// Benchmark helper types
type benchCatalog struct {
	schemas []catalog.Schema
}

func (c *benchCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	return c.schemas, nil
}

func (c *benchCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	for _, s := range c.schemas {
		if s.Name() == name {
			return s, nil
		}
	}
	return nil, nil
}

type benchSchema struct {
	name   string
	tables []catalog.Table
}

func (s *benchSchema) Name() string {
	return s.name
}

func (s *benchSchema) Comment() string {
	return ""
}

func (s *benchSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	return s.tables, nil
}

func (s *benchSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	for _, t := range s.tables {
		if t.Name() == name {
			return t, nil
		}
	}
	return nil, nil
}

func (s *benchSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

func (s *benchSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

func (s *benchSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
	return nil, nil
}

type benchTable struct {
	name   string
	schema *arrow.Schema
}

func (t *benchTable) Name() string {
	return t.name
}

func (t *benchTable) Comment() string {
	return ""
}

func (t *benchTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *benchTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}
