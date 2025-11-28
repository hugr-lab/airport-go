package airport

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/serialize"
)

// BenchmarkCatalogSerialization benchmarks catalog metadata serialization.
func BenchmarkCatalogSerialization(b *testing.B) {
	// Create a moderately complex catalog
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
				name:   "table_" + string(rune('a'+j)),
				schema: arrowSchema,
			}
			tables = append(tables, table)
		}

		schema := &benchSchema{
			name:   "schema_" + string(rune('0'+i)),
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
	// Report compression ratio
	data, _ := serialize.SerializeCatalog(ctx, cat, allocator)
	b.ReportMetric(float64(len(data)), "bytes")
}

// BenchmarkTableScan benchmarks scanning a table with varying row counts.
func BenchmarkTableScan(b *testing.B) {
	rowCounts := []int{100, 1000, 10000}

	for _, rows := range rowCounts {
		b.Run("rows_"+string(rune('0'+rows/100)), func(b *testing.B) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			}, nil)

			scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
				builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
				defer builder.Release()

				ids := make([]int64, rows)
				values := make([]float64, rows)
				for i := 0; i < rows; i++ {
					ids[i] = int64(i)
					values[i] = float64(i) * 1.5
				}

				builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
				builder.Field(1).(*array.Float64Builder).AppendValues(values, nil)

				record := builder.NewRecordBatch()
				defer record.Release()

				return array.NewRecordReader(schema, []arrow.RecordBatch{record})
			}

			cat, _ := NewCatalogBuilder().
				Schema("bench").
				SimpleTable(SimpleTableDef{
					Name:     "data",
					Schema:   schema,
					ScanFunc: scanFunc,
				}).
				Build()

			ctx := context.Background()
			testSchema, _ := cat.Schema(ctx, "bench")
			table, _ := testSchema.Table(ctx, "data")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader, err := table.Scan(ctx, &catalog.ScanOptions{})
				if err != nil {
					b.Fatalf("Scan failed: %v", err)
				}

				rowCount := int64(0)
				for reader.Next() {
					rowCount += reader.RecordBatch().NumRows()
				}

				reader.Release()

				if rowCount != int64(rows) {
					b.Fatalf("Expected %d rows, got %d", rows, rowCount)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(rows), "rows/scan")
		})
	}
}

// BenchmarkCatalogBuilder benchmarks building catalogs of various sizes.
func BenchmarkCatalogBuilder(b *testing.B) {
	tableCounts := []int{1, 10, 100}

	for _, tableCount := range tableCounts {
		b.Run("tables_"+string(rune('0'+tableCount/10)), func(b *testing.B) {
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
				builder := NewCatalogBuilder().Schema("test")

				for j := 0; j < tableCount; j++ {
					builder.SimpleTable(SimpleTableDef{
						Name:     "table_" + string(rune('a'+j)),
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

	// Pre-generate test data
	intData := make([]int64, 1000)
	strData := make([]string, 1000)
	floatData := make([]float64, 1000)

	for i := 0; i < 1000; i++ {
		intData[i] = int64(i)
		strData[i] = "value_" + string(rune('a'+(i%26)))
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

// BenchmarkConcurrentScans benchmarks concurrent table scans.
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

	cat, _ := NewCatalogBuilder().
		Schema("bench").
		SimpleTable(SimpleTableDef{
			Name:     "data",
			Schema:   schema,
			ScanFunc: scanFunc,
		}).
		Build()

	ctx := context.Background()
	testSchema, _ := cat.Schema(ctx, "bench")
	table, _ := testSchema.Table(ctx, "data")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reader, err := table.Scan(ctx, &catalog.ScanOptions{})
			if err != nil {
				b.Fatalf("Scan failed: %v", err)
			}

			for reader.Next() {
				_ = reader.RecordBatch()
			}

			reader.Release()
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

func (t *benchTable) ArrowSchema() *arrow.Schema {
	return t.schema
}

func (t *benchTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}
