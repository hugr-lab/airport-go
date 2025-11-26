package airport

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
)

// TestMemoryLeaks uses memory.NewCheckedAllocator to detect memory leaks.
// This test ensures that all Arrow objects are properly released.
func TestMemoryLeaks(t *testing.T) {
	// Create checked allocator that tracks allocations
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0) // Verify no leaks at end

	// Test 1: Building a catalog should not leak
	t.Run("CatalogBuilder", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
			builder := array.NewRecordBuilder(allocator, schema)
			defer builder.Release()
			record := builder.NewRecord()
			defer record.Release()
			return array.NewRecordReader(schema, []arrow.Record{record})
		}

		cat, err := NewCatalogBuilder().
			Schema("test").
			SimpleTable(SimpleTableDef{
				Name:     "table1",
				Schema:   schema,
				ScanFunc: scanFunc,
			}).
			Build()

		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		// Use the catalog
		ctx := context.Background()
		schemas, err := cat.Schemas(ctx)
		if err != nil {
			t.Fatalf("Schemas failed: %v", err)
		}

		if len(schemas) != 1 {
			t.Errorf("Expected 1 schema, got %d", len(schemas))
		}

		// All Arrow objects should be released by now
	})

	// Test 2: Scanning a table should not leak
	t.Run("TableScan", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

		scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
			builder := array.NewRecordBuilder(allocator, schema)
			defer builder.Release()

			// Build some test data
			builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
			builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)

			record := builder.NewRecord()
			defer record.Release()

			return array.NewRecordReader(schema, []arrow.Record{record})
		}

		cat, err := NewCatalogBuilder().
			Schema("test").
			SimpleTable(SimpleTableDef{
				Name:     "users",
				Schema:   schema,
				ScanFunc: scanFunc,
			}).
			Build()

		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		// Scan the table
		ctx := context.Background()
		testSchema, err := cat.Schema(ctx, "test")
		if err != nil {
			t.Fatalf("Schema failed: %v", err)
		}

		table, err := testSchema.Table(ctx, "users")
		if err != nil {
			t.Fatalf("Table failed: %v", err)
		}

		reader, err := table.Scan(ctx, &catalog.ScanOptions{})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		defer reader.Release()

		// Read all records
		for reader.Next() {
			record := reader.Record()
			// Don't need to release - reader owns it
			if record.NumRows() != 3 {
				t.Errorf("Expected 3 rows, got %d", record.NumRows())
			}
		}

		if err := reader.Err(); err != nil {
			t.Fatalf("Reader error: %v", err)
		}
	})

	// Test 3: Multiple scans should not accumulate leaks
	t.Run("MultipleScans", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
			builder := array.NewRecordBuilder(allocator, schema)
			defer builder.Release()
			builder.Field(0).(*array.Int64Builder).AppendValues([]int64{42}, nil)
			record := builder.NewRecord()
			defer record.Release()
			return array.NewRecordReader(schema, []arrow.Record{record})
		}

		cat, err := NewCatalogBuilder().
			Schema("test").
			SimpleTable(SimpleTableDef{
				Name:     "data",
				Schema:   schema,
				ScanFunc: scanFunc,
			}).
			Build()

		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		ctx := context.Background()
		testSchema, _ := cat.Schema(ctx, "test")
		table, _ := testSchema.Table(ctx, "data")

		// Perform multiple scans
		for i := 0; i < 10; i++ {
			reader, err := table.Scan(ctx, &catalog.ScanOptions{})
			if err != nil {
				t.Fatalf("Scan %d failed: %v", i, err)
			}

			for reader.Next() {
				_ = reader.Record()
			}

			reader.Release()
		}

		// Memory should be back to baseline
	})

	// Test 4: Building and releasing records
	t.Run("RecordBuilding", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Float64},
			{Name: "y", Type: arrow.PrimitiveTypes.Float64},
		}, nil)

		// Build multiple records
		for i := 0; i < 5; i++ {
			builder := array.NewRecordBuilder(allocator, schema)

			builder.Field(0).(*array.Float64Builder).AppendValues([]float64{1.0, 2.0}, nil)
			builder.Field(1).(*array.Float64Builder).AppendValues([]float64{3.0, 4.0}, nil)

			record := builder.NewRecord()

			// Verify record
			if record.NumRows() != 2 {
				t.Errorf("Expected 2 rows, got %d", record.NumRows())
			}

			// Clean up
			record.Release()
			builder.Release()
		}
	})

	// Test 5: Array building
	t.Run("ArrayBuilding", func(t *testing.T) {
		// Build various array types
		intBuilder := array.NewInt64Builder(allocator)
		intBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		intArray := intBuilder.NewInt64Array()
		intArray.Release()
		intBuilder.Release()

		strBuilder := array.NewStringBuilder(allocator)
		strBuilder.AppendValues([]string{"hello", "world"}, nil)
		strArray := strBuilder.NewStringArray()
		strArray.Release()
		strBuilder.Release()

		floatBuilder := array.NewFloat64Builder(allocator)
		floatBuilder.AppendValues([]float64{1.1, 2.2, 3.3}, nil)
		floatArray := floatBuilder.NewFloat64Array()
		floatArray.Release()
		floatBuilder.Release()
	})
}

// TestMemoryLeaksInConcurrentScans tests that concurrent scans don't leak memory.
func TestMemoryLeaksInConcurrentScans(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(allocator, schema)
		defer builder.Release()
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		record := builder.NewRecord()
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.Record{record})
	}

	cat, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "data",
			Schema:   schema,
			ScanFunc: scanFunc,
		}).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	testSchema, _ := cat.Schema(ctx, "test")
	table, _ := testSchema.Table(ctx, "data")

	// Concurrent scans
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			reader, err := table.Scan(ctx, &catalog.ScanOptions{})
			if err != nil {
				t.Errorf("Scan failed: %v", err)
				done <- false
				return
			}
			defer reader.Release()

			for reader.Next() {
				_ = reader.Record()
			}

			done <- true
		}()
	}

	// Wait for all to complete
	for i := 0; i < 5; i++ {
		<-done
	}
}

// TestNoMemoryLeaksWithErrors tests that memory is released even when errors occur.
func TestNoMemoryLeaksWithErrors(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create a scan function that sometimes fails
	callCount := 0
	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		callCount++
		builder := array.NewRecordBuilder(allocator, schema)
		defer builder.Release()

		if callCount%2 == 0 {
			// Even calls fail - but still clean up
			return nil, context.Canceled
		}

		// Odd calls succeed
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
		record := builder.NewRecord()
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.Record{record})
	}

	cat, _ := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "data",
			Schema:   schema,
			ScanFunc: scanFunc,
		}).
		Build()

	ctx := context.Background()
	testSchema, _ := cat.Schema(ctx, "test")
	table, _ := testSchema.Table(ctx, "data")

	// Try multiple scans - some will fail
	for i := 0; i < 4; i++ {
		reader, err := table.Scan(ctx, &catalog.ScanOptions{})
		if err != nil {
			// Expected for even calls
			continue
		}

		// For successful scans, read and release
		for reader.Next() {
			_ = reader.Record()
		}
		reader.Release()
	}

	// All memory should be released despite errors
}
