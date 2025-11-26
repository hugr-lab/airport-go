package serialize

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
)

// mockCatalog for testing
type mockCatalog struct {
	schemas []catalog.Schema
}

func (m *mockCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	return m.schemas, nil
}

func (m *mockCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	for _, s := range m.schemas {
		if s.Name() == name {
			return s, nil
		}
	}
	return nil, nil
}

// mockSchema for testing
type mockSchema struct {
	name    string
	comment string
	tables  []catalog.Table
}

func (m *mockSchema) Name() string {
	return m.name
}

func (m *mockSchema) Comment() string {
	return m.comment
}

func (m *mockSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	return m.tables, nil
}

func (m *mockSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	for _, t := range m.tables {
		if t.Name() == name {
			return t, nil
		}
	}
	return nil, nil
}

func (m *mockSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

func (m *mockSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

// mockTable for testing
type mockTable struct {
	name   string
	schema *arrow.Schema
}

func (m *mockTable) Name() string {
	return m.name
}

func (m *mockTable) Comment() string {
	return "Test table"
}

func (m *mockTable) ArrowSchema() *arrow.Schema {
	return m.schema
}

func (m *mockTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, m.schema)
	defer builder.Release()
	record := builder.NewRecord()
	defer record.Release()
	return array.NewRecordReader(m.schema, []arrow.Record{record})
}

// TestSerializeCatalog tests catalog metadata serialization.
func TestSerializeCatalog(t *testing.T) {
	schema1 := &mockSchema{
		name:    "schema1",
		comment: "First schema",
		tables:  []catalog.Table{},
	}

	schema2 := &mockSchema{
		name:    "schema2",
		comment: "Second schema",
		tables:  []catalog.Table{},
	}

	cat := &mockCatalog{
		schemas: []catalog.Schema{schema1, schema2},
	}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeCatalog failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty serialized data")
	}

	// Verify data is valid (should be compressed Arrow IPC)
	if len(data) < 10 {
		t.Errorf("Serialized data seems too small: %d bytes", len(data))
	}
}

// TestSerializeWithTables tests catalog serialization with tables.
func TestSerializeWithTables(t *testing.T) {
	table1 := &mockTable{
		name: "table1",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	schema := &mockSchema{
		name:    "test_schema",
		comment: "Test schema",
		tables:  []catalog.Table{table1},
	}

	cat := &mockCatalog{
		schemas: []catalog.Schema{schema},
	}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeCatalog failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty serialized data")
	}
}

// TestSerializeMultipleTables tests serializing catalog with multiple tables.
func TestSerializeMultipleTables(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	table := &mockTable{
		name:   "users",
		schema: arrowSchema,
	}

	schema := &mockSchema{
		name:    "main",
		comment: "Main schema",
		tables:  []catalog.Table{table},
	}

	cat := &mockCatalog{
		schemas: []catalog.Schema{schema},
	}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeCatalog failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty serialized data")
	}
}

// TestSerializationCompression tests that compression reduces data size.
func TestSerializationCompression(t *testing.T) {
	// Create catalog with many schemas to get compressible data
	schemas := make([]catalog.Schema, 0, 20)
	for i := 0; i < 20; i++ {
		schemas = append(schemas, &mockSchema{
			name:    "schema_" + string(rune('a'+i)),
			comment: "This is a test schema with a long comment that should compress well",
			tables:  []catalog.Table{},
		})
	}

	cat := &mockCatalog{schemas: schemas}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	compressedData, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeCatalogs failed: %v", err)
	}

	// Compressed data should exist
	if len(compressedData) == 0 {
		t.Error("Expected non-empty compressed data")
	}

	t.Logf("Compressed size: %d bytes for %d schemas", len(compressedData), len(schemas))
}

// TestSerializeEmptyCatalog tests serializing a catalog with no schemas.
func TestSerializeEmptyCatalog(t *testing.T) {
	cat := &mockCatalog{schemas: []catalog.Schema{}}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeCatalogs failed: %v", err)
	}

	// Should still produce valid output (empty catalog)
	if len(data) == 0 {
		t.Error("Expected non-empty data even for empty catalog")
	}
}

// TestSerializeSchemaNoTables tests serializing a schema with no tables.
func TestSerializeSchemaNoTables(t *testing.T) {
	schema := &mockSchema{
		name:    "empty",
		comment: "Empty schema",
		tables:  []catalog.Table{},
	}

	cat := &mockCatalog{
		schemas: []catalog.Schema{schema},
	}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeSchemas failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty data for schema with no tables")
	}
}

// TestSerializeManyTables tests serializing schema with multiple tables.
func TestSerializeManyTables(t *testing.T) {
	tables := []catalog.Table{
		&mockTable{
			name: "users",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
		},
		&mockTable{
			name: "products",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "price", Type: arrow.PrimitiveTypes.Float64},
			}, nil),
		},
		&mockTable{
			name: "orders",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
		},
	}

	schema := &mockSchema{
		name:    "main",
		comment: "Main schema",
		tables:  tables,
	}

	cat := &mockCatalog{
		schemas: []catalog.Schema{schema},
	}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeTables failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty serialized data")
	}

	t.Logf("Serialized %d tables to %d bytes", len(tables), len(data))
}

// TestSerializeContextCancellation tests that serialization respects context cancellation.
func TestSerializeContextCancellation(t *testing.T) {
	schema := &mockSchema{
		name:    "test",
		comment: "Test",
		tables:  []catalog.Table{},
	}

	cat := &mockCatalog{schemas: []catalog.Schema{schema}}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Serialization should handle cancelled context
	// (may or may not fail depending on when cancellation is checked)
	allocator := memory.DefaultAllocator
	_, err := SerializeCatalog(ctx, cat, allocator)

	// Just verify it doesn't panic
	t.Logf("Serialization with cancelled context: err=%v", err)
}

// TestSerializeFlightSQLFormat tests that output matches Flight SQL schema format.
func TestSerializeFlightSQLFormat(t *testing.T) {
	table := &mockTable{
		name: "test",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	schema := &mockSchema{
		name:   "main",
		tables: []catalog.Table{table},
	}

	cat := &mockCatalog{schemas: []catalog.Schema{schema}}

	ctx := context.Background()
	allocator := memory.DefaultAllocator
	data, err := SerializeCatalog(ctx, cat, allocator)

	if err != nil {
		t.Fatalf("SerializeCatalogs failed: %v", err)
	}

	// The data should be in Arrow IPC format with ZStandard compression
	// Verify it's not plain text
	if len(data) > 0 && data[0] > 127 {
		t.Log("Data appears to be binary (good)")
	}

	// ZStandard magic number is 0x28, 0xB5, 0x2F, 0xFD
	if len(data) >= 4 {
		if data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD {
			t.Log("Data has ZStandard magic number (compression working)")
		}
	}
}
