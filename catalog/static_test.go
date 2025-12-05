package catalog

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// testScanFunc creates a simple scan function for testing
func testScanFunc(schema *arrow.Schema) ScanFunc {
	return func(ctx context.Context, opts *ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		record := builder.NewRecordBatch()
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	}
}

// TestStaticCatalogSchemas tests retrieving schemas from static catalog.
func TestStaticCatalogSchemas(t *testing.T) {
	cat := NewStaticCatalog()

	cat.AddSchema("schema1", "First schema", make(map[string]Table), nil, nil, nil)
	cat.AddSchema("schema2", "Second schema", make(map[string]Table), nil, nil, nil)

	ctx := context.Background()
	schemas, err := cat.Schemas(ctx)
	if err != nil {
		t.Fatalf("Schemas() failed: %v", err)
	}

	if len(schemas) != 2 {
		t.Errorf("Expected 2 schemas, got %d", len(schemas))
	}
}

// TestStaticCatalogSchemaLookup tests looking up specific schemas.
func TestStaticCatalogSchemaLookup(t *testing.T) {
	cat := NewStaticCatalog()
	cat.AddSchema("test", "Test schema", make(map[string]Table), nil, nil, nil)

	ctx := context.Background()

	// Existing schema
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	if schema == nil {
		t.Fatal("Expected non-nil schema")
	}

	if schema.Name() != "test" {
		t.Errorf("Expected schema name 'test', got '%s'", schema.Name())
	}

	// Non-existent schema
	schema, err = cat.Schema(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Schema() failed for nonexistent: %v", err)
	}

	if schema != nil {
		t.Error("Expected nil for nonexistent schema")
	}
}

// TestStaticSchemaComments tests that schema comments are preserved.
func TestStaticSchemaComments(t *testing.T) {
	cat := NewStaticCatalog()
	cat.AddSchema("test", "This is a test comment", make(map[string]Table), nil, nil, nil)

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	if schema.Comment() != "This is a test comment" {
		t.Errorf("Expected comment 'This is a test comment', got '%s'", schema.Comment())
	}
}

// TestStaticSchemaTables tests retrieving tables from schema.
func TestStaticSchemaTables(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	table1 := NewStaticTable("table1", "First table", arrowSchema, testScanFunc(arrowSchema))
	table2 := NewStaticTable("table2", "Second table", arrowSchema, testScanFunc(arrowSchema))

	tables := map[string]Table{
		"table1": table1,
		"table2": table2,
	}

	cat := NewStaticCatalog()
	cat.AddSchema("test", "Test schema", tables, nil, nil, nil)

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	allTables, err := schema.Tables(ctx)
	if err != nil {
		t.Fatalf("Tables() failed: %v", err)
	}

	if len(allTables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(allTables))
	}
}

// TestStaticSchemaTableLookup tests looking up specific tables.
func TestStaticSchemaTableLookup(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	table1 := NewStaticTable("users", "Users table", arrowSchema, testScanFunc(arrowSchema))

	tables := map[string]Table{
		"users": table1,
	}

	cat := NewStaticCatalog()
	cat.AddSchema("test", "Test schema", tables, nil, nil, nil)

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	// Existing table
	table, err := schema.Table(ctx, "users")
	if err != nil {
		t.Fatalf("Table() failed: %v", err)
	}

	if table == nil {
		t.Fatal("Expected non-nil table")
	}

	if table.Name() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", table.Name())
	}

	// Non-existent table
	table, err = schema.Table(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Table() failed for nonexistent: %v", err)
	}

	if table != nil {
		t.Error("Expected nil for nonexistent table")
	}
}

// TestStaticTableProperties tests that table properties are correct.
func TestStaticTableProperties(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	table := NewStaticTable("users", "User accounts", arrowSchema, testScanFunc(arrowSchema))

	if table.Name() != "users" {
		t.Errorf("Expected name 'users', got '%s'", table.Name())
	}

	if table.Comment() != "User accounts" {
		t.Errorf("Expected comment 'User accounts', got '%s'", table.Comment())
	}

	if table.ArrowSchema(nil) != arrowSchema {
		t.Error("Arrow schema mismatch")
	}
}

// TestStaticTableScan tests scanning a table.
func TestStaticTableScan(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	table := NewStaticTable("test", "Test table", arrowSchema, testScanFunc(arrowSchema))

	ctx := context.Background()
	reader, err := table.Scan(ctx, &ScanOptions{})
	if err != nil {
		t.Fatalf("Scan() failed: %v", err)
	}
	defer reader.Release()

	if reader == nil {
		t.Fatal("Expected non-nil reader")
	}
}

// TestStaticSchemaFunctions tests scalar and table functions.
func TestStaticSchemaFunctions(t *testing.T) {
	mockScalar := &mockScalarFunc{name: "TEST_FUNC"}
	mockTable := &mockTableFunc{name: "TEST_TABLE_FUNC"}

	cat := NewStaticCatalog()
	cat.AddSchema("test", "Test schema", make(map[string]Table), []ScalarFunction{mockScalar}, []TableFunction{mockTable}, nil)

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	scalarFuncs, err := schema.ScalarFunctions(ctx)
	if err != nil {
		t.Fatalf("ScalarFunctions() failed: %v", err)
	}

	if len(scalarFuncs) != 1 {
		t.Errorf("Expected 1 scalar function, got %d", len(scalarFuncs))
	}

	tableFuncs, err := schema.TableFunctions(ctx)
	if err != nil {
		t.Fatalf("TableFunctions() failed: %v", err)
	}

	if len(tableFuncs) != 1 {
		t.Errorf("Expected 1 table function, got %d", len(tableFuncs))
	}
}

// TestStaticCatalogConcurrentAccess tests thread safety of static catalog.
func TestStaticCatalogConcurrentAccess(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	table1 := NewStaticTable("table1", "Test table", arrowSchema, testScanFunc(arrowSchema))
	tables := map[string]Table{
		"table1": table1,
	}

	cat := NewStaticCatalog()
	cat.AddSchema("test", "Test schema", tables, nil, nil, nil)

	ctx := context.Background()

	// Run concurrent reads
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Read schemas
			_, err := cat.Schemas(ctx)
			if err != nil {
				errors <- err
				return
			}

			// Read specific schema
			schema, err := cat.Schema(ctx, "test")
			if err != nil {
				errors <- err
				return
			}

			// Read tables
			_, err = schema.Tables(ctx)
			if err != nil {
				errors <- err
				return
			}

			// Read specific table
			_, err = schema.Table(ctx, "table1")
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}
}

// TestStaticCatalogContextCancellation tests that operations respect context cancellation.
func TestStaticCatalogContextCancellation(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create scan function that checks context
	scanFunc := func(ctx context.Context, opts *ScanOptions) (array.RecordReader, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer builder.Release()
			record := builder.NewRecordBatch()
			defer record.Release()
			return array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
		}
	}

	table1 := NewStaticTable("table1", "Test table", arrowSchema, scanFunc)
	tables := map[string]Table{
		"table1": table1,
	}

	cat := NewStaticCatalog()
	cat.AddSchema("test", "Test schema", tables, nil, nil, nil)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() should not fail on cancelled context: %v", err)
	}

	table, err := schema.Table(ctx, "table1")
	if err != nil {
		t.Fatalf("Table() should not fail on cancelled context: %v", err)
	}

	// Scan should respect cancellation
	_, err = table.Scan(ctx, &ScanOptions{})
	if err == nil {
		t.Error("Expected error from Scan() with cancelled context")
	}
}

// Mock functions for testing
type mockScalarFunc struct {
	name string
}

func (m *mockScalarFunc) Name() string {
	return m.name
}

func (m *mockScalarFunc) Comment() string {
	return "Mock function"
}

func (m *mockScalarFunc) Signature() FunctionSignature {
	return FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Int64},
		ReturnType: arrow.PrimitiveTypes.Int64,
	}
}

func (m *mockScalarFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	// Return first column as result
	return input.Column(0), nil
}

type mockTableFunc struct {
	name string
}

func (m *mockTableFunc) Name() string {
	return m.name
}

func (m *mockTableFunc) Comment() string {
	return "Mock table function"
}

func (m *mockTableFunc) Signature() FunctionSignature {
	return FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String},
		ReturnType: nil,
	}
}

func (m *mockTableFunc) SchemaForParameters(ctx context.Context, params []interface{}) (*arrow.Schema, error) {
	return arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil), nil
}

func (m *mockTableFunc) Execute(ctx context.Context, params []interface{}, opts *ScanOptions) (array.RecordReader, error) {
	schema, _ := m.SchemaForParameters(ctx, params)
	return testScanFunc(schema)(ctx, opts)
}
