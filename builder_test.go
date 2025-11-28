package airport

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
)

// Test helper: creates a simple scan function for testing
func testScanFunc(schema *arrow.Schema) catalog.ScanFunc {
	return func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		record := builder.NewRecordBatch()
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	}
}

// TestCatalogBuilderBasic tests basic catalog building functionality.
func TestCatalogBuilderBasic(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cat, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err != nil {
		t.Fatalf("Expected successful build, got error: %v", err)
	}

	if cat == nil {
		t.Fatal("Expected non-nil catalog")
	}
}

// TestCatalogBuilderMultipleSchemas tests adding multiple schemas.
func TestCatalogBuilderMultipleSchemas(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cat, err := NewCatalogBuilder().
		Schema("schema1").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Schema("schema2").
		SimpleTable(SimpleTableDef{
			Name:     "table2",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err != nil {
		t.Fatalf("Expected successful build, got error: %v", err)
	}

	// Verify both schemas exist
	ctx := context.Background()
	schemas, err := cat.Schemas(ctx)
	if err != nil {
		t.Fatalf("Failed to get schemas: %v", err)
	}

	if len(schemas) != 2 {
		t.Errorf("Expected 2 schemas, got %d", len(schemas))
	}
}

// TestCatalogBuilderEmptySchemaName tests that empty schema names are rejected.
func TestCatalogBuilderEmptySchemaName(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	_, err := NewCatalogBuilder().
		Schema("").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err == nil {
		t.Error("Expected error for empty schema name, got nil")
	}
}

// TestCatalogBuilderDuplicateSchemaNames tests that duplicate schema names are rejected.
func TestCatalogBuilderDuplicateSchemaNames(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	_, err := NewCatalogBuilder().
		Schema("duplicate").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Schema("duplicate").
		SimpleTable(SimpleTableDef{
			Name:     "table2",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err == nil {
		t.Error("Expected error for duplicate schema names, got nil")
	}

	if err != nil && !contains(err.Error(), "duplicate schema name") {
		t.Errorf("Expected 'duplicate schema name' error, got: %v", err)
	}
}

// TestCatalogBuilderDuplicateTableNames tests that duplicate table names in same schema are rejected.
func TestCatalogBuilderDuplicateTableNames(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	_, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "duplicate",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		SimpleTable(SimpleTableDef{
			Name:     "duplicate",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err == nil {
		t.Error("Expected error for duplicate table names, got nil")
	}

	if err != nil && !contains(err.Error(), "duplicate table name") {
		t.Errorf("Expected 'duplicate table name' error, got: %v", err)
	}
}

// TestCatalogBuilderEmptyTableName tests that empty table names are rejected.
func TestCatalogBuilderEmptyTableName(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	_, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err == nil {
		t.Error("Expected error for empty table name, got nil")
	}

	if err != nil && !contains(err.Error(), "table name cannot be empty") {
		t.Errorf("Expected 'table name cannot be empty' error, got: %v", err)
	}
}

// TestCatalogBuilderNilSchema tests that nil Arrow schema is rejected.
func TestCatalogBuilderNilSchema(t *testing.T) {
	_, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   nil,
			ScanFunc: testScanFunc(nil),
		}).
		Build()

	if err == nil {
		t.Error("Expected error for nil schema, got nil")
	}

	if err != nil && !contains(err.Error(), "nil schema") {
		t.Errorf("Expected 'nil schema' error, got: %v", err)
	}
}

// TestCatalogBuilderNilScanFunc tests that nil scan function is rejected.
func TestCatalogBuilderNilScanFunc(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	_, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: nil,
		}).
		Build()

	if err == nil {
		t.Error("Expected error for nil scan function, got nil")
	}

	if err != nil && !contains(err.Error(), "nil scan function") {
		t.Errorf("Expected 'nil scan function' error, got: %v", err)
	}
}

// TestCatalogBuilderCannotBuildTwice tests that building twice returns error.
func TestCatalogBuilderCannotBuildTwice(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		})

	// First build should succeed
	_, err := builder.Build()
	if err != nil {
		t.Fatalf("First build failed: %v", err)
	}

	// Second build should fail
	_, err = builder.Build()
	if err == nil {
		t.Error("Expected error when building twice, got nil")
	}

	if err != nil && !contains(err.Error(), "already built") {
		t.Errorf("Expected 'already built' error, got: %v", err)
	}
}

// TestCatalogBuilderWithComment tests adding comments to schemas.
func TestCatalogBuilderWithComment(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cat, err := NewCatalogBuilder().
		Schema("test").
		Comment("Test schema comment").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Comment:  "Test table comment",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify comment is preserved
	ctx := context.Background()
	testSchema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}

	if testSchema.Comment() != "Test schema comment" {
		t.Errorf("Expected schema comment 'Test schema comment', got '%s'", testSchema.Comment())
	}
}

// TestCatalogBuilderWithFunctions tests adding scalar and table functions.
func TestCatalogBuilderWithFunctions(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create mock functions
	mockScalar := &mockScalarFunc{name: "TEST_FUNC"}
	mockTable := &mockTableFunc{name: "TEST_TABLE_FUNC"}

	cat, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "table1",
			Schema:   schema,
			ScanFunc: testScanFunc(schema),
		}).
		ScalarFunc(mockScalar).
		TableFunc(mockTable).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify functions are present
	ctx := context.Background()
	testSchema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}

	scalarFuncs, err := testSchema.ScalarFunctions(ctx)
	if err != nil {
		t.Fatalf("Failed to get scalar functions: %v", err)
	}

	if len(scalarFuncs) != 1 {
		t.Errorf("Expected 1 scalar function, got %d", len(scalarFuncs))
	}

	tableFuncs, err := testSchema.TableFunctions(ctx)
	if err != nil {
		t.Fatalf("Failed to get table functions: %v", err)
	}

	if len(tableFuncs) != 1 {
		t.Errorf("Expected 1 table function, got %d", len(tableFuncs))
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
	return "Mock scalar function"
}

func (m *mockScalarFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Int64},
		ReturnType: arrow.PrimitiveTypes.Int64,
		Variadic:   false,
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

func (m *mockTableFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String},
		ReturnType: nil, // Table functions don't have scalar return type
		Variadic:   false,
	}
}

func (m *mockTableFunc) SchemaForParameters(ctx context.Context, params []interface{}) (*arrow.Schema, error) {
	return arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil), nil
}

func (m *mockTableFunc) Execute(ctx context.Context, params []interface{}, opts *catalog.ScanOptions) (array.RecordReader, error) {
	schema, _ := m.SchemaForParameters(ctx, params)
	return testScanFunc(schema)(ctx, opts)
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findInString(s, substr))
}

func findInString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
