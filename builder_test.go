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
	}
}

func (m *mockTableFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
	return arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil), nil
}

func (m *mockTableFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
	schema, _ := m.SchemaForParameters(ctx, params)
	return testScanFunc(schema)(ctx, opts)
}

// mockTableRef implements catalog.TableRef for testing.
type mockTableRef struct {
	name    string
	comment string
	schema  *arrow.Schema
}

func (m *mockTableRef) Name() string              { return m.name }
func (m *mockTableRef) Comment() string            { return m.comment }
func (m *mockTableRef) ArrowSchema() *arrow.Schema { return m.schema }

func (m *mockTableRef) FunctionCalls(ctx context.Context, req *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
	return []catalog.FunctionCall{
		{
			FunctionName: "read_csv",
			Args: []catalog.FunctionCallArg{
				{Value: "/data/test.csv", Type: arrow.BinaryTypes.String},
			},
		},
	}, nil
}

// TestCatalogBuilderWithTableRef tests adding a table reference.
func TestCatalogBuilderWithTableRef(t *testing.T) {
	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	ref := &mockTableRef{
		name:    "csv_data",
		comment: "CSV data reference",
		schema:  refSchema,
	}

	cat, err := NewCatalogBuilder().
		Schema("test").
		TableRef(ref).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	// Check SchemaWithTableRefs interface
	schemaWithRefs, ok := schema.(catalog.SchemaWithTableRefs)
	if !ok {
		t.Fatal("Expected schema to implement SchemaWithTableRefs")
	}

	refs, err := schemaWithRefs.TableRefs(ctx)
	if err != nil {
		t.Fatalf("TableRefs() failed: %v", err)
	}

	if len(refs) != 1 {
		t.Errorf("Expected 1 table ref, got %d", len(refs))
	}

	// Lookup by name
	found, err := schemaWithRefs.TableRef(ctx, "csv_data")
	if err != nil {
		t.Fatalf("TableRef() failed: %v", err)
	}

	if found == nil {
		t.Fatal("Expected non-nil table ref")
	}

	if found.Name() != "csv_data" {
		t.Errorf("Expected name 'csv_data', got '%s'", found.Name())
	}

	// Not found case
	notFound, err := schemaWithRefs.TableRef(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("TableRef() failed for nonexistent: %v", err)
	}
	if notFound != nil {
		t.Error("Expected nil for nonexistent table ref")
	}
}

// TestCatalogBuilderTableRefDuplicateName tests that duplicate table ref names are rejected.
func TestCatalogBuilderTableRefDuplicateName(t *testing.T) {
	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ref1 := &mockTableRef{name: "dup", schema: refSchema}
	ref2 := &mockTableRef{name: "dup", schema: refSchema}

	_, err := NewCatalogBuilder().
		Schema("test").
		TableRef(ref1).
		TableRef(ref2).
		Build()

	if err == nil {
		t.Error("Expected error for duplicate table ref names, got nil")
	}
}

// TestCatalogBuilderTableRefEmptyName tests that empty table ref names are rejected.
func TestCatalogBuilderTableRefEmptyName(t *testing.T) {
	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ref := &mockTableRef{name: "", schema: refSchema}

	_, err := NewCatalogBuilder().
		Schema("test").
		TableRef(ref).
		Build()

	if err == nil {
		t.Error("Expected error for empty table ref name, got nil")
	}
}

// TestCatalogBuilderTableRefWithTable tests that table refs and regular tables coexist.
func TestCatalogBuilderTableRefWithTable(t *testing.T) {
	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	ref := &mockTableRef{name: "csv_ref", schema: refSchema}

	cat, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "regular_table",
			Schema:   tableSchema,
			ScanFunc: testScanFunc(tableSchema),
		}).
		TableRef(ref).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	// Regular table should be accessible
	table, err := schema.Table(ctx, "regular_table")
	if err != nil {
		t.Fatalf("Table() failed: %v", err)
	}
	if table == nil {
		t.Fatal("Expected non-nil table")
	}

	// Table ref should be accessible
	schemaWithRefs := schema.(catalog.SchemaWithTableRefs)
	ref2, err := schemaWithRefs.TableRef(ctx, "csv_ref")
	if err != nil {
		t.Fatalf("TableRef() failed: %v", err)
	}
	if ref2 == nil {
		t.Fatal("Expected non-nil table ref")
	}
}

// TestCatalogBuilderTableRefConflictsWithTable tests that a table ref
// name cannot conflict with a regular table name.
func TestCatalogBuilderTableRefConflictsWithTable(t *testing.T) {
	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ref := &mockTableRef{name: "same_name", schema: tableSchema}

	_, err := NewCatalogBuilder().
		Schema("test").
		SimpleTable(SimpleTableDef{
			Name:     "same_name",
			Schema:   tableSchema,
			ScanFunc: testScanFunc(tableSchema),
		}).
		TableRef(ref).
		Build()

	if err == nil {
		t.Error("Expected error for conflicting table and table ref names, got nil")
	}
}

// TestCatalogBuilderTableRefWithTableFunc tests that table refs and table functions coexist.
func TestCatalogBuilderTableRefWithTableFunc(t *testing.T) {
	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ref := &mockTableRef{name: "csv_ref", schema: refSchema}
	fn := &mockTableFunc{name: "GENERATE"}

	cat, err := NewCatalogBuilder().
		Schema("test").
		TableRef(ref).
		TableFunc(fn).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	// Table ref should be accessible
	schemaWithRefs := schema.(catalog.SchemaWithTableRefs)
	found, err := schemaWithRefs.TableRef(ctx, "csv_ref")
	if err != nil {
		t.Fatalf("TableRef() failed: %v", err)
	}
	if found == nil {
		t.Fatal("Expected non-nil table ref")
	}

	// Table function should be accessible
	tableFuncs, err := schema.TableFunctions(ctx)
	if err != nil {
		t.Fatalf("TableFunctions() failed: %v", err)
	}
	if len(tableFuncs) != 1 {
		t.Errorf("Expected 1 table function, got %d", len(tableFuncs))
	}
}

// TestCatalogBuilderMultipleTableRefs tests multiple table refs in the same schema.
func TestCatalogBuilderMultipleTableRefs(t *testing.T) {
	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	ref1 := &mockTableRef{name: "ref_a", schema: schema1}
	ref2 := &mockTableRef{name: "ref_b", schema: schema2}

	cat, err := NewCatalogBuilder().
		Schema("test").
		TableRef(ref1).
		TableRef(ref2).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	schema, err := cat.Schema(ctx, "test")
	if err != nil {
		t.Fatalf("Schema() failed: %v", err)
	}

	schemaWithRefs := schema.(catalog.SchemaWithTableRefs)
	refs, err := schemaWithRefs.TableRefs(ctx)
	if err != nil {
		t.Fatalf("TableRefs() failed: %v", err)
	}

	if len(refs) != 2 {
		t.Errorf("Expected 2 table refs, got %d", len(refs))
	}

	// Both should be individually accessible
	for _, name := range []string{"ref_a", "ref_b"} {
		ref, err := schemaWithRefs.TableRef(ctx, name)
		if err != nil {
			t.Fatalf("TableRef(%q) failed: %v", name, err)
		}
		if ref == nil {
			t.Errorf("Expected non-nil table ref for %q", name)
		}
	}
}

// TestCatalogBuilderTableRefsInDifferentSchemas tests table refs across schemas.
func TestCatalogBuilderTableRefsInDifferentSchemas(t *testing.T) {
	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ref1 := &mockTableRef{name: "ref_in_s1", schema: refSchema}
	ref2 := &mockTableRef{name: "ref_in_s2", schema: refSchema}

	cat, err := NewCatalogBuilder().
		Schema("schema1").
		TableRef(ref1).
		Schema("schema2").
		TableRef(ref2).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()

	// Verify schema1 has ref_in_s1
	s1, err := cat.Schema(ctx, "schema1")
	if err != nil {
		t.Fatalf("Schema(schema1) failed: %v", err)
	}
	s1Refs := s1.(catalog.SchemaWithTableRefs)
	ref, err := s1Refs.TableRef(ctx, "ref_in_s1")
	if err != nil || ref == nil {
		t.Errorf("Expected ref_in_s1 in schema1, got ref=%v, err=%v", ref, err)
	}

	// ref_in_s2 should not be in schema1
	notFound, err := s1Refs.TableRef(ctx, "ref_in_s2")
	if err != nil {
		t.Fatalf("TableRef(ref_in_s2) in schema1 failed: %v", err)
	}
	if notFound != nil {
		t.Error("Expected nil for ref_in_s2 in schema1")
	}

	// Verify schema2 has ref_in_s2
	s2, err := cat.Schema(ctx, "schema2")
	if err != nil {
		t.Fatalf("Schema(schema2) failed: %v", err)
	}
	s2Refs := s2.(catalog.SchemaWithTableRefs)
	ref, err = s2Refs.TableRef(ctx, "ref_in_s2")
	if err != nil || ref == nil {
		t.Errorf("Expected ref_in_s2 in schema2, got ref=%v, err=%v", ref, err)
	}
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
