package airport_test

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/hugr-lab/airport-go/catalog"
)

// =============================================================================
// Mock Dynamic Catalog for DDL Integration Tests
// =============================================================================
// These mocks implement DynamicCatalog, DynamicSchema, and DynamicTable
// interfaces to test DDL operations via the Airport Flight protocol.
// =============================================================================

// mockDynamicCatalog implements catalog.DynamicCatalog for DDL testing.
type mockDynamicCatalog struct {
	mu      sync.RWMutex
	schemas map[string]*mockDynamicSchema
}

// newMockDynamicCatalog creates a new mock dynamic catalog.
func newMockDynamicCatalog() *mockDynamicCatalog {
	return &mockDynamicCatalog{
		schemas: make(map[string]*mockDynamicSchema),
	}
}

// Schemas returns all schemas in the catalog.
func (c *mockDynamicCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schemas := make([]catalog.Schema, 0, len(c.schemas))
	for _, s := range c.schemas {
		schemas = append(schemas, s)
	}
	return schemas, nil
}

// Schema returns a schema by name.
func (c *mockDynamicCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if s, ok := c.schemas[name]; ok {
		return s, nil
	}
	return nil, nil
}

// CreateSchema creates a new schema.
func (c *mockDynamicCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.schemas[name]; exists {
		return nil, catalog.ErrAlreadyExists
	}

	schema := &mockDynamicSchema{
		name:    name,
		comment: opts.Comment,
		tags:    opts.Tags,
		tables:  make(map[string]*mockDynamicTable),
	}
	c.schemas[name] = schema
	return schema, nil
}

// DropSchema removes a schema from the catalog.
func (c *mockDynamicCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schema, exists := c.schemas[name]
	if !exists {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	// Check if schema contains tables (FR-016)
	if len(schema.tables) > 0 {
		return catalog.ErrSchemaNotEmpty
	}

	delete(c.schemas, name)
	return nil
}

// HasSchema checks if a schema exists (for testing).
func (c *mockDynamicCatalog) HasSchema(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.schemas[name]
	return exists
}

// GetSchema returns a schema for testing.
func (c *mockDynamicCatalog) GetSchema(name string) *mockDynamicSchema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.schemas[name]
}

// mockDynamicSchema implements catalog.DynamicSchema for DDL testing.
type mockDynamicSchema struct {
	mu      sync.RWMutex
	name    string
	comment string
	tags    map[string]string
	tables  map[string]*mockDynamicTable
}

// Name returns the schema name.
func (s *mockDynamicSchema) Name() string {
	return s.name
}

// Comment returns the schema comment.
func (s *mockDynamicSchema) Comment() string {
	return s.comment
}

// Tables returns all tables in the schema.
func (s *mockDynamicSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables := make([]catalog.Table, 0, len(s.tables))
	for _, t := range s.tables {
		tables = append(tables, t)
	}
	return tables, nil
}

// Table returns a table by name.
func (s *mockDynamicSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if t, ok := s.tables[name]; ok {
		return t, nil
	}
	return nil, nil
}

// ScalarFunctions returns scalar functions (empty for mock).
func (s *mockDynamicSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

// TableFunctions returns table functions (empty for mock).
func (s *mockDynamicSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

// TableFunctionsInOut returns table functions (in/out) (empty for mock).
func (s *mockDynamicSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
	return nil, nil
}

// CreateTable creates a new table in the schema.
func (s *mockDynamicSchema) CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts catalog.CreateTableOptions) (catalog.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.tables[name]; exists {
		switch opts.OnConflict {
		case catalog.OnConflictIgnore:
			return existing, nil
		case catalog.OnConflictReplace:
			// Fall through to create new table
		default: // OnConflictError
			return nil, catalog.ErrAlreadyExists
		}
	}

	table := &mockDynamicTable{
		name:    name,
		schema:  schema,
		comment: opts.Comment,
	}
	s.tables[name] = table
	return table, nil
}

// DropTable removes a table from the schema.
func (s *mockDynamicSchema) DropTable(_ context.Context, name string, opts catalog.DropTableOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; !exists {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	delete(s.tables, name)
	return nil
}

// RenameTable renames a table in the schema.
func (s *mockDynamicSchema) RenameTable(_ context.Context, oldName, newName string, opts catalog.RenameTableOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table, exists := s.tables[oldName]
	if !exists {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	if _, exists := s.tables[newName]; exists {
		return catalog.ErrAlreadyExists
	}

	// Rename the table
	table.name = newName
	delete(s.tables, oldName)
	s.tables[newName] = table
	return nil
}

// HasTable checks if a table exists (for testing).
func (s *mockDynamicSchema) HasTable(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.tables[name]
	return exists
}

// GetTable returns a table for testing.
func (s *mockDynamicSchema) GetTable(name string) *mockDynamicTable {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tables[name]
}

// mockDynamicTable implements catalog.DynamicTable for DDL testing.
type mockDynamicTable struct {
	mu      sync.RWMutex
	name    string
	schema  *arrow.Schema
	comment string
	records []arrow.RecordBatch // stored records for Scan
}

// Name returns the table name.
func (t *mockDynamicTable) Name() string {
	return t.name
}

// Comment returns the table comment.
func (t *mockDynamicTable) Comment() string {
	return t.comment
}

// ArrowSchema returns the Arrow schema for the table.
func (t *mockDynamicTable) ArrowSchema(cols []string) *arrow.Schema {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(cols) == 0 {
		return t.schema
	}

	// Project to requested columns
	return catalog.ProjectSchema(t.schema, cols)
}

// Scan returns stored records.
func (t *mockDynamicTable) Scan(_ context.Context, _ *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Convert stored records to record batches
	batches := make([]arrow.RecordBatch, len(t.records))
	for i, rec := range t.records {
		rec.Retain()
		batches[i] = rec
	}

	return array.NewRecordReader(t.schema, batches)
}

// Insert adds rows to the table (implements InsertableTable).
func (t *mockDynamicTable) Insert(_ context.Context, rows array.RecordReader, _ *catalog.DMLOptions) (*catalog.DMLResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Store records and count rows
	var rowCount int64
	for rows.Next() {
		rec := rows.RecordBatch()
		rec.Retain()
		t.records = append(t.records, rec)
		rowCount += rec.NumRows()
	}

	return &catalog.DMLResult{
		AffectedRows: rowCount,
	}, nil
}

// AddColumn adds a column to the table.
func (t *mockDynamicTable) AddColumn(ctx context.Context, columnSchema *arrow.Schema, opts catalog.AddColumnOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if columnSchema.NumFields() != 1 {
		return catalog.ErrNotFound // Should use InvalidArgument but we don't have that error
	}

	newCol := columnSchema.Field(0)

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == newCol.Name {
			if opts.IfColumnNotExists {
				return nil
			}
			return catalog.ErrAlreadyExists
		}
	}

	// Add column to schema
	fields := make([]arrow.Field, t.schema.NumFields()+1)
	for i := 0; i < t.schema.NumFields(); i++ {
		fields[i] = t.schema.Field(i)
	}
	fields[len(fields)-1] = newCol

	meta := t.schema.Metadata()
	t.schema = arrow.NewSchema(fields, &meta)

	return nil
}

// RemoveColumn removes a column from the table.
func (t *mockDynamicTable) RemoveColumn(ctx context.Context, name string, opts catalog.RemoveColumnOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Find column index
	colIdx := -1
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == name {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		if opts.IfColumnExists {
			return nil
		}
		return catalog.ErrNotFound
	}

	// Remove column from schema
	fields := make([]arrow.Field, 0, t.schema.NumFields()-1)
	for i := 0; i < t.schema.NumFields(); i++ {
		if i != colIdx {
			fields = append(fields, t.schema.Field(i))
		}
	}

	meta := t.schema.Metadata()
	t.schema = arrow.NewSchema(fields, &meta)

	return nil
}

// RenameColumn renames a column in the table.
func (t *mockDynamicTable) RenameColumn(_ context.Context, oldName, newName string, opts catalog.RenameColumnOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Find column index
	colIdx := -1
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == oldName {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	// Check if new name already exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == newName {
			return catalog.ErrAlreadyExists
		}
	}

	// Rename column in schema
	fields := make([]arrow.Field, t.schema.NumFields())
	for i := 0; i < t.schema.NumFields(); i++ {
		if i == colIdx {
			oldField := t.schema.Field(i)
			fields[i] = arrow.Field{Name: newName, Type: oldField.Type, Nullable: oldField.Nullable, Metadata: oldField.Metadata}
		} else {
			fields[i] = t.schema.Field(i)
		}
	}

	meta := t.schema.Metadata()
	t.schema = arrow.NewSchema(fields, &meta)
	return nil
}

// ChangeColumnType changes the type of a column.
func (t *mockDynamicTable) ChangeColumnType(_ context.Context, columnSchema *arrow.Schema, _ string, opts catalog.ChangeColumnTypeOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if columnSchema.NumFields() != 1 {
		return catalog.ErrNotFound
	}

	newField := columnSchema.Field(0)

	// Find column index
	colIdx := -1
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == newField.Name {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	// Change column type in schema
	fields := make([]arrow.Field, t.schema.NumFields())
	for i := 0; i < t.schema.NumFields(); i++ {
		if i == colIdx {
			fields[i] = newField
		} else {
			fields[i] = t.schema.Field(i)
		}
	}

	meta := t.schema.Metadata()
	t.schema = arrow.NewSchema(fields, &meta)
	return nil
}

// SetNotNull adds a NOT NULL constraint to a column (mock: no-op).
func (t *mockDynamicTable) SetNotNull(_ context.Context, columnName string, opts catalog.SetNotNullOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			return nil // Success (no-op in mock)
		}
	}

	if opts.IgnoreNotFound {
		return nil
	}
	return catalog.ErrNotFound
}

// DropNotNull removes a NOT NULL constraint from a column (mock: no-op).
func (t *mockDynamicTable) DropNotNull(_ context.Context, columnName string, opts catalog.DropNotNullOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			return nil // Success (no-op in mock)
		}
	}

	if opts.IgnoreNotFound {
		return nil
	}
	return catalog.ErrNotFound
}

// SetDefault sets or changes the default value of a column (mock: no-op).
func (t *mockDynamicTable) SetDefault(_ context.Context, columnName, _ string, opts catalog.SetDefaultOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			return nil // Success (no-op in mock)
		}
	}

	if opts.IgnoreNotFound {
		return nil
	}
	return catalog.ErrNotFound
}

// AddField adds a field to a struct-typed column (mock: simplified implementation).
func (t *mockDynamicTable) AddField(_ context.Context, _ *arrow.Schema, opts catalog.AddFieldOptions) error {
	// Mock implementation: just return success
	// Real implementation would parse the schema and add field to struct column
	if opts.IgnoreNotFound {
		return nil
	}
	return nil
}

// RenameField renames a field in a struct-typed column (mock: simplified implementation).
func (t *mockDynamicTable) RenameField(_ context.Context, columnPath []string, _ string, opts catalog.RenameFieldOptions) error {
	// Mock implementation: just return success if path is valid
	// Real implementation would find the struct column and rename the field
	if len(columnPath) == 0 {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}
	return nil
}

// HasColumn checks if a column exists (for testing).
func (t *mockDynamicTable) HasColumn(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == name {
			return true
		}
	}
	return false
}

// ColumnCount returns the number of columns (for testing).
func (t *mockDynamicTable) ColumnCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.schema.NumFields()
}

// =============================================================================
// DDL Integration Tests via DuckDB SQL
// =============================================================================
// These tests use DuckDB as a Flight client to execute SQL DDL statements
// (CREATE SCHEMA, DROP SCHEMA, CREATE TABLE, etc.) against the Airport server.
// =============================================================================

// Note: DuckDB DDL tests are in ddl_test.go.
