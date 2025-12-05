// Package main demonstrates an Airport Flight server with DDL (Data Definition Language) support.
// This example shows how to implement dynamic catalog operations like CREATE SCHEMA,
// DROP SCHEMA, CREATE TABLE, DROP TABLE, and ALTER TABLE operations.
//
// To test with DuckDB CLI:
//
//	duckdb
//	INSTALL airport FROM community;
//	LOAD airport;
//	ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
//
//	-- Schema operations:
//	CREATE SCHEMA demo.analytics;
//	DROP SCHEMA demo.analytics;
//
//	-- Table operations:
//	CREATE SCHEMA demo.app;
//	CREATE TABLE demo.app.users (id INTEGER, name VARCHAR, email VARCHAR);
//	ALTER TABLE demo.app.users ADD COLUMN age INTEGER;
//	ALTER TABLE demo.app.users DROP COLUMN age;
//	ALTER TABLE demo.app.users RENAME COLUMN name TO full_name;
//	DROP TABLE demo.app.users;
//
//	-- CREATE TABLE AS SELECT (requires INSERT support):
//	CREATE TABLE demo.app.backup AS SELECT * FROM demo.app.users;
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Create a dynamic catalog that supports DDL operations
	cat := NewDDLCatalog()

	// Pre-create a schema with a sample table
	dataSchema := cat.GetOrCreateSchema("data")
	usersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	_, _ = dataSchema.CreateTable(context.Background(), "sample", usersSchema, catalog.CreateTableOptions{
		Comment: "Sample table created at startup",
	})

	// Create gRPC server with debug logging
	debugLevel := slog.LevelDebug
	config := airport.ServerConfig{
		Catalog:  cat,
		LogLevel: &debugLevel,
	}

	opts := airport.ServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	if err := airport.NewServer(grpcServer, config); err != nil {
		log.Fatalf("Failed to register Airport server: %v", err)
	}

	// Start serving
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Airport DDL server listening on :50051")
	log.Println("")
	log.Println("Example catalog structure:")
	log.Println("  - Schema: data")
	log.Println("    - Table: sample (id INTEGER, name VARCHAR)")
	log.Println("")
	log.Println("Test with DuckDB CLI:")
	log.Println("  ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');")
	log.Println("")
	log.Println("  -- Schema operations:")
	log.Println("  CREATE SCHEMA demo.analytics;")
	log.Println("  DROP SCHEMA demo.analytics;")
	log.Println("")
	log.Println("  -- Table operations:")
	log.Println("  CREATE TABLE demo.data.users (id INTEGER, name VARCHAR);")
	log.Println("  ALTER TABLE demo.data.users ADD COLUMN email VARCHAR;")
	log.Println("  ALTER TABLE demo.data.users RENAME COLUMN name TO full_name;")
	log.Println("  DROP TABLE demo.data.users;")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// =============================================================================
// DDL Catalog Implementation
// =============================================================================

// DDLCatalog implements catalog.DynamicCatalog for DDL operations.
// It supports CREATE SCHEMA, DROP SCHEMA, and delegates table operations
// to DDLSchema which implements catalog.DynamicSchema.
type DDLCatalog struct {
	mu      sync.RWMutex
	schemas map[string]*DDLSchema
}

func NewDDLCatalog() *DDLCatalog {
	return &DDLCatalog{
		schemas: make(map[string]*DDLSchema),
	}
}

// GetOrCreateSchema returns an existing schema or creates a new one.
func (c *DDLCatalog) GetOrCreateSchema(name string) *DDLSchema {
	c.mu.Lock()
	defer c.mu.Unlock()

	if s, ok := c.schemas[name]; ok {
		return s
	}

	s := NewDDLSchema(name, "")
	c.schemas[name] = s
	return s
}

// Schemas implements catalog.Catalog.
func (c *DDLCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]catalog.Schema, 0, len(c.schemas))
	for _, s := range c.schemas {
		result = append(result, s)
	}
	return result, nil
}

// Schema implements catalog.Catalog.
func (c *DDLCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if s, ok := c.schemas[name]; ok {
		return s, nil
	}
	return nil, nil
}

// CreateSchema implements catalog.DynamicCatalog.
func (c *DDLCatalog) CreateSchema(_ context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.schemas[name]; exists {
		return nil, catalog.ErrAlreadyExists
	}

	s := NewDDLSchema(name, opts.Comment)
	c.schemas[name] = s

	fmt.Printf("[DDLCatalog] Created schema: %s\n", name)
	return s, nil
}

// DropSchema implements catalog.DynamicCatalog.
func (c *DDLCatalog) DropSchema(_ context.Context, name string, opts catalog.DropSchemaOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, exists := c.schemas[name]
	if !exists {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	// Check if schema has tables
	if s.TableCount() > 0 {
		return catalog.ErrSchemaNotEmpty
	}

	delete(c.schemas, name)
	fmt.Printf("[DDLCatalog] Dropped schema: %s\n", name)
	return nil
}

// =============================================================================
// DDL Schema Implementation
// =============================================================================

// DDLSchema implements catalog.DynamicSchema for table management operations.
type DDLSchema struct {
	mu      sync.RWMutex
	name    string
	comment string
	tables  map[string]*DDLTable
}

func NewDDLSchema(name, comment string) *DDLSchema {
	return &DDLSchema{
		name:    name,
		comment: comment,
		tables:  make(map[string]*DDLTable),
	}
}

func (s *DDLSchema) Name() string    { return s.name }
func (s *DDLSchema) Comment() string { return s.comment }

func (s *DDLSchema) TableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tables)
}

// Tables implements catalog.Schema.
func (s *DDLSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]catalog.Table, 0, len(s.tables))
	for _, t := range s.tables {
		result = append(result, t)
	}
	return result, nil
}

// Table implements catalog.Schema.
func (s *DDLSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if t, ok := s.tables[name]; ok {
		return t, nil
	}
	return nil, nil
}

// ScalarFunctions implements catalog.Schema.
func (s *DDLSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

// TableFunctions implements catalog.Schema.
func (s *DDLSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

// TableFunctionsInOut implements catalog.Schema.
func (s *DDLSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
	return nil, nil
}

// CreateTable implements catalog.DynamicSchema.
func (s *DDLSchema) CreateTable(_ context.Context, name string, schema *arrow.Schema, opts catalog.CreateTableOptions) (catalog.Table, error) {
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

	t := NewDDLTable(name, schema, opts.Comment)
	s.tables[name] = t

	fmt.Printf("[DDLSchema:%s] Created table: %s (%d columns)\n", s.name, name, schema.NumFields())
	return t, nil
}

// DropTable implements catalog.DynamicSchema.
func (s *DDLSchema) DropTable(_ context.Context, name string, opts catalog.DropTableOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; !exists {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	delete(s.tables, name)
	fmt.Printf("[DDLSchema:%s] Dropped table: %s\n", s.name, name)
	return nil
}

// RenameTable implements catalog.DynamicSchema.
func (s *DDLSchema) RenameTable(_ context.Context, oldName, newName string, opts catalog.RenameTableOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, exists := s.tables[oldName]
	if !exists {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}

	if _, exists := s.tables[newName]; exists {
		return catalog.ErrAlreadyExists
	}

	t.name = newName
	delete(s.tables, oldName)
	s.tables[newName] = t

	fmt.Printf("[DDLSchema:%s] Renamed table: %s -> %s\n", s.name, oldName, newName)
	return nil
}

// =============================================================================
// DDL Table Implementation
// =============================================================================

// DDLTable implements catalog.DynamicTable for column management operations.
// It also implements catalog.InsertableTable for CREATE TABLE AS SELECT support.
type DDLTable struct {
	mu      sync.RWMutex
	name    string
	schema  *arrow.Schema
	comment string
	data    []arrow.RecordBatch // stored data for Scan
}

func NewDDLTable(name string, schema *arrow.Schema, comment string) *DDLTable {
	return &DDLTable{
		name:    name,
		schema:  schema,
		comment: comment,
		data:    make([]arrow.RecordBatch, 0),
	}
}

func (t *DDLTable) Name() string    { return t.name }
func (t *DDLTable) Comment() string { return t.comment }

func (t *DDLTable) ArrowSchema(columns []string) *arrow.Schema {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return catalog.ProjectSchema(t.schema, columns)
}

// Scan implements catalog.Table.
func (t *DDLTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Return stored data
	batches := make([]arrow.RecordBatch, len(t.data))
	for i, rec := range t.data {
		rec.Retain()
		batches[i] = rec
	}

	return array.NewRecordReader(t.schema, batches)
}

// Insert implements catalog.InsertableTable (for CREATE TABLE AS SELECT).
func (t *DDLTable) Insert(_ context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var rowCount int64
	for rows.Next() {
		rec := rows.RecordBatch()
		rec.Retain()
		t.data = append(t.data, rec)
		rowCount += rec.NumRows()
	}

	fmt.Printf("[DDLTable:%s] Inserted %d rows\n", t.name, rowCount)
	return &catalog.DMLResult{AffectedRows: rowCount}, nil
}

// AddColumn implements catalog.DynamicTable.
func (t *DDLTable) AddColumn(_ context.Context, columnSchema *arrow.Schema, opts catalog.AddColumnOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if columnSchema.NumFields() != 1 {
		return fmt.Errorf("column schema must contain exactly one field")
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

	fmt.Printf("[DDLTable:%s] Added column: %s (%s)\n", t.name, newCol.Name, newCol.Type)
	return nil
}

// RemoveColumn implements catalog.DynamicTable.
func (t *DDLTable) RemoveColumn(_ context.Context, name string, opts catalog.RemoveColumnOptions) error {
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

	fmt.Printf("[DDLTable:%s] Removed column: %s\n", t.name, name)
	return nil
}

// RenameColumn implements catalog.DynamicTable.
func (t *DDLTable) RenameColumn(_ context.Context, oldName, newName string, opts catalog.RenameColumnOptions) error {
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

	fmt.Printf("[DDLTable:%s] Renamed column: %s -> %s\n", t.name, oldName, newName)
	return nil
}

// ChangeColumnType implements catalog.DynamicTable.
func (t *DDLTable) ChangeColumnType(_ context.Context, columnSchema *arrow.Schema, _ string, opts catalog.ChangeColumnTypeOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if columnSchema.NumFields() != 1 {
		return fmt.Errorf("column schema must contain exactly one field")
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

	fmt.Printf("[DDLTable:%s] Changed column type: %s to %s\n", t.name, newField.Name, newField.Type)
	return nil
}

// SetNotNull implements catalog.DynamicTable.
func (t *DDLTable) SetNotNull(_ context.Context, columnName string, opts catalog.SetNotNullOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			fmt.Printf("[DDLTable:%s] Set NOT NULL on column: %s\n", t.name, columnName)
			return nil
		}
	}

	if opts.IgnoreNotFound {
		return nil
	}
	return catalog.ErrNotFound
}

// DropNotNull implements catalog.DynamicTable.
func (t *DDLTable) DropNotNull(_ context.Context, columnName string, opts catalog.DropNotNullOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			fmt.Printf("[DDLTable:%s] Dropped NOT NULL on column: %s\n", t.name, columnName)
			return nil
		}
	}

	if opts.IgnoreNotFound {
		return nil
	}
	return catalog.ErrNotFound
}

// SetDefault implements catalog.DynamicTable.
func (t *DDLTable) SetDefault(_ context.Context, columnName, expression string, opts catalog.SetDefaultOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if column exists
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			fmt.Printf("[DDLTable:%s] Set DEFAULT on column: %s = %s\n", t.name, columnName, expression)
			return nil
		}
	}

	if opts.IgnoreNotFound {
		return nil
	}
	return catalog.ErrNotFound
}

// AddField implements catalog.DynamicTable.
//
//nolint:unparam // Simplified implementation always returns nil
func (t *DDLTable) AddField(_ context.Context, _ *arrow.Schema, _ catalog.AddFieldOptions) error {
	// Simplified implementation - real implementation would handle nested struct fields
	fmt.Printf("[DDLTable:%s] AddField called (simplified)\n", t.name)
	return nil
}

// RenameField implements catalog.DynamicTable.
func (t *DDLTable) RenameField(_ context.Context, columnPath []string, newName string, opts catalog.RenameFieldOptions) error {
	// Simplified implementation - real implementation would handle nested struct fields
	if len(columnPath) == 0 {
		if opts.IgnoreNotFound {
			return nil
		}
		return catalog.ErrNotFound
	}
	fmt.Printf("[DDLTable:%s] RenameField called: %v -> %s (simplified)\n", t.name, columnPath, newName)
	return nil
}
