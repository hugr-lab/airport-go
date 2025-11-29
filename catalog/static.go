package catalog

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// staticCatalog is an immutable catalog implementation built from CatalogBuilder.
type staticCatalog struct {
	schemas map[string]*staticSchema
}

// NewStaticCatalog creates a static catalog.
// This is exported for use by the airport package builder.
func NewStaticCatalog() *staticCatalog {
	return &staticCatalog{
		schemas: make(map[string]*staticSchema),
	}
}

// AddSchema adds a schema to the static catalog.
// This is used during catalog building.
// Tables can be any implementation of the Table interface, including DynamicSchemaTable.
func (c *staticCatalog) AddSchema(name, comment string, tables map[string]Table, scalarFuncs []ScalarFunction, tableFuncs []TableFunction, tableFuncsInOut []TableFunctionInOut) {
	c.schemas[name] = &staticSchema{
		name:            name,
		comment:         comment,
		tables:          tables,
		scalarFuncs:     scalarFuncs,
		tableFuncs:      tableFuncs,
		tableFuncsInOut: tableFuncsInOut,
	}
}

// NewStaticTable creates a static table.
// This is exported for use by the airport package builder.
func NewStaticTable(name, comment string, schema *arrow.Schema, scanFunc ScanFunc) *StaticTable {
	return &StaticTable{
		name:     name,
		comment:  comment,
		schema:   schema,
		scanFunc: scanFunc,
	}
}

// Schemas implements Catalog interface.
func (c *staticCatalog) Schemas(ctx context.Context) ([]Schema, error) {
	result := make([]Schema, 0, len(c.schemas))
	for _, schema := range c.schemas {
		result = append(result, schema)
	}
	return result, nil
}

// Schema implements Catalog interface.
func (c *staticCatalog) Schema(ctx context.Context, name string) (Schema, error) {
	schema, ok := c.schemas[name]
	if !ok {
		return nil, nil // Not found, not an error
	}
	return schema, nil
}

// staticSchema is an immutable schema implementation.
type staticSchema struct {
	name            string
	comment         string
	tables          map[string]Table
	scalarFuncs     []ScalarFunction
	tableFuncs      []TableFunction
	tableFuncsInOut []TableFunctionInOut
}

// Name implements Schema interface.
func (s *staticSchema) Name() string {
	return s.name
}

// Comment implements Schema interface.
func (s *staticSchema) Comment() string {
	return s.comment
}

// Tables implements Schema interface.
func (s *staticSchema) Tables(ctx context.Context) ([]Table, error) {
	result := make([]Table, 0, len(s.tables))
	for _, table := range s.tables {
		result = append(result, table)
	}
	return result, nil
}

// Table implements Schema interface.
func (s *staticSchema) Table(ctx context.Context, name string) (Table, error) {
	table, ok := s.tables[name]
	if !ok {
		return nil, nil // Not found, not an error
	}
	return table, nil
}

// ScalarFunctions implements Schema interface.
func (s *staticSchema) ScalarFunctions(ctx context.Context) ([]ScalarFunction, error) {
	return s.scalarFuncs, nil
}

// TableFunctions implements Schema interface.
func (s *staticSchema) TableFunctions(ctx context.Context) ([]TableFunction, error) {
	return s.tableFuncs, nil
}

// TableFunctionsInOut implements Schema interface.
func (s *staticSchema) TableFunctionsInOut(ctx context.Context) ([]TableFunctionInOut, error) {
	return s.tableFuncsInOut, nil
}

// StaticTable is an immutable table implementation.
type StaticTable struct {
	name     string
	comment  string
	schema   *arrow.Schema
	scanFunc ScanFunc
}

// Name implements Table interface.
func (t *StaticTable) Name() string {
	return t.name
}

// Comment implements Table interface.
func (t *StaticTable) Comment() string {
	return t.comment
}

// ArrowSchema implements Table interface.
// If columns is nil or empty, returns full schema.
// If columns is provided, returns projected schema with only those columns.
func (t *StaticTable) ArrowSchema(columns []string) *arrow.Schema {
	return ProjectSchema(t.schema, columns)
}

// Scan implements Table interface.
func (t *StaticTable) Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error) {
	return t.scanFunc(ctx, opts)
}
