package airport

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/hugr-lab/airport-go/catalog"
)

// SimpleTableDef defines a table with fixed schema.
// Used with SchemaBuilder.SimpleTable().
type SimpleTableDef struct {
	// Name is the table name (e.g., "users", "orders").
	// REQUIRED: MUST be non-empty and unique within schema.
	Name string

	// Comment is optional table documentation.
	// OPTIONAL: Empty string if no comment.
	Comment string

	// Schema is the Arrow schema describing table columns.
	// REQUIRED: MUST NOT be nil.
	Schema *arrow.Schema

	// ScanFunc provides table data as RecordReader.
	// REQUIRED: MUST NOT be nil.
	ScanFunc catalog.ScanFunc
}

// CatalogBuilder builds static catalogs using fluent API.
// Not thread-safe - use only during initialization.
type CatalogBuilder struct {
	schemas []*schemaBuilder
	built   bool
}

// NewCatalogBuilder creates a new fluent catalog builder.
// Returns builder in "empty" state (no schemas).
//
// Example:
//
//	cat := airport.NewCatalogBuilder().
//	    Schema("main").
//	        SimpleTable(...).
//	        ScalarFunc(...).
//	    Build()
func NewCatalogBuilder() *CatalogBuilder {
	return &CatalogBuilder{
		schemas: make([]*schemaBuilder, 0),
		built:   false,
	}
}

// Schema starts defining a new schema.
// Returns SchemaBuilder for adding tables/functions to this schema.
// Schema name MUST be non-empty and unique within catalog.
//
// Example:
//
//	builder.Schema("main").
//	    Comment("Main application schema").
//	    Table(...).
//	Schema("staging").
//	    Table(...)
func (cb *CatalogBuilder) Schema(name string) *SchemaBuilder {
	sb := &schemaBuilder{
		name:           name,
		comment:        "",
		tables:         make([]SimpleTableDef, 0),
		scalarFuncs:    make([]catalog.ScalarFunction, 0),
		tableFuncs:     make([]catalog.TableFunction, 0),
		catalogBuilder: cb,
	}
	cb.schemas = append(cb.schemas, sb)
	return &SchemaBuilder{builder: sb}
}

// Build finalizes the catalog and returns immutable Catalog implementation.
// Can only be called once. Further modifications return error.
// Returns error if catalog is invalid (e.g., duplicate schema names).
func (cb *CatalogBuilder) Build() (catalog.Catalog, error) {
	if cb.built {
		return nil, fmt.Errorf("catalog already built")
	}

	// Validate schema names are unique and non-empty
	seenNames := make(map[string]bool)
	for _, sb := range cb.schemas {
		if sb.name == "" {
			return nil, fmt.Errorf("schema name cannot be empty")
		}
		if seenNames[sb.name] {
			return nil, fmt.Errorf("duplicate schema name: %s", sb.name)
		}
		seenNames[sb.name] = true

		// Validate table names within schema
		tableNames := make(map[string]bool)
		for _, table := range sb.tables {
			if table.Name == "" {
				return nil, fmt.Errorf("table name cannot be empty in schema %s", sb.name)
			}
			if tableNames[table.Name] {
				return nil, fmt.Errorf("duplicate table name %s in schema %s", table.Name, sb.name)
			}
			tableNames[table.Name] = true

			// Validate table definition
			if table.Schema == nil {
				return nil, fmt.Errorf("table %s.%s has nil schema", sb.name, table.Name)
			}
			if table.ScanFunc == nil {
				return nil, fmt.Errorf("table %s.%s has nil scan function", sb.name, table.Name)
			}
		}
	}

	cb.built = true

	// Create static catalog and populate it
	cat := catalog.NewStaticCatalog()
	for _, sb := range cb.schemas {
		// Convert tables to static tables
		tables := make(map[string]*catalog.StaticTable)
		for _, tableDef := range sb.tables {
			tables[tableDef.Name] = catalog.NewStaticTable(
				tableDef.Name,
				tableDef.Comment,
				tableDef.Schema,
				tableDef.ScanFunc,
			)
		}

		// Add schema to catalog
		cat.AddSchema(sb.name, sb.comment, tables, sb.scalarFuncs, sb.tableFuncs, sb.tableFuncsInOut)
	}

	return cat, nil
}

// SchemaBuilder builds a schema within a catalog.
// Not thread-safe - use only during initialization.
type SchemaBuilder struct {
	builder *schemaBuilder
}

// schemaBuilder is the internal schema builder implementation.
type schemaBuilder struct {
	name            string
	comment         string
	tables          []SimpleTableDef
	scalarFuncs     []catalog.ScalarFunction
	tableFuncs      []catalog.TableFunction
	tableFuncsInOut []catalog.TableFunctionInOut
	catalogBuilder  *CatalogBuilder
}

// Comment sets optional schema documentation.
// Returns self for method chaining.
func (sb *SchemaBuilder) Comment(comment string) *SchemaBuilder {
	sb.builder.comment = comment
	return sb
}

// SimpleTable adds a table with fixed schema using SimpleTableDef.
// Returns self for method chaining.
// Table name MUST be unique within schema.
//
// Example:
//
//	schema.SimpleTable(airport.SimpleTableDef{
//	    Name: "users",
//	    Comment: "User accounts",
//	    Schema: userSchema,
//	    ScanFunc: scanUsers,
//	})
func (sb *SchemaBuilder) SimpleTable(def SimpleTableDef) *SchemaBuilder {
	sb.builder.tables = append(sb.builder.tables, def)
	return sb
}

// ScalarFunc adds a scalar function to this schema.
// Returns self for method chaining.
// Function name MUST be unique within schema.
//
// Example:
//
//	schema.ScalarFunc(&UppercaseFunc{})
func (sb *SchemaBuilder) ScalarFunc(fn catalog.ScalarFunction) *SchemaBuilder {
	sb.builder.scalarFuncs = append(sb.builder.scalarFuncs, fn)
	return sb
}

// TableFunc adds a table-valued function to this schema.
// Returns self for method chaining.
// Function name MUST be unique within schema.
//
// Example:
//
//	schema.TableFunc(&ReadParquetFunc{})
func (sb *SchemaBuilder) TableFunc(fn catalog.TableFunction) *SchemaBuilder {
	sb.builder.tableFuncs = append(sb.builder.tableFuncs, fn)
	return sb
}

// TableFuncInOut registers a table function that accepts row sets as input.
// Returns self for method chaining.
// Function name MUST be unique within schema.
//
// Example:
//
//	schema.TableFuncInOut(&FilterRowsFunc{})
func (sb *SchemaBuilder) TableFuncInOut(fn catalog.TableFunctionInOut) *SchemaBuilder {
	sb.builder.tableFuncsInOut = append(sb.builder.tableFuncsInOut, fn)
	return sb
}

// Schema starts a new schema definition (returns to CatalogBuilder).
// Finalizes current schema and begins new one.
// Allows chaining: Schema("a").Table(...).Schema("b").Table(...)
func (sb *SchemaBuilder) Schema(name string) *SchemaBuilder {
	return sb.builder.catalogBuilder.Schema(name)
}

// Build finalizes the catalog (returns to CatalogBuilder).
// Same as calling catalogBuilder.Build().
func (sb *SchemaBuilder) Build() (catalog.Catalog, error) {
	return sb.builder.catalogBuilder.Build()
}
