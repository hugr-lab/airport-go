# Contract: Catalog Builder API

**Package**: `github.com/your-org/airport-go`
**Purpose**: Fluent API for building static catalogs
**Go Version**: 1.23+
**Arrow Version**: `github.com/apache/arrow-go/v18/arrow`

## CatalogBuilder API

```go
package airport

import (
    "github.com/apache/arrow-go/v18/arrow"

    "github.com/your-org/airport-go/catalog"
)

// NewCatalogBuilder creates a new fluent catalog builder.
// Returns builder in "empty" state (no schemas).
//
// Example:
//   cat := airport.NewCatalogBuilder().
//       Schema("main").
//           Table(...).
//           ScalarFunc(...).
//       Build()
func NewCatalogBuilder() *CatalogBuilder

// CatalogBuilder builds static catalogs using fluent API.
// Not thread-safe - use only during initialization.
type CatalogBuilder struct {
    // private fields
}

// Schema starts defining a new schema.
// Returns SchemaBuilder for adding tables/functions to this schema.
// Schema name MUST be non-empty and unique within catalog.
//
// Example:
//   builder.Schema("main").
//       Comment("Main application schema").
//       Table(...).
//   Schema("staging").
//       Table(...)
func (cb *CatalogBuilder) Schema(name string) *SchemaBuilder

// Build finalizes the catalog and returns immutable Catalog implementation.
// Can only be called once. Further modifications return error.
// Returns error if catalog is invalid (e.g., duplicate schema names).
func (cb *CatalogBuilder) Build() (catalog.Catalog, error)
```

## SchemaBuilder API

```go
// SchemaBuilder builds a schema within a catalog.
// Not thread-safe - use only during initialization.
type SchemaBuilder struct {
    // private fields
}

// Comment sets optional schema documentation.
// Returns self for method chaining.
func (sb *SchemaBuilder) Comment(comment string) *SchemaBuilder

// SimpleTable adds a table with fixed schema using SimpleTableDef.
// Returns self for method chaining.
// Table name MUST be unique within schema.
//
// Example:
//   schema.SimpleTable(catalog.SimpleTableDef{
//       Name: "users",
//       Comment: "User accounts",
//       Schema: userSchema,
//       ScanFunc: scanUsers,
//   })
func (sb *SchemaBuilder) SimpleTable(def SimpleTableDef) *SchemaBuilder

// DynamicTable adds a table with parameter/time-dependent schema.
// Returns self for method chaining.
// Table name MUST be unique within schema.
// Table MUST implement DynamicSchemaTable interface.
//
// Example:
//   schema.DynamicTable(&myTimeSeriesTable{...})
func (sb *SchemaBuilder) DynamicTable(table catalog.DynamicSchemaTable) *SchemaBuilder

// ScalarFunc adds a scalar function to this schema.
// Returns self for method chaining.
// Function name MUST be unique within schema.
//
// Example:
//   schema.ScalarFunc(&UppercaseFunc{})
func (sb *SchemaBuilder) ScalarFunc(fn catalog.ScalarFunction) *SchemaBuilder

// TableFunc adds a table-valued function to this schema.
// Returns self for method chaining.
// Function name MUST be unique within schema.
//
// Example:
//   schema.TableFunc(&ReadParquetFunc{})
func (sb *SchemaBuilder) TableFunc(fn catalog.TableFunction) *SchemaBuilder

// Schema starts a new schema definition (returns to CatalogBuilder).
// Finalizes current schema and begins new one.
// Allows chaining: Schema("a").Table(...).Schema("b").Table(...)
func (sb *SchemaBuilder) Schema(name string) *SchemaBuilder

// Build finalizes the catalog (returns to CatalogBuilder).
// Same as calling catalogBuilder.Build().
func (sb *SchemaBuilder) Build() (catalog.Catalog, error)
```

## SimpleTableDef Struct

```go
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
```

## Example Usage

### Single Schema, Single Table

```go
func buildCatalog() catalog.Catalog {
    cat, err := airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(SimpleTableDef{
                Name:    "users",
                Comment: "User accounts",
                Schema: arrow.NewSchema([]arrow.Field{
                    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
                    {Name: "name", Type: arrow.BinaryTypes.String},
                    {Name: "email", Type: arrow.BinaryTypes.String},
                }, nil),
                ScanFunc: scanUsers,
            }).
        Build()

    if err != nil {
        log.Fatal(err)
    }

    return cat
}

func scanUsers(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Fetch data from database, API, etc.
    rows, err := db.QueryContext(ctx, "SELECT id, name, email FROM users")
    if err != nil {
        return nil, err
    }

    return convertToArrowReader(rows), nil
}
```

### Multiple Schemas, Multiple Tables

```go
func buildCatalog() catalog.Catalog {
    return airport.NewCatalogBuilder().
        Schema("main").
            Comment("Production data").
            SimpleTable(SimpleTableDef{
                Name:     "users",
                Schema:   userSchema,
                ScanFunc: scanUsers,
            }).
            SimpleTable(SimpleTableDef{
                Name:     "orders",
                Schema:   orderSchema,
                ScanFunc: scanOrders,
            }).
        Schema("staging").
            Comment("Staging data").
            SimpleTable(SimpleTableDef{
                Name:     "users",
                Schema:   userSchema,
                ScanFunc: scanStagingUsers,
            }).
        Build()
}
```

### Schema with Tables and Scalar Functions

```go
func buildCatalog() catalog.Catalog {
    return airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(SimpleTableDef{
                Name:     "users",
                Schema:   userSchema,
                ScanFunc: scanUsers,
            }).
            ScalarFunc(&UppercaseFunc{}).
            ScalarFunc(&HashFunc{}).
        Build()
}

type UppercaseFunc struct{}

func (f *UppercaseFunc) Name() string { return "UPPERCASE" }
func (f *UppercaseFunc) Comment() string { return "Convert to uppercase" }

func (f *UppercaseFunc) Signature() catalog.FunctionSignature {
    return catalog.FunctionSignature{
        Parameters: []arrow.DataType{arrow.BinaryTypes.String},
        ReturnType: arrow.BinaryTypes.String,
    }
}

func (f *UppercaseFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
    // Vectorized execution on entire batch
    inputCol := input.Column(0).(*array.String)

    builder := array.NewStringBuilder(memory.DefaultAllocator)
    defer builder.Release()

    for i := 0; i < inputCol.Len(); i++ {
        if inputCol.IsNull(i) {
            builder.AppendNull()
        } else {
            builder.Append(strings.ToUpper(inputCol.Value(i)))
        }
    }

    resultCol := builder.NewArray()
    defer resultCol.Release()

    resultSchema := arrow.NewSchema([]arrow.Field{
        {Name: "result", Type: arrow.BinaryTypes.String},
    }, nil)

    return array.NewRecord(resultSchema, []arrow.Array{resultCol}, int64(inputCol.Len())), nil
}
```

### Schema with Table Function

```go
func buildCatalog() catalog.Catalog {
    return airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(SimpleTableDef{
                Name:     "users",
                Schema:   userSchema,
                ScanFunc: scanUsers,
            }).
            TableFunc(&ReadParquetFunc{}).
        Build()
}

type ReadParquetFunc struct{}

func (f *ReadParquetFunc) Name() string { return "READ_PARQUET" }
func (f *ReadParquetFunc) Comment() string { return "Read Parquet file" }

func (f *ReadParquetFunc) Signature() catalog.FunctionSignature {
    return catalog.FunctionSignature{
        Parameters: []arrow.DataType{arrow.BinaryTypes.String}, // file path
        ReturnType: nil, // Table function
    }
}

func (f *ReadParquetFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
    filePath := params[0].(string)

    // Read schema from Parquet metadata (without loading all data)
    file, err := parquet.OpenFile(filePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    return file.Schema(), nil
}

func (f *ReadParquetFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
    filePath := params[0].(string)

    file, err := parquet.OpenFile(filePath)
    if err != nil {
        return nil, err
    }

    return file.RecordReader(), nil
}
```

### Dynamic Schema Table (Time Travel)

```go
func buildCatalog() catalog.Catalog {
    return airport.NewCatalogBuilder().
        Schema("main").
            DynamicTable(&TimeSeriesTable{
                name:    "metrics",
                backend: myTimeSeriesDB,
            }).
        Build()
}

type TimeSeriesTable struct {
    name    string
    backend TimeSeriesBackend
}

func (t *TimeSeriesTable) Name() string { return t.name }
func (t *TimeSeriesTable) Comment() string { return "Time-series metrics" }

// Return nil for dynamic schema
func (t *TimeSeriesTable) ArrowSchema() *arrow.Schema {
    return nil
}

// Provide schema for specific time point
func (t *TimeSeriesTable) SchemaForRequest(ctx context.Context, req *catalog.SchemaRequest) (*arrow.Schema, error) {
    if req.TimePoint == nil {
        return t.backend.CurrentSchema(ctx)
    }

    return t.backend.SchemaAtTime(ctx, req.TimePoint)
}

func (t *TimeSeriesTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    return t.backend.ScanAtTime(ctx, opts.TimePoint, opts)
}
```

## Validation Rules

Builder MUST validate:
1. Schema names are non-empty and unique
2. Table names are non-empty and unique within schema
3. Function names are non-empty and unique within schema (across scalar + table functions)
4. SimpleTableDef fields are valid (non-nil Schema, non-nil ScanFunc)
5. ArrowSchema has at least 1 field

Builder MUST return error from `Build()` if validation fails.

## Builder State Machine

```
Empty → HasSchemas → Built
         ↓           (terminal)
         ↑
      (looping on Schema())
```

- **Empty**: No schemas added yet
- **HasSchemas**: At least one schema defined
- **Built**: `Build()` called, catalog returned, builder unusable

Calling `Schema()` or `SimpleTable()` after `Build()` returns error.

## Thread Safety

- Builder is NOT thread-safe (single-threaded initialization only)
- Built catalog IS thread-safe (immutable, concurrent access supported)

## Testing Recommendations

```go
func TestBuilderSingleSchema(t *testing.T) {
    cat, err := airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(SimpleTableDef{
                Name:     "test",
                Schema:   testSchema,
                ScanFunc: mockScan,
            }).
        Build()

    assert.NoError(t, err)
    assert.NotNil(t, cat)

    schemas, _ := cat.Schemas(context.Background())
    assert.Len(t, schemas, 1)
    assert.Equal(t, "main", schemas[0].Name())
}

func TestBuilderDuplicateSchemaNames(t *testing.T) {
    _, err := airport.NewCatalogBuilder().
        Schema("main").SimpleTable(testTable1).
        Schema("main").SimpleTable(testTable2). // Duplicate!
        Build()

    assert.Error(t, err)
}

func TestBuilderInvalidTableDef(t *testing.T) {
    _, err := airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(SimpleTableDef{
                Name:     "test",
                Schema:   nil, // Invalid!
                ScanFunc: mockScan,
            }).
        Build()

    assert.Error(t, err)
}
```

## Performance Considerations

- Builder operations are O(1) append (no validation until `Build()`)
- `Build()` validates all names/schemas (O(n) where n = total entities)
- Built catalog uses hash maps for O(1) lookups by name
- Built catalog is immutable (no locks needed for reads)
