# API Guide

This document provides a comprehensive reference for the airport-go public API.

## Package Structure

```text
github.com/hugr-lab/airport-go
├── airport.go          # Main package: Server, CatalogBuilder
├── catalog/            # Catalog interfaces, geometry support
├── auth/               # Authentication implementations
├── filter/             # Filter pushdown parsing and encoding
└── flight/             # Flight handler (internal)
```

## Core Interfaces

### catalog.Catalog

The root interface for exposing data. Every Airport server requires a Catalog implementation.

```go
type Catalog interface {
    // Schemas returns all schemas in this catalog.
    // Context may contain auth info for permission-based filtering.
    // Returns empty slice (not nil) if no schemas available.
    Schemas(ctx context.Context) ([]Schema, error)

    // Schema returns a specific schema by name.
    // Returns (nil, nil) if schema doesn't exist (not an error).
    // Returns (nil, err) if lookup fails for other reasons.
    Schema(ctx context.Context, name string) (Schema, error)
}
```

### catalog.NamedCatalog

Optional interface that provides a name for the catalog. When implemented, DuckDB can use the catalog name in ATTACH statements.

```go
type NamedCatalog interface {
    Catalog

    // Name returns the catalog name (e.g., "default", "analytics").
    // MUST return string (can be empty).
    Name() string
}
```

**Usage in DuckDB:**

```sql
-- When server implements NamedCatalog returning "mydata":
ATTACH 'mydata' AS analytics (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

-- Query using the alias
SELECT * FROM analytics.schema.table;
```

**Example Implementation:**

```go
type MyCatalog struct {
    // ... fields
}

func (c *MyCatalog) Name() string {
    return "mydata"
}

func (c *MyCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    // ... implementation
}

func (c *MyCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
    // ... implementation
}
```

### catalog.Schema

Represents a namespace containing tables and functions.

```go
type Schema interface {
    // Name returns the schema name (e.g., "main", "information_schema").
    Name() string

    // Comment returns optional schema documentation.
    Comment() string

    // Tables returns all tables in this schema.
    // Returns empty slice (not nil) if no tables available.
    Tables(ctx context.Context) ([]Table, error)

    // Table returns a specific table by name.
    // Returns (nil, nil) if table doesn't exist (not an error).
    Table(ctx context.Context, name string) (Table, error)

    // ScalarFunctions returns all scalar functions in this schema.
    ScalarFunctions(ctx context.Context) ([]ScalarFunction, error)

    // TableFunctions returns all table-valued functions in this schema.
    TableFunctions(ctx context.Context) ([]TableFunction, error)

    // TableFunctionsInOut returns all table functions that accept row sets as input.
    TableFunctionsInOut(ctx context.Context) ([]TableFunctionInOut, error)
}
```

### catalog.Table

Represents a scannable table.

```go
type Table interface {
    // Name returns the table name.
    Name() string

    // Comment returns an optional description.
    Comment() string

    // ArrowSchema returns the table's Arrow schema.
    // If columns is non-nil, returns a projected schema with only those columns.
    // Column order in the returned schema matches the order in columns slice.
    ArrowSchema(columns []string) *arrow.Schema

    // Scan returns a RecordReader for reading table data.
    // IMPORTANT: Must return data matching the full schema, not projected.
    // DuckDB handles column projection client-side.
    // Caller MUST call reader.Release() to free memory.
    Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}
```

### catalog.ScanOptions

Options passed to Table.Scan:

```go
type ScanOptions struct {
    // Columns is the list of columns requested by the client.
    // If nil or empty, client wants all columns.
    // NOTE: Scan must still return full schema data.
    Columns []string

    // Filter contains a serialized JSON predicate expression from DuckDB.
    // nil means no filtering (return all rows).
    // See "Filter Pushdown" section below for format details.
    Filter []byte

    // Limit is maximum rows to return.
    // 0 or negative means no limit.
    Limit int64

    // BatchSize is a hint for RecordReader batch size.
    // 0 means implementation chooses default.
    BatchSize int

    // TimePoint specifies point-in-time for time-travel queries.
    // nil for "current" time (no time travel).
    TimePoint *TimePoint
}

type TimePoint struct {
    Unit  string // "timestamp", "version", or "snapshot"
    Value string // Time value in appropriate format
}
```

### Filter Pushdown

The `ScanOptions.Filter` field contains a JSON-serialized predicate expression that represents WHERE clause filters pushed down from DuckDB. Implementing filter pushdown can significantly improve query performance by reducing data transfer.

**JSON Structure:**

```json
{
  "filters": [...],
  "column_binding_names_by_index": ["column1", "column2", ...]
}
```

**Expression Types:**

| Expression Class | Description | Key Fields |
|-----------------|-------------|------------|
| `BOUND_COMPARISON` | Comparison operators (=, >, <, >=, <=, !=) | `type`: COMPARE_EQUAL, COMPARE_GREATERTHAN, etc. |
| `BOUND_COLUMN_REF` | Column references | `binding.table_index`, `binding.column_index` |
| `BOUND_CONSTANT` | Literal values | `value`, `return_type.id` |
| `BOUND_CONJUNCTION` | Logical operators | `type`: CONJUNCTION_AND, CONJUNCTION_OR |
| `BOUND_FUNCTION` | Function calls | `function.name`, `children` |

**Example Implementation:**

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    if opts.Filter != nil {
        // Parse the JSON filter
        var filterExpr struct {
            Filters              []json.RawMessage `json:"filters"`
            ColumnBindingNames   []string          `json:"column_binding_names_by_index"`
        }
        if err := json.Unmarshal(opts.Filter, &filterExpr); err != nil {
            // Fall back to unfiltered scan
            return t.scanAll(ctx)
        }

        // Interpret filter expressions and push to your data source
        // e.g., convert to SQL WHERE clause, API query parameters, etc.
    }

    return t.scanAll(ctx)
}
```

**Note:** Currently, implementations must parse the raw JSON manually. Future versions of airport-go will provide helper types and functions for filter interpretation, including:

- Type-safe expression tree structures
- Operator and value extraction utilities
- SQL/query builder helpers

For the complete JSON format specification, see the [Airport Extension documentation](https://airport.query.farm/server_predicate_pushdown.html).

## Table References

### catalog.TableRef

Represents a read-only table that delegates data reading to DuckDB function calls via `data://` endpoint URIs. Instead of serving data through Arrow Flight DoGet, a TableRef returns function calls that DuckDB executes locally.

```go
type TableRef interface {
    // Name returns the table reference name.
    Name() string

    // Comment returns optional documentation.
    Comment() string

    // ArrowSchema returns the Arrow schema describing the columns.
    ArrowSchema() *arrow.Schema

    // FunctionCalls generates DuckDB function calls for the given request.
    // Returns at least one function call. Multiple calls enable parallel reads.
    FunctionCalls(ctx context.Context, req *FunctionCallRequest) ([]FunctionCall, error)
}
```

### catalog.DynamicSchemaTableRef

Optional extension for table references with parameter-dependent or time-dependent schemas:

```go
type DynamicSchemaTableRef interface {
    TableRef

    // SchemaForRequest returns the schema for a specific request context.
    SchemaForRequest(ctx context.Context, req *SchemaRequest) (*arrow.Schema, error)
}
```

### catalog.FunctionCallRequest

Contains the scan context passed to a TableRef:

```go
type FunctionCallRequest struct {
    Filters    string       // JSON filter predicates (empty = no filters)
    Columns    []string     // Column names for projection (nil = all columns)
    Parameters []any        // Function parameters from Arrow IPC
    TimePoint  *TimePoint   // Point-in-time for time-travel queries (nil = current)
}
```

### catalog.FunctionCall and FunctionCallArg

Represents a DuckDB function call and its arguments:

```go
type FunctionCall struct {
    FunctionName string            // DuckDB function name (e.g., "read_csv")
    Args         []FunctionCallArg // Function arguments
}

type FunctionCallArg struct {
    Name  string         // Parameter name (empty = positional arg)
    Value any            // Argument value
    Type  arrow.DataType // Arrow data type for encoding
}
```

Supported Value types: `string`, `bool`, `int`, `int8`-`int64`, `uint8`-`uint64`, `float32`, `float64`, `time.Time`, `orb.Geometry`, `[]byte`, `[]any`, `map[string]any`.

### catalog.SchemaWithTableRefs

Optional extension interface for schemas containing table references:

```go
type SchemaWithTableRefs interface {
    // TableRefs returns all table references in this schema.
    TableRefs(ctx context.Context) ([]TableRef, error)

    // TableRef returns a specific table reference by name.
    // Returns (nil, nil) if not found.
    TableRef(ctx context.Context, name string) (TableRef, error)
}
```

### Using Table References with CatalogBuilder

```go
cat, err := airport.NewCatalogBuilder().
    Schema("data").
    TableRef(&myCSVRef{}).
    TableRef(&myParquetRef{}).
    Build()
```

**Example Implementation:**

```go
type csvRef struct {
    url string
}

func (r *csvRef) Name() string              { return "orders" }
func (r *csvRef) Comment() string            { return "Order data" }
func (r *csvRef) ArrowSchema() *arrow.Schema { return orderSchema }

func (r *csvRef) FunctionCalls(ctx context.Context, req *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
    return []catalog.FunctionCall{
        {
            FunctionName: "read_csv",
            Args: []catalog.FunctionCallArg{
                {Value: r.url, Type: arrow.BinaryTypes.String},
                {Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
            },
        },
    }, nil
}
```

See [examples/tableref](../examples/tableref/) for a complete example.

## DML Interfaces

### catalog.InsertableTable

Extends Table with data insertion capability.

```go
type InsertableTable interface {
    Table

    // Insert adds new rows to the table.
    // The rows RecordReader provides batches of data to insert.
    // opts contains RETURNING clause information.
    // Returns DMLResult with affected row count and optional returning data.
    // Caller MUST call rows.Release() after Insert returns.
    Insert(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### catalog.UpdatableTable

Extends Table with data update capability.

```go
type UpdatableTable interface {
    Table

    // Update modifies existing rows identified by rowIDs.
    // The rows RecordReader provides replacement data for matched rows.
    // Row order in RecordReader must correspond to rowIDs order.
    // opts contains RETURNING clause information.
    // Returns DMLResult with affected row count.
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}
```

### catalog.DeletableTable

Extends Table with data deletion capability.

```go
type DeletableTable interface {
    Table

    // Delete removes rows identified by rowIDs.
    // opts contains RETURNING clause information.
    // Returns DMLResult with affected row count.
    Delete(ctx context.Context, rowIDs []int64, opts *DMLOptions) (*DMLResult, error)
}
```

### catalog.UpdatableBatchTable

Alternative UPDATE interface where the rowid column is embedded in the RecordBatch.
This interface is preferred over `UpdatableTable` when both are implemented.

```go
type UpdatableBatchTable interface {
    Table

    // Update modifies existing rows using data from the RecordBatch.
    // The rows RecordBatch contains both the rowid column (identifying rows to update)
    // and the new column values. Implementations MUST extract rowid values from
    // the rowid column (identified by name "rowid" or metadata key "is_rowid").
    // Use FindRowIDColumn(rows.Schema()) to locate the rowid column.
    // Implementations MUST return ErrNullRowID if any rowid value is null.
    // Row order in RecordBatch determines update order.
    // opts contains RETURNING clause information.
    // Returns DMLResult with affected row count and optional returning data.
    // Caller MUST call rows.Release() after Update returns.
    Update(ctx context.Context, rows arrow.RecordBatch, opts *DMLOptions) (*DMLResult, error)
}
```

### catalog.DeletableBatchTable

Alternative DELETE interface where the rowid column is embedded in the RecordBatch.
This interface is preferred over `DeletableTable` when both are implemented.

```go
type DeletableBatchTable interface {
    Table

    // Delete removes rows identified by rowid values in the RecordBatch.
    // The rows RecordBatch contains the rowid column (identified by name "rowid"
    // or metadata key "is_rowid") that identifies rows to delete.
    // Use FindRowIDColumn(rows.Schema()) to locate the rowid column.
    // Implementations MUST return ErrNullRowID if any rowid value is null.
    // opts contains RETURNING clause information.
    // Returns DMLResult with affected row count and optional returning data.
    // Caller MUST call rows.Release() after Delete returns.
    Delete(ctx context.Context, rows arrow.RecordBatch, opts *DMLOptions) (*DMLResult, error)
}
```

### catalog.ErrNullRowID

Sentinel error for null rowid values:

```go
// ErrNullRowID is returned when a null rowid value is encountered in UPDATE or DELETE operations.
var ErrNullRowID = errors.New("null rowid value not allowed")
```

### catalog.FindRowIDColumn

Helper function to locate the rowid column in a schema:

```go
// FindRowIDColumn returns the index of the rowid column in the schema.
// Returns -1 if no rowid column is found.
//
// Rowid column is identified by:
//   - Column name "rowid" (case-sensitive), or
//   - Metadata key "is_rowid" with non-empty value
func FindRowIDColumn(schema *arrow.Schema) int
```

**Example usage:**

```go
func (t *MyTable) Update(ctx context.Context, rows arrow.RecordBatch, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    rowidIdx := catalog.FindRowIDColumn(rows.Schema())
    if rowidIdx == -1 {
        return nil, errors.New("rowid column required")
    }

    // Check for null rowids
    rowidCol := rows.Column(rowidIdx)
    if rowidCol.NullN() > 0 {
        return nil, catalog.ErrNullRowID
    }

    // Direct access to RecordBatch - no iterator needed
    rowidArr := rowidCol.(*array.Int64)
    for i := 0; i < int(rows.NumRows()); i++ {
        rowID := rowidArr.Value(i)
        // Process update for rowID...
    }

    return &catalog.DMLResult{AffectedRows: rows.NumRows()}, nil
}
```

### Choosing Between Legacy and Batch Interfaces

| Interface | rowID Handling | Use Case |
|-----------|----------------|----------|
| `UpdatableTable` | rowIDs passed as separate `[]int64` slice | Simpler implementation when rowIDs are processed independently |
| `UpdatableBatchTable` | rowIDs embedded in RecordReader | When you need access to rowIDs alongside data columns |
| `DeletableTable` | rowIDs passed as separate `[]int64` slice | Simpler implementation for deletion |
| `DeletableBatchTable` | rowIDs embedded in RecordReader | When deletion logic needs the full batch context |

**Interface Priority:** When a table implements both legacy and batch interfaces, the batch interface is always preferred.

### catalog.DMLOptions

Options for DML operations:

```go
type DMLOptions struct {
    // Returning indicates whether a RETURNING clause was specified.
    // When true, the implementation should populate DMLResult.ReturningData.
    Returning bool

    // ReturningColumns specifies which columns to include in RETURNING results.
    // Only meaningful when Returning is true.
    //
    // IMPORTANT: DuckDB Airport extension does NOT communicate which specific
    // columns are in the RETURNING clause (e.g., "RETURNING id" vs "RETURNING *").
    // The protocol only sends a boolean flag (return-chunks header).
    //
    // The server populates ReturningColumns with ALL table column names
    // (excluding pseudo-columns like rowid) to indicate "return all columns".
    // DuckDB handles column projection CLIENT-SIDE: the server returns all
    // available columns, and DuckDB filters to only the requested columns.
    ReturningColumns []string
}
```

### catalog.DMLResult

Result of DML operations:

```go
type DMLResult struct {
    // AffectedRows is the count of rows inserted, updated, or deleted.
    AffectedRows int64

    // ReturningData contains rows affected by the operation when
    // a RETURNING clause was specified. nil if no RETURNING requested.
    // Caller is responsible for releasing resources (RecordReader.Release).
    ReturningData array.RecordReader
}
```

**Example Implementation:**

```go
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
    var insertedRows []MyRow

    // Process input rows
    for rows.Next() {
        batch := rows.RecordBatch()
        // Insert data and track inserted rows for RETURNING
        insertedRows = append(insertedRows, t.insertBatch(batch)...)
    }

    result := &catalog.DMLResult{
        AffectedRows: int64(len(insertedRows)),
    }

    // Build RETURNING data if requested
    if opts != nil && opts.Returning && len(insertedRows) > 0 {
        // Project schema to ReturningColumns
        returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
        result.ReturningData = t.buildReturningReader(returningSchema, insertedRows)
    }

    return result, nil
}
```

## DDL Interfaces

### catalog.DynamicCatalog

Extends Catalog with schema management:

```go
type DynamicCatalog interface {
    Catalog

    // CreateSchema creates a new schema in the catalog.
    // Returns ErrAlreadyExists if schema exists.
    CreateSchema(ctx context.Context, name string, opts CreateSchemaOptions) (Schema, error)

    // DropSchema removes a schema from the catalog.
    // Returns ErrNotFound if schema doesn't exist and IgnoreNotFound is false.
    // Returns ErrSchemaNotEmpty if schema contains tables.
    DropSchema(ctx context.Context, name string, opts DropSchemaOptions) error
}

type CreateSchemaOptions struct {
    Comment string            // Optional documentation
    Tags    map[string]string // Optional metadata
}

type DropSchemaOptions struct {
    IgnoreNotFound bool // Don't error if schema doesn't exist
}
```

### catalog.DynamicSchema

Extends Schema with table management:

```go
type DynamicSchema interface {
    Schema

    // CreateTable creates a new table in the schema.
    // Returns ErrAlreadyExists if table exists and OnConflict is OnConflictError.
    CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts CreateTableOptions) (Table, error)

    // DropTable removes a table from the schema.
    // Returns ErrNotFound if table doesn't exist and IgnoreNotFound is false.
    DropTable(ctx context.Context, name string, opts DropTableOptions) error

    // RenameTable renames a table in the schema.
    // Returns ErrNotFound if table doesn't exist and IgnoreNotFound is false.
    // Returns ErrAlreadyExists if newName already exists.
    RenameTable(ctx context.Context, oldName, newName string, opts RenameTableOptions) error
}

type OnConflict string

const (
    OnConflictError   OnConflict = "error"   // Return error if exists
    OnConflictIgnore  OnConflict = "ignore"  // Silently succeed if exists
    OnConflictReplace OnConflict = "replace" // Drop and recreate
)

type CreateTableOptions struct {
    OnConflict         OnConflict // Behavior when table exists
    Comment            string     // Optional documentation
    NotNullConstraints []uint64   // Column indices with NOT NULL
    UniqueConstraints  []uint64   // Column indices that must be unique
    CheckConstraints   []string   // SQL check expressions
}

type DropTableOptions struct {
    IgnoreNotFound bool // Don't error if table doesn't exist
}

type RenameTableOptions struct {
    IgnoreNotFound bool // Don't error if table doesn't exist
}
```

### catalog.DynamicTable

Extends Table with column management:

```go
type DynamicTable interface {
    Table

    // AddColumn adds a new column to the table.
    // The columnSchema should contain a single field defining the column.
    AddColumn(ctx context.Context, columnSchema *arrow.Schema, opts AddColumnOptions) error

    // RemoveColumn removes a column from the table.
    RemoveColumn(ctx context.Context, name string, opts RemoveColumnOptions) error

    // RenameColumn renames a column in the table.
    RenameColumn(ctx context.Context, oldName, newName string, opts RenameColumnOptions) error

    // ChangeColumnType changes the type of a column.
    // The columnSchema should contain a single field with the new type.
    // The expression is a SQL expression for type conversion.
    ChangeColumnType(ctx context.Context, columnSchema *arrow.Schema, expression string, opts ChangeColumnTypeOptions) error

    // SetNotNull adds a NOT NULL constraint to a column.
    SetNotNull(ctx context.Context, columnName string, opts SetNotNullOptions) error

    // DropNotNull removes a NOT NULL constraint from a column.
    DropNotNull(ctx context.Context, columnName string, opts DropNotNullOptions) error

    // SetDefault sets or changes the default value of a column.
    SetDefault(ctx context.Context, columnName, expression string, opts SetDefaultOptions) error

    // AddField adds a field to a struct-typed column.
    AddField(ctx context.Context, columnSchema *arrow.Schema, opts AddFieldOptions) error

    // RenameField renames a field in a struct-typed column.
    RenameField(ctx context.Context, columnPath []string, newName string, opts RenameFieldOptions) error

    // RemoveField removes a field from a struct-typed column.
    RemoveField(ctx context.Context, columnPath []string, opts RemoveFieldOptions) error
}

type AddColumnOptions struct {
    IfColumnNotExists bool // Don't error if column exists
    IgnoreNotFound    bool // Don't error if table doesn't exist
}

type RemoveColumnOptions struct {
    IfColumnExists bool // Don't error if column doesn't exist
    IgnoreNotFound bool // Don't error if table doesn't exist
    Cascade        bool // Remove dependent objects
}

type RenameColumnOptions struct {
    IgnoreNotFound bool // Don't error if table/column doesn't exist
}

type ChangeColumnTypeOptions struct {
    IgnoreNotFound bool
}

type SetNotNullOptions struct {
    IgnoreNotFound bool
}

type DropNotNullOptions struct {
    IgnoreNotFound bool
}

type SetDefaultOptions struct {
    IgnoreNotFound bool
}

type AddFieldOptions struct {
    IgnoreNotFound   bool
    IfFieldNotExists bool
}

type RenameFieldOptions struct {
    IgnoreNotFound bool
}

type RemoveFieldOptions struct {
    IgnoreNotFound bool // Don't error if table/column doesn't exist
}
```

## Statistics Interface

### catalog.StatisticsTable

Enables DuckDB query optimization through column statistics:

```go
type StatisticsTable interface {
    Table

    // ColumnStatistics returns statistics for a specific column.
    // columnName identifies the column to get statistics for.
    // columnType is the DuckDB type name (e.g., "VARCHAR", "INTEGER").
    // Returns ColumnStats with nil fields for unavailable statistics.
    // Returns ErrNotFound if the column doesn't exist.
    ColumnStatistics(ctx context.Context, columnName string, columnType string) (*ColumnStats, error)
}

type ColumnStats struct {
    HasNotNull      *bool   // Column contains non-null values
    HasNull         *bool   // Column contains null values
    DistinctCount   *uint64 // Approximate unique value count
    Min             any     // Minimum value (type matches column)
    Max             any     // Maximum value (type matches column)
    MaxStringLength *uint64 // Max string length (string columns only)
    ContainsUnicode *bool   // Has unicode chars (string columns only)
}
```

## Function Interfaces

### catalog.ScalarFunction

Custom scalar functions executed via DoExchange:

```go
type ScalarFunction interface {
    // Name returns the function name (e.g., "UPPERCASE").
    Name() string

    // Comment returns optional documentation.
    Comment() string

    // Signature returns the function signature.
    Signature() FunctionSignature

    // Execute runs the function on input record batch and returns result array.
    // Input record columns match parameter types from Signature.
    // Returned array matches return type from Signature.
    // Returned array length equals input record row count.
    Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error)
}

type FunctionSignature struct {
    Parameters []arrow.DataType // Parameter types (in order)
    ReturnType arrow.DataType   // Return type (nil for table functions)
}
```

### catalog.TableFunction

Functions that return tables:

```go
type TableFunction interface {
    // Name returns the function name.
    Name() string

    // Comment returns optional documentation.
    Comment() string

    // Signature returns the function signature.
    Signature() FunctionSignature

    // SchemaForParameters returns the output schema for given parameters.
    // Parameters are MessagePack-decoded values matching Signature.
    SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error)

    // Execute runs the table function and returns a RecordReader.
    Execute(ctx context.Context, params []any, opts *ScanOptions) (array.RecordReader, error)
}
```

### catalog.TableFunctionInOut

Table functions that accept row sets as input:

```go
type TableFunctionInOut interface {
    // Name returns the function name.
    Name() string

    // Comment returns optional documentation.
    Comment() string

    // Signature returns the function signature.
    // First N-1 parameters are scalars.
    // Last parameter type describes the input row schema.
    Signature() FunctionSignature

    // SchemaForParameters returns output schema based on params and input schema.
    SchemaForParameters(ctx context.Context, params []any, inputSchema *arrow.Schema) (*arrow.Schema, error)

    // Execute processes input rows and returns output rows.
    Execute(ctx context.Context, params []any, input array.RecordReader, opts *ScanOptions) (array.RecordReader, error)
}
```

## Versioned Catalog

### catalog.VersionedCatalog

Enables catalog version tracking for cache invalidation:

```go
type VersionedCatalog interface {
    Catalog

    // CatalogVersion returns the current version of the catalog.
    // When version changes, clients refresh their cached schema.
    CatalogVersion(ctx context.Context) (CatalogVersion, error)
}

type CatalogVersion struct {
    Version uint64 // Current version number
    IsFixed bool   // If true, version is fixed for session
}
```

## Transaction Support

### catalog.TransactionManager

Optional interface for transaction coordination:

```go
type TransactionManager interface {
    // BeginTransaction creates a new transaction.
    BeginTransaction(ctx context.Context) (txID string, err error)

    // CommitTransaction marks transaction as complete.
    CommitTransaction(ctx context.Context, txID string) error

    // RollbackTransaction aborts a transaction.
    RollbackTransaction(ctx context.Context, txID string) error

    // GetTransactionStatus returns current transaction state.
    GetTransactionStatus(ctx context.Context, txID string) (TransactionState, bool)
}

type TransactionState string

const (
    TransactionActive    TransactionState = "active"
    TransactionCommitted TransactionState = "committed"
    TransactionAborted   TransactionState = "aborted"
)
```

## CatalogBuilder

The fluent builder for creating static catalogs:

```go
// Create a new builder
builder := airport.NewCatalogBuilder()

// Add a schema
builder.Schema("my_schema")

// Add a simple table with scan function
builder.SimpleTable(airport.SimpleTableDef{
    Name:     "my_table",
    Comment:  "Description",
    Schema:   arrowSchema,
    ScanFunc: func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
        // Return data
    },
})

// Add a table implementing catalog.Table interface
builder.Table(myTableImpl)

// Add a table reference (data:// URI delegation)
builder.TableRef(myTableRef)

// Add a scalar function
builder.ScalarFunc(airport.ScalarFuncDef{
    Name:         "my_func",
    Comment:      "Description",
    InputSchema:  inputSchema,
    OutputSchema: outputSchema,
    CallFunc: func(ctx context.Context, args arrow.RecordBatch) (arrow.Array, error) {
        // Execute function
    },
})

// Build the catalog
catalog, err := builder.Build()
```

### SimpleTableDef

```go
type SimpleTableDef struct {
    Name     string
    Comment  string
    Schema   *arrow.Schema
    ScanFunc func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error)
}
```

### ScalarFuncDef

```go
type ScalarFuncDef struct {
    Name         string
    Comment      string
    InputSchema  *arrow.Schema
    OutputSchema *arrow.Schema
    CallFunc     func(ctx context.Context, args arrow.RecordBatch) (arrow.Array, error)
}
```

### TableFuncDef

```go
type TableFuncDef struct {
    Name         string
    Comment      string
    InputSchema  *arrow.Schema
    OutputSchema *arrow.Schema
    CallFunc     func(ctx context.Context, args arrow.RecordBatch) (array.RecordReader, error)
}
```

## Server Configuration

### ServerConfig

```go
type ServerConfig struct {
    // Catalog is the catalog implementation (required)
    Catalog catalog.Catalog

    // Auth is the authentication handler (optional)
    Auth Authenticator

    // Address is the server address for FlightEndpoint locations
    Address string

    // MaxMessageSize sets the maximum gRPC message size (default: 4MB)
    MaxMessageSize int

    // LogLevel sets the logging verbosity (default: Info)
    LogLevel *slog.Level
}
```

### Creating a Server

```go
// Create gRPC server with Airport options
config := airport.ServerConfig{
    Catalog:        myCatalog,
    Auth:           myAuth,
    MaxMessageSize: 16 * 1024 * 1024, // 16MB
}

opts := airport.ServerOptions(config)
grpcServer := grpc.NewServer(opts...)

// Register Airport service
err := airport.NewServer(grpcServer, config)
if err != nil {
    log.Fatal(err)
}

// Start serving
lis, _ := net.Listen("tcp", ":50051")
grpcServer.Serve(lis)
```

### MultiCatalogServerConfig

For servers that need to serve multiple catalogs, use `MultiCatalogServerConfig`:

```go
type MultiCatalogServerConfig struct {
    // Catalogs is the list of initial catalogs to serve. Optional (can be empty).
    // Each catalog should implement catalog.NamedCatalog for routing.
    // Catalog names must be unique (including empty string for default).
    // Additional catalogs can be added at runtime via AddCatalog().
    Catalogs []catalog.Catalog

    // Allocator for Arrow memory management. Optional, defaults to DefaultAllocator.
    Allocator memory.Allocator

    // Logger for server events. Optional, defaults to slog with Info level.
    Logger *slog.Logger

    // LogLevel sets the minimum log level. Optional, defaults to Info.
    // Only used if Logger is nil.
    LogLevel *slog.Level

    // Address is the server's public address for FlightEndpoint locations.
    // Optional, defaults to reuse connection.
    Address string

    // TransactionManager coordinates transactions across catalogs. Optional.
    // Must implement CatalogTransactionManager for multi-catalog support.
    TransactionManager catalog.CatalogTransactionManager

    // Auth is the authenticator for validating requests. Optional.
    // If the authenticator also implements CatalogAuthorizer, AuthorizeCatalog
    // is called after Authenticate to perform per-catalog authorization.
    Auth auth.Authenticator

    // MaxMessageSize is the maximum gRPC message size. Optional.
    MaxMessageSize int
}
```

### Creating a Multi-Catalog Server

```go
// Define catalogs (must implement catalog.NamedCatalog)
salesCatalog := NewSalesCatalog()      // Name() returns "sales"
analyticsCatalog := NewAnalyticsCatalog() // Name() returns "analytics"

// Create server configuration
config := airport.MultiCatalogServerConfig{
    Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
    Auth:     myCatalogAwareAuth, // Optional: implements CatalogAuthorizer
}

// Create gRPC server with options
opts := airport.MultiCatalogServerOptions(config)
grpcServer := grpc.NewServer(opts...)

// Register multi-catalog Airport service
// Returns *MultiCatalogServer which can be used to add/remove catalogs at runtime
mcs, err := airport.NewMultiCatalogServer(grpcServer, config)
if err != nil {
    log.Fatal(err)
}

// Dynamic catalog management (can be called while server is running):
//
// Add a new catalog at runtime:
//   newCatalog := NewInventoryCatalog() // Must implement catalog.NamedCatalog
//   if err := mcs.AddCatalog(newCatalog); err != nil {
//       log.Printf("Failed to add catalog: %v", err)
//   }
//
// Remove a catalog by name:
//   if err := mcs.RemoveCatalog("inventory"); err != nil {
//       log.Printf("Failed to remove catalog: %v", err)
//   }
//
// Note: In-flight requests to a removed catalog complete normally.
// New requests to the removed catalog receive NotFound error.

// Start serving
lis, _ := net.Listen("tcp", ":50051")
grpcServer.Serve(lis)
```

Clients specify the target catalog via the `airport-catalog` gRPC metadata header.
If no header is provided, the request is routed to the default catalog (empty name).

## Authentication

### Authenticator Interface

```go
type Authenticator interface {
    // Authenticate validates credentials and returns an identity.
    // Returns ErrUnauthorized if credentials are invalid.
    Authenticate(ctx context.Context, token string) (string, error)
}
```

### CatalogAuthorizer Interface

For multi-catalog servers, authenticators can implement `CatalogAuthorizer` to provide
per-catalog authorization:

```go
type CatalogAuthorizer interface {
    // AuthorizeCatalog authorizes access to a specific catalog.
    // Called after successful Authenticate() to check catalog-level permissions.
    // Parameters:
    //   - ctx: Request context with identity already set from Authenticate()
    //   - catalog: Target catalog name (empty string for default)
    // Returns:
    //   - ctx: Potentially enriched context (e.g., with catalog-specific claims)
    //   - err: Non-nil if authorization fails (returns gRPC PermissionDenied status)
    AuthorizeCatalog(ctx context.Context, catalog string) (context.Context, error)
}
```

**Example Implementation:**

```go
type MultiCatalogAuth struct {
    // userCatalogs maps user identity to allowed catalog names
    userCatalogs map[string][]string
}

func (a *MultiCatalogAuth) Authenticate(ctx context.Context, token string) (string, error) {
    // Validate token and return identity
    identity, ok := a.validateToken(token)
    if !ok {
        return "", auth.ErrUnauthenticated
    }
    return identity, nil
}

func (a *MultiCatalogAuth) AuthorizeCatalog(ctx context.Context, catalogName string) (context.Context, error) {
    identity := auth.IdentityFromContext(ctx)

    allowedCatalogs, ok := a.userCatalogs[identity]
    if !ok {
        return ctx, errors.New("user has no catalog access")
    }

    for _, allowed := range allowedCatalogs {
        if allowed == catalogName {
            return ctx, nil // Access granted
        }
    }

    return ctx, fmt.Errorf("user %s cannot access catalog %s", identity, catalogName)
}
```

### Built-in Implementations

```go
// Bearer token authentication
auth := airport.BearerAuth(func(token string) (string, error) {
    if token == "valid-token" {
        return "user-identity", nil
    }
    return "", airport.ErrUnauthorized
})

// No authentication (default)
auth := nil
```

### Getting Identity in Handlers

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Get authenticated identity from context
    identity := auth.IdentityFromContext(ctx)
    if identity != "" {
        // User is authenticated
    }
    // ...
}
```

## Utility Functions

### catalog.ProjectSchema

Projects an Arrow schema to include only specified columns:

```go
// Get full schema
fullSchema := table.ArrowSchema(nil)

// Project to specific columns
projected := catalog.ProjectSchema(fullSchema, []string{"id", "name"})
```

### Context Helpers

```go
// Get identity from context (auth package)
identity := auth.IdentityFromContext(ctx)

// Get transaction ID from context (catalog package)
txID, ok := catalog.TransactionIDFromContext(ctx)

// Add transaction ID to context
ctx = catalog.WithTransactionID(ctx, txID)
```

### Request Metadata (flight package)

The `flight` package provides helpers for accessing gRPC metadata from request context:

```go
import "github.com/hugr-lab/airport-go/flight"

// Get catalog name from request metadata (airport-catalog header)
catalogName := flight.CatalogNameFromContext(ctx)

// Get trace ID from request metadata (airport-trace-id header)
traceID := flight.TraceIDFromContext(ctx)

// Get session ID from request metadata (airport-client-session-id header)
sessionID := flight.SessionIDFromContext(ctx)

// Get authorization header value
authHeader := flight.AuthorizationFromContext(ctx)

// Get all metadata at once
meta := flight.MetaFromContext(ctx)
if meta != nil {
    fmt.Printf("Catalog: %s, TraceID: %s\n", meta.CatalogName, meta.TraceID)
}
```

**Metadata Headers:**

| Header | Description | Context Helper |
|--------|-------------|----------------|
| `authorization` | Bearer token for authentication | `AuthorizationFromContext()` |
| `airport-catalog` | Target catalog name for routing | `CatalogNameFromContext()` |
| `airport-trace-id` | Distributed trace identifier | `TraceIDFromContext()` |
| `airport-client-session-id` | Client session identifier | `SessionIDFromContext()` |

### Error Types

```go
var (
    // ErrUnauthorized is returned when authentication fails
    ErrUnauthorized = errors.New("unauthorized")

    // ErrNotFound is returned when a resource doesn't exist
    ErrNotFound = errors.New("not found")

    // ErrAlreadyExists is returned when creating an object that exists
    ErrAlreadyExists = errors.New("already exists")

    // ErrSchemaNotEmpty is returned when dropping non-empty schema
    ErrSchemaNotEmpty = errors.New("schema contains tables")

    // ErrNotImplemented is returned for unsupported operations
    ErrNotImplemented = errors.New("not implemented")
)
```

## Thread Safety

All interface implementations must be safe for concurrent use:

- Multiple goroutines may call Scan simultaneously
- Schema/Table discovery may happen during scans
- DDL operations may occur concurrently with queries

Use appropriate synchronization in your implementations.

## Filter Pushdown Package

The `filter` package enables parsing and encoding DuckDB filter pushdown JSON.

### Parsing Filters

```go
import "github.com/hugr-lab/airport-go/filter"

// Parse filter JSON from ScanOptions.Filter
fp, err := filter.Parse(scanOpts.Filter)
if err != nil {
    return err // Malformed JSON
}

// Access filters (implicitly AND'ed)
for _, f := range fp.Filters {
    // Process filter expression
}

// Resolve column names
colRef := f.(*filter.ColumnRefExpression)
name, err := fp.ColumnName(colRef)
```

### Encoding to SQL

```go
// Create DuckDB encoder
enc := filter.NewDuckDBEncoder(nil)

// Encode all filters to WHERE clause body
sql := enc.EncodeFilters(fp)
if sql != "" {
    query := "SELECT * FROM table WHERE " + sql
}
```

### Column Mapping

```go
// Map column names during encoding
enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
    ColumnMapping: map[string]string{
        "user_id": "uid",           // user_id → uid
        "created": "created_at",    // created → created_at
    },
})
```

### Expression Replacement

```go
// Replace columns with SQL expressions
enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
    ColumnExpressions: map[string]string{
        "full_name": "CONCAT(first_name, ' ', last_name)",
    },
})
```

### Unsupported Expression Handling

The encoder gracefully skips unsupported expressions:
- **AND**: Skips unsupported children, keeps others
- **OR**: If any child unsupported, skips entire OR
- **All unsupported**: Returns empty string

This produces the widest possible filter; DuckDB client applies filters client-side as fallback.

### Expression Types

```go
switch e := expr.(type) {
case *filter.ComparisonExpression:
    // =, <>, <, >, <=, >=, IN, NOT IN, BETWEEN
case *filter.ConjunctionExpression:
    // AND, OR
case *filter.ConstantExpression:
    // Literal values
case *filter.ColumnRefExpression:
    // Column references
case *filter.FunctionExpression:
    // Function calls (LOWER, LENGTH, etc.)
case *filter.CastExpression:
    // Type casts
case *filter.BetweenExpression:
    // BETWEEN expressions
case *filter.OperatorExpression:
    // IS NULL, IS NOT NULL, NOT
case *filter.CaseExpression:
    // CASE WHEN ... END
}
```

See `examples/filter/main.go` for complete examples.

## Geometry (GeoArrow) Support

The `catalog` package provides GeoArrow WKB extension type support for geometry columns, compatible with DuckDB's spatial extension.

### GeometryExtensionType

Arrow extension type for geometry data stored as WKB (Well-Known Binary):

```go
// Create a geometry extension type
extType := catalog.NewGeometryExtensionType()

// Extension name is "geoarrow.wkb" for DuckDB/GeoArrow compatibility
extType.ExtensionName() // "geoarrow.wkb"

// Storage type is Binary
extType.StorageType() // arrow.BinaryTypes.Binary
```

### Creating Geometry Fields

Use `NewGeometryField` to create Arrow fields with proper extension type and CRS metadata:

```go
// Create a geometry field with EPSG:4326 (WGS84) coordinate system
field := catalog.NewGeometryField(
    "location",  // field name
    true,        // nullable
    4326,        // SRID (EPSG code)
    "Point",     // geometry type constraint
)

// Use in schema
schema := arrow.NewSchema([]arrow.Field{
    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
    {Name: "name", Type: arrow.BinaryTypes.String},
    catalog.NewGeometryField("geom", true, 4326, "Point"),
}, nil)
```

**Supported Geometry Types:**

| Type | Description |
|------|-------------|
| `"Point"` | Single coordinate |
| `"LineString"` | Sequence of coordinates |
| `"Polygon"` | Closed ring(s) |
| `"MultiPoint"` | Collection of points |
| `"MultiLineString"` | Collection of linestrings |
| `"MultiPolygon"` | Collection of polygons |
| `"GeometryCollection"` | Mixed geometry collection |
| `"GEOMETRY"` or `""` | Any geometry type |

### GeometryBuilder

Custom builder for appending geometry data. Automatically used when `RecordBuilder` encounters a geometry field:

```go
import (
    "github.com/hugr-lab/airport-go/catalog"
    "github.com/paulmach/orb"
)

// Method 1: Direct usage
builder := catalog.NewGeometryBuilder(memory.DefaultAllocator)
defer builder.Release()

// Append geometry (encodes to WKB automatically)
builder.Append(orb.Point{-122.4194, 37.7749})
builder.Append(orb.LineString{{0, 0}, {1, 1}, {2, 2}})
builder.AppendNull()

// Append raw WKB bytes
builder.AppendWKB(wkbBytes)

// Batch append with validity mask
geoms := []orb.Geometry{orb.Point{1, 2}, orb.Point{3, 4}}
valid := []bool{true, false}
builder.AppendValues(geoms, valid)

// Build array
arr := builder.NewGeometryArray()
defer arr.Release()

// Method 2: Via RecordBuilder (automatic)
schema := arrow.NewSchema([]arrow.Field{
    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
    catalog.NewGeometryField("geom", true, 4326, "Point"),
}, nil)

recBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
defer recBuilder.Release()

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
// GeometryBuilder is automatically used for geometry columns
geomBuilder := recBuilder.Field(1).(*catalog.GeometryBuilder)

idBuilder.Append(1)
geomBuilder.Append(orb.Point{-122.4194, 37.7749})

record := recBuilder.NewRecordBatch()
```

### GeometryArray

Extension array for reading geometry data:

```go
// From RecordBatch column
geomCol := record.Column(1).(*catalog.GeometryArray)

// Get raw WKB bytes
wkbBytes := geomCol.ValueBytes(0)

// Get decoded geometry (returns orb.Geometry)
geom, err := geomCol.Value(0)
if err != nil {
    // Handle decode error
}

// Type assertion to specific geometry type
switch g := geom.(type) {
case orb.Point:
    fmt.Printf("Point: %f, %f\n", g.Lon(), g.Lat())
case orb.LineString:
    fmt.Printf("LineString with %d points\n", len(g))
case orb.Polygon:
    fmt.Printf("Polygon with %d rings\n", len(g))
}

// Check for null
if geomCol.IsNull(i) {
    // Value is null
}
```

### Encoding/Decoding Utilities

Low-level functions for WKB conversion:

```go
// Encode geometry to WKB
point := orb.Point{-122.4194, 37.7749}
wkbBytes, err := catalog.EncodeGeometry(point)

// Decode WKB to geometry
geom, err := catalog.DecodeGeometry(wkbBytes)

// Validate geometry before encoding
err := catalog.ValidateGeometry(polygon)
if err != nil {
    // e.g., "polygon outer ring is not closed"
}

// Get geometry type name
typeName := catalog.GeometryTypeName(geom) // "Point", "Polygon", etc.
```

### DuckDB Client Setup

**Important:** To query geometry data from DuckDB, the spatial extension must be installed and GeoArrow extensions registered:

```sql
-- Install and load Airport
INSTALL airport FROM community;
LOAD airport;

-- REQUIRED for geometry support
INSTALL spatial;
LOAD spatial;
FROM register_geoarrow_extensions();

-- Connect to server
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');

-- Query geometry columns
SELECT * FROM demo.geo.locations;

-- Spatial functions work with geometry columns
SELECT name, ST_AsText(geom) as wkt FROM demo.geo.locations;
SELECT name FROM demo.geo.locations WHERE ST_X(geom) < 0;
SELECT ST_Distance(a.geom, b.geom) FROM demo.geo.locations a, demo.geo.locations b;
```

The `register_geoarrow_extensions()` function registers a type handler that converts the `geoarrow.wkb` extension type to DuckDB's native GEOMETRY type.

### Complete Example

```go
package main

import (
    "context"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/paulmach/orb"
    "github.com/hugr-lab/airport-go/catalog"
)

type LocationsTable struct {
    schema *arrow.Schema
    data   []Location
}

type Location struct {
    ID    int64
    Name  string
    Point orb.Point
}

func NewLocationsTable() *LocationsTable {
    return &LocationsTable{
        schema: arrow.NewSchema([]arrow.Field{
            {Name: "id", Type: arrow.PrimitiveTypes.Int64},
            {Name: "name", Type: arrow.BinaryTypes.String},
            catalog.NewGeometryField("geom", true, 4326, "Point"),
        }, nil),
        data: []Location{
            {1, "San Francisco", orb.Point{-122.4194, 37.7749}},
            {2, "New York", orb.Point{-73.9857, 40.7484}},
        },
    }
}

func (t *LocationsTable) Name() string { return "locations" }
func (t *LocationsTable) Comment() string { return "Locations with geometry" }
func (t *LocationsTable) ArrowSchema(columns []string) *arrow.Schema {
    return catalog.ProjectSchema(t.schema, columns)
}

func (t *LocationsTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
    defer builder.Release()

    idBuilder := builder.Field(0).(*array.Int64Builder)
    nameBuilder := builder.Field(1).(*array.StringBuilder)
    geomBuilder := builder.Field(2).(*catalog.GeometryBuilder)

    for _, loc := range t.data {
        idBuilder.Append(loc.ID)
        nameBuilder.Append(loc.Name)
        geomBuilder.Append(loc.Point) // Auto-encodes to WKB
    }

    record := builder.NewRecordBatch()
    return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}
```

See `examples/geometry/` for a complete runnable example.
