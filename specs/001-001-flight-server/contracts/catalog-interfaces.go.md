# Contract: Catalog Interfaces

**Package**: `github.com/your-org/airport-go/catalog`
**Purpose**: Core catalog abstraction interfaces supporting DuckDB Airport Extension protocol
**Go Version**: 1.23+
**Arrow Version**: `github.com/apache/arrow-go/v18/arrow`

## Catalog Interface

```go
package catalog

import (
    "context"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
)

// Catalog represents the top-level metadata container.
// Implementations can be static (from builder) or dynamic (user-provided).
// All methods MUST be goroutine-safe.
type Catalog interface {
    // Schemas returns all schemas visible in this catalog.
    // Context may contain auth info for permission-based filtering.
    // Returns empty slice (not nil) if no schemas available.
    // MUST respect context cancellation and deadlines.
    Schemas(ctx context.Context) ([]Schema, error)

    // Schema returns a specific schema by name.
    // Returns (nil, nil) if schema doesn't exist (not an error).
    // Returns (nil, err) if lookup fails for other reasons.
    Schema(ctx context.Context, name string) (Schema, error)
}
```

## Schema Interface

```go
// Schema represents a database schema containing tables and functions.
// Implementations MUST be goroutine-safe.
type Schema interface {
    // Name returns the schema name (e.g., "main", "information_schema").
    // MUST return non-empty string.
    Name() string

    // Comment returns optional schema documentation.
    // Returns empty string if no comment provided.
    Comment() string

    // Tables returns all tables in this schema.
    // Context may contain auth info for permission filtering.
    // Returns empty slice (not nil) if no tables available.
    // MUST respect context cancellation.
    Tables(ctx context.Context) ([]Table, error)

    // Table returns a specific table by name.
    // Returns (nil, nil) if table doesn't exist (not an error).
    // Returns (nil, err) if lookup fails for other reasons.
    Table(ctx context.Context, name string) (Table, error)

    // ScalarFunctions returns all scalar functions in this schema.
    // Returns empty slice (not nil) if no functions available.
    // MUST respect context cancellation.
    ScalarFunctions(ctx context.Context) ([]ScalarFunction, error)

    // TableFunctions returns all table-valued functions in this schema.
    // Returns empty slice (not nil) if no table functions available.
    // MUST respect context cancellation.
    TableFunctions(ctx context.Context) ([]TableFunction, error)
}
```

## Table Interface

```go
// Table represents a queryable table or view with fixed schema.
// For tables with dynamic schemas (based on parameters/time), see DynamicSchemaTable.
// Implementations MUST be goroutine-safe.
type Table interface {
    // Name returns the table name (e.g., "users", "orders").
    // MUST return non-empty string.
    Name() string

    // Comment returns optional table documentation.
    // Returns empty string if no comment provided.
    Comment() string

    // ArrowSchema returns the logical schema describing table columns.
    // Returns nil if schema is dynamic (see DynamicSchemaTable interface).
    // If non-nil, MUST return valid *arrow.Schema.
    ArrowSchema() *arrow.Schema

    // Scan executes a scan operation and returns a RecordReader.
    // Context allows cancellation; implementation MUST respect ctx.Done().
    // Caller MUST call reader.Release() to free memory.
    // Returned RecordReader schema MUST match ArrowSchema() (if non-nil).
    Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}

// DynamicSchemaTable extends Table for tables with parameter/time-dependent schemas.
// Used for table functions and time-travel queries.
// Implements DuckDB Airport Extension actions:
// - table_function_flight_info (schema depends on parameters)
// - endpoints (schema depends on time point)
type DynamicSchemaTable interface {
    Table

    // SchemaForRequest returns the schema for a specific request.
    // Request contains parameters and/or time point.
    // Used by GetFlightInfo to provide schema without executing full scan.
    // Returns error if parameters are invalid or schema cannot be determined.
    SchemaForRequest(ctx context.Context, req *SchemaRequest) (*arrow.Schema, error)
}
```

## TableFunction Interface

```go
// TableFunction represents a table-valued function (returns table, not scalar).
// Example: read_parquet('file.parquet'), read_json_auto('data.json')
// Supports DuckDB Airport Extension table_function_flight_info action.
type TableFunction interface {
    // Name returns the function name (e.g., "READ_PARQUET").
    // By convention, use UPPERCASE for function names.
    // MUST return non-empty string.
    Name() string

    // Comment returns optional function documentation.
    // Returns empty string if no comment provided.
    Comment() string

    // Signature returns the function signature.
    // Defines parameter types (parameters determine output schema).
    Signature() FunctionSignature

    // SchemaForParameters returns the output schema for given parameters.
    // Used by GetFlightInfo before executing function.
    // Parameters are MessagePack-decoded values matching Signature.
    // Returns error if parameters are invalid.
    SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error)

    // Execute runs the table function and returns a RecordReader.
    // Parameters match Signature types.
    // Returned RecordReader schema MUST match SchemaForParameters result.
    // Caller MUST call reader.Release().
    Execute(ctx context.Context, params []any, opts *ScanOptions) (array.RecordReader, error)
}
```

## ScalarFunction Interface

```go
// ScalarFunction represents a user-defined scalar function.
// Callable from DuckDB queries via Airport extension.
// Implementations MUST be goroutine-safe.
type ScalarFunction interface {
    // Name returns the function name (e.g., "UPPERCASE").
    // By convention, use UPPERCASE for function names.
    // MUST return non-empty string.
    Name() string

    // Comment returns optional function documentation.
    // Returns empty string if no comment provided.
    Comment() string

    // Signature returns the function signature.
    // Defines parameter types and return type.
    // MUST return valid signature with at least 1 parameter.
    Signature() FunctionSignature

    // Execute runs the function on input record and returns result record.
    // Input record columns MUST match parameter types from Signature.
    // Returned record MUST have single column matching return type.
    // Caller MUST call result.Release() to free memory.
    // MUST respect context cancellation.
    // Processes entire batch at once (vectorized execution).
    Execute(ctx context.Context, input arrow.Record) (arrow.Record, error)
}
```

## Supporting Types

```go
// ScanOptions provides options for table scans.
type ScanOptions struct {
    // Columns to return. If nil/empty, return all columns.
    Columns []string

    // Filter predicate (serialized Arrow expression).
    // If nil, no filtering (return all rows).
    Filter []byte

    // Limit is maximum rows to return.
    // If 0 or negative, no limit.
    Limit int64

    // BatchSize is hint for RecordReader batch size.
    // If 0, implementation chooses default.
    // Implementations MAY ignore this hint.
    BatchSize int

    // TimePoint specifies point-in-time for time-travel queries.
    // Nil for "current" time (no time travel).
    // Supports DuckDB Airport Extension "endpoints" action.
    TimePoint *TimePoint
}

// TimePoint represents a point-in-time for time-travel queries.
// Supports DuckDB Airport Extension temporal query protocol.
type TimePoint struct {
    // Unit specifies time granularity ("timestamp", "version", "snapshot").
    Unit string

    // Value is the time point value (format depends on Unit).
    // Examples:
    // - Unit="timestamp", Value="2024-01-15T10:30:00Z"
    // - Unit="version", Value="42"
    // - Unit="snapshot", Value="abc123def"
    Value string
}

// SchemaRequest contains parameters for determining dynamic schema.
// Used by DynamicSchemaTable.SchemaForRequest().
type SchemaRequest struct {
    // Parameters for table functions (MessagePack-decoded values).
    // Nil for regular tables without parameters.
    Parameters []any

    // TimePoint for time-travel queries.
    // Nil for "current" time.
    TimePoint *TimePoint

    // Columns requested (for projection pushdown).
    // Nil/empty means all columns.
    Columns []string
}

// FunctionSignature describes scalar/table function types.
type FunctionSignature struct {
    // Parameters is list of parameter types (in order).
    // MUST have at least 1 parameter.
    Parameters []arrow.DataType

    // ReturnType is the function's return type (for scalar functions).
    // Nil for table functions (schema determined by SchemaForParameters).
    ReturnType arrow.DataType

    // Variadic indicates if last parameter accepts multiple values.
    Variadic bool
}

// ScanFunc is a function type for table data retrieval.
// User implements this to connect to their data source.
type ScanFunc func(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
```

## DuckDB Airport Extension Actions

The following Airport-specific actions MUST be supported:

### 1. `table_function_flight_info`

**Purpose**: Get schema for table function without executing it.

**Request**: MessagePack-encoded
```go
type TableFunctionFlightInfoRequest struct {
    FunctionName string `msgpack:"name"`
    Parameters   []any  `msgpack:"parameters"`
}
```

**Response**: `FlightInfo` with schema from `TableFunction.SchemaForParameters()`.

**Implementation**: Handler looks up `TableFunction` by name, calls `SchemaForParameters()`, returns `FlightInfo`.

### 2. `endpoints`

**Purpose**: Get flight endpoints for time-travel query.

**Request**: MessagePack-encoded
```go
type EndpointsRequest struct {
    Schema    string     `msgpack:"schema"`
    Table     string     `msgpack:"table"`
    TimePoint *TimePoint `msgpack:"time_point,omitempty"`
}
```

**Response**: List of `FlightEndpoint` with locations and schema for that time point.

**Implementation**: Handler looks up table, checks if it implements `DynamicSchemaTable`, calls `SchemaForRequest()` with time point.

### 3. Standard Flight SQL Actions

- `GetCatalogs`: List all catalogs
- `GetDbSchemas`: List schemas (with optional filter patterns)
- `GetTables`: List tables (with optional filters, include_schema flag)
- `GetTableTypes`: Return supported table types (TABLE, VIEW, etc.)

All catalog actions return Arrow IPC data compressed with ZStandard.

## Example Implementations

### Static Table (Fixed Schema)

```go
type staticTable struct {
    name     string
    comment  string
    schema   *arrow.Schema
    scanFunc ScanFunc
}

func (t *staticTable) Name() string { return t.name }
func (t *staticTable) Comment() string { return t.comment }
func (t *staticTable) ArrowSchema() *arrow.Schema { return t.schema }

func (t *staticTable) Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error) {
    return t.scanFunc(ctx, opts)
}
```

### Dynamic Schema Table (Time Travel)

```go
type timeSeriesTable struct {
    staticTable // Embed for Name(), Comment()
    backend TimeSeriesBackend
}

func (t *timeSeriesTable) ArrowSchema() *arrow.Schema {
    return nil // Schema depends on time point
}

func (t *timeSeriesTable) SchemaForRequest(ctx context.Context, req *SchemaRequest) (*arrow.Schema, error) {
    if req.TimePoint == nil {
        // Current schema
        return t.backend.CurrentSchema(ctx)
    }

    // Historical schema at time point
    return t.backend.SchemaAtTime(ctx, req.TimePoint)
}

func (t *timeSeriesTable) Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error) {
    return t.backend.ScanAtTime(ctx, opts.TimePoint, opts)
}
```

### Table Function (Parameterized Schema)

```go
type readParquetFunc struct {
    name string
}

func (f *readParquetFunc) Name() string { return "READ_PARQUET" }
func (f *readParquetFunc) Comment() string { return "Read Parquet file" }

func (f *readParquetFunc) Signature() FunctionSignature {
    return FunctionSignature{
        Parameters: []arrow.DataType{arrow.BinaryTypes.String}, // file path
        ReturnType: nil, // Table function, no scalar return
    }
}

func (f *readParquetFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
    filePath := params[0].(string)

    // Open Parquet file and read schema (without reading all data)
    reader, err := parquet.OpenFile(filePath)
    if err != nil {
        return nil, err
    }
    defer reader.Close()

    return reader.Schema(), nil
}

func (f *readParquetFunc) Execute(ctx context.Context, params []any, opts *ScanOptions) (array.RecordReader, error) {
    filePath := params[0].(string)

    // Open and stream Parquet data
    reader, err := parquet.OpenFile(filePath)
    if err != nil {
        return nil, err
    }

    return reader.RecordReader(opts), nil
}
```

### Scalar Function (Vectorized Execution)

```go
type uppercaseFunc struct{}

func (f *uppercaseFunc) Name() string { return "UPPERCASE" }
func (f *uppercaseFunc) Comment() string { return "Convert string to uppercase" }

func (f *uppercaseFunc) Signature() FunctionSignature {
    return FunctionSignature{
        Parameters: []arrow.DataType{arrow.BinaryTypes.String},
        ReturnType: arrow.BinaryTypes.String,
    }
}

func (f *uppercaseFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
    // Input has 1 column (string array)
    inputCol := input.Column(0).(*array.String)

    // Build output array
    builder := array.NewStringBuilder(memory.DefaultAllocator)
    defer builder.Release()

    for i := 0; i < inputCol.Len(); i++ {
        if inputCol.IsNull(i) {
            builder.AppendNull()
        } else {
            builder.Append(strings.ToUpper(inputCol.Value(i)))
        }
    }

    outputCol := builder.NewArray()
    defer outputCol.Release()

    // Create output record with single column
    outputSchema := arrow.NewSchema([]arrow.Field{
        {Name: "result", Type: arrow.BinaryTypes.String},
    }, nil)

    return array.NewRecord(outputSchema, []arrow.Array{outputCol}, int64(inputCol.Len())), nil
}
```

## Contract Validation

Implementations MUST:
1. Be goroutine-safe (safe for concurrent access)
2. Respect context cancellation (check `ctx.Done()` in loops)
3. Return `(nil, nil)` for "not found" cases (not an error)
4. Return empty slices (not nil) for "no items" cases
5. Return valid Arrow types (never nil schemas for static tables)
6. Return nil from `ArrowSchema()` if schema is dynamic
7. Implement `DynamicSchemaTable` if `ArrowSchema()` returns nil
8. Support Airport Extension actions (table_function_flight_info, endpoints)
9. Process entire batches in scalar functions (vectorized, not row-by-row)
10. Document Release() requirements for caller

## Notes on Design Changes

**Why `arrow.Record` for ScalarFunction.Execute()?**
- Vectorized execution: Process entire batch at once, not row-by-row
- More efficient: Avoids per-row function call overhead
- Arrow-native: Matches Arrow compute function pattern
- Batch-aware: Can use SIMD/vectorized operations

**Why `DynamicSchemaTable` interface?**
- Separation of concerns: Most tables have fixed schemas
- Type safety: Compiler enforces `SchemaForRequest()` if `ArrowSchema()` returns nil
- Backward compatible: Existing static tables don't need changes
- Airport protocol: Directly maps to `table_function_flight_info` and `endpoints` actions

**Why `TableFunction` separate from `ScalarFunction`?**
- Different semantics: Table functions return tables, scalars return values
- Different signatures: Table functions' output schema depends on input parameters
- Airport protocol: `table_function_flight_info` action specifically for table functions
- Clearer API: Users know immediately what function type they're implementing
