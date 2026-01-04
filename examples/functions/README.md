# Functions Example

This example demonstrates how to implement scalar and table functions in Airport Go.

## Features Demonstrated

### Scalar Functions
- **MULTIPLY(value INT64, factor INT64) -> INT64**: Multiplies an integer by a constant factor
- Vectorized batch processing for performance
- Type validation and error handling

### Table Functions with Fixed Schema
- **GENERATE_SERIES(start, stop, [step])**: Generates a series of integers
- Similar to PostgreSQL's `generate_series` function
- Optional step parameter (defaults to 1)
- Returns single column: `value INT64`

### Table Functions with Dynamic Schema
- **GENERATE_RANGE(start, stop, column_count)**: Generates table with dynamic columns
- Schema determined by `column_count` parameter
- Returns 1-10 columns named `col1`, `col2`, etc.
- Demonstrates parameter-dependent schema generation

## Running the Example

### 1. Start the Server

```bash
cd examples/functions
go run main.go
```

You should see:
```
Airport Flight server with functions listening on [::]:50051
Connect with: ATTACH '' AS my_server (TYPE airport, LOCATION 'grpc://localhost:50051')

Available functions:
  - Scalar: MULTIPLY(x INT64, factor INT64) -> INT64
  - Table:  GENERATE_SERIES(start INT64, stop INT64, [step INT64]) -> (value INT64)
  - Table:  GENERATE_RANGE(start, stop, columns) -> dynamic schema
```

### 2. Connect from DuckDB

Open DuckDB and run the example queries:

```bash
duckdb < client.sql
```

Or interactively:

```sql
INSTALL airport FROM community;
LOAD airport;
ATTACH '' AS funcs (TYPE airport, LOCATION 'grpc://localhost:50051');

-- Generate series 1-5
SELECT * FROM funcs.functions_demo.GENERATE_SERIES(1, 5, 1);

-- Generate table with 3 columns
SELECT * FROM funcs.functions_demo.GENERATE_RANGE(1, 5, 3);
```

## Implementation Details

### Scalar Function Interface

```go
type ScalarFunction interface {
    Name() string
    Comment() string
    Signature() FunctionSignature
    Execute(ctx context.Context, input arrow.Record) (arrow.Record, error)
}
```

**Key Points:**
- Processes entire Arrow batches (vectorized)
- Input record contains parameter columns
- Output record must have single column matching return type
- Must handle NULL values appropriately

### Table Function Interface

```go
type TableFunction interface {
    Name() string
    Comment() string
    Signature() FunctionSignature
    SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error)
    Execute(ctx context.Context, params []any, opts *ScanOptions) (array.RecordReader, error)
}
```

**Key Points:**
- `SchemaForParameters()` called first to determine output schema
- Schema can vary based on parameter values (dynamic schemas)
- `Execute()` must return RecordReader matching declared schema
- Parameters are JSON-decoded values (numbers as float64, strings as string)

## Dynamic Schema Example

The `GENERATE_RANGE` function shows how to create dynamic schemas:

```go
func (f *generateRangeFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
    columnCount := int(params[2].(float64))

    // Build schema with N columns
    fields := make([]arrow.Field, columnCount)
    for i := 0; i < columnCount; i++ {
        fields[i] = arrow.Field{
            Name: fmt.Sprintf("col%d", i+1),
            Type: arrow.PrimitiveTypes.Int64,
        }
    }

    return arrow.NewSchema(fields, nil), nil
}
```

Query with 2 columns:
```sql
SELECT * FROM GENERATE_RANGE(1, 3, 2);
-- Returns: col1 | col2
--          1    | 2
--          2    | 4
--          3    | 6
```

Query with 5 columns:
```sql
SELECT * FROM GENERATE_RANGE(1, 3, 5);
-- Returns: col1 | col2 | col3 | col4 | col5
--          1    | 2    | 3    | 4    | 5
--          2    | 4    | 6    | 8    | 10
--          3    | 6    | 9    | 12   | 15
```

## Best Practices

### Parameter Validation
```go
func (f *myFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
    if len(params) != 2 {
        return nil, fmt.Errorf("function requires 2 parameters, got %d", len(params))
    }

    // Type assertions for parameters
    start, ok := params[0].(float64)
    if !ok {
        return nil, fmt.Errorf("parameter 1 must be number, got %T", params[0])
    }

    // ... validation logic
}
```

### Memory Management
```go
func (f *myFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
    builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
    defer builder.Release()  // Always release builders

    // Build records...
    record := builder.NewRecord()

    // RecordReader takes ownership - caller will release
    return array.NewRecordReader(schema, []arrow.Record{record})
}
```

### Context Cancellation
```go
func (f *myFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }

    // Process data...
}
```

## Limitations

### Current Limitations
1. **Scalar Functions**: Fully supported. Can be called like regular tables in SELECT clauses.

2. **Table Functions**: Fully supported. Can be called like regular tables in FROM clauses.

3. **Parameter Types**: Parameters are deserialized from Arrow Record Batches and come as Go types:
   - Integer types (INT8, INT16, INT32, INT64, UINT8, etc.) → `int64`
   - Float types (FLOAT32, FLOAT64) → `float64`
   - Strings → `string`
   - Binary → `[]byte`
   - Booleans → `bool`

   Functions should handle both `int64` and `float64` for numeric parameters to be flexible.

## See Also

- [Catalog Builder API](../../README.md#catalog-builder)
- [Function Interfaces](../../catalog/function.go)
- [Integration Tests](../../tests/integration/functions_test.go)
- [Arrow Go Documentation](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow)
