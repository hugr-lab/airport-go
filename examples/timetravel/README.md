# Time Travel Example

This example demonstrates time travel support in an Airport Flight server, allowing queries to retrieve data at specific points in time using DuckDB's `AT` syntax.

## Features

- **Version-Based Queries**: Query data at specific version numbers
- **Historical Data Access**: Access previous states of table data
- **Column Projection**: Works with column selection (projection pushdown)
- **DynamicSchemaTable**: Schema can vary based on time point

## Prerequisites

- Go 1.25+
- DuckDB 1.4+ (for client testing)
- Airport extension for DuckDB

## Running the Server

Start the time travel-enabled Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` and output:
```
Airport Time Travel server listening on :50051
Example catalog contains:
  - Schema: test
    - Table: users (supports time travel with 3 versions)

Available versions:
  - VERSION 1: Only Alice
  - VERSION 2: Alice + Bob
  - VERSION 3: Alice + Bob + Charlie (current)
```

## Testing with DuckDB

Start DuckDB and connect to the server:

```bash
duckdb
```

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the time travel server
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
```

### Query Current Data

```sql
-- Get current (latest) data
SELECT * FROM demo.test.users;
```

Expected output:
```
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ Alice   │
│     2 │ Bob     │
│     3 │ Charlie │
└───────┴─────────┘
```

### Query Historical Versions

```sql
-- Version 1: Only Alice existed
SELECT * FROM demo.test.users AT (VERSION => 1);
```

Output:
```
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ Alice   │
└───────┴─────────┘
```

```sql
-- Version 2: Alice and Bob
SELECT * FROM demo.test.users AT (VERSION => 2);
```

Output:
```
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ Alice   │
│     2 │ Bob     │
└───────┴─────────┘
```

### Column Projection with Time Travel

```sql
-- Select specific columns at a version
SELECT name FROM demo.test.users AT (VERSION => 2);
```

Output:
```
┌─────────┐
│  name   │
│ varchar │
├─────────┤
│ Alice   │
│ Bob     │
└─────────┘
```

## Implementation Details

This example implements the `DynamicSchemaTable` interface:

### DynamicSchemaTable Interface

```go
type DynamicSchemaTable interface {
    Table

    // SchemaForRequest returns the schema for a specific request.
    // Request contains parameters and/or time point.
    SchemaForRequest(ctx context.Context, req *SchemaRequest) (*arrow.Schema, error)
}
```

### SchemaRequest Structure

```go
type SchemaRequest struct {
    // TimePoint specifies the point in time for time-travel queries
    TimePoint *TimePoint

    // Columns specifies which columns to include (projection)
    // The server should return a full schema, the projection is handled by duckdb.
    // The server may use this information for optimization.
    // The unselected columns can contain nulls or default values in the returned arrays.
    Columns []string
}

type TimePoint struct {
    Unit  string  // "version", "timestamp", etc.
    Value string  // The version number or timestamp value
}
```

### Implementing Time Travel

```go
func (t *TimeTravelTable) SchemaForRequest(ctx context.Context, req *catalog.SchemaRequest) (*arrow.Schema, error) {
    // Handle time point if provided
    if req.TimePoint != nil {
        version, err := strconv.ParseInt(req.TimePoint.Value, 10, 64)
        if err != nil {
            return nil, fmt.Errorf("invalid version: %w", err)
        }
        // Return schema appropriate for this version
        return t.schemaForVersion(version), nil
    }

    // Return current schema
    return t.schema, nil
}

func (t *TimeTravelTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Check for time travel in scan options
    if opts.TimePoint != nil {
        version, _ := strconv.ParseInt(opts.TimePoint.Value, 10, 64)
        return t.scanAtVersion(version)
    }

    // Return current data
    return t.scanCurrent()
}
```

## Use Cases

Time travel is useful for:

- **Audit Trails**: View data as it existed at a specific point
- **Data Recovery**: Access data before accidental modifications
- **Historical Analysis**: Compare data across different time periods
- **Debugging**: Understand how data changed over time

## Next Steps

- Try the [dml example](../dml/) for data modification operations
- Try the [ddl example](../ddl/) for schema management
- Read the [main README](../../README.md) for more advanced usage
