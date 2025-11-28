# Quickstart: Airport Go Flight Server

**Date**: 2025-11-25
**Target**: Build and run your first Airport Flight server in under 30 lines of Go code
**Go Version**: 1.25+
**Package**: `github.com/hugr-lab/airport-go`

## Overview

This quickstart demonstrates creating a basic Arrow Flight server compatible with DuckDB Airport Extension. The server exposes a single `users` table with in-memory data.

## Prerequisites

```bash
# Go 1.25 or later
go version

# Install airport-go package
go get github.com/hugr-lab/airport-go@latest
go get github.com/apache/arrow-go/v18/arrow@latest
```

## Complete Example (Under 30 Lines)

```go
package main

import (
    "context"
    "log"
    "net"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "google.golang.org/grpc"

    "github.com/hugr-lab/airport-go"
)

func main() {
    // Define schema and scan function
    userSchema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
    }, nil)

    scanUsers := func(ctx context.Context, opts *airport.ScanOptions) (array.RecordReader, error) {
        // Create in-memory data
        builder := array.NewRecordBuilder(memory.DefaultAllocator, userSchema)
        defer builder.Release()

        builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
        builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

        record := builder.NewRecord()
        defer record.Release()

        return array.NewRecordReader(userSchema, []arrow.Record{record})
    }

    // Build catalog with builder API
    cat, _ := airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(airport.SimpleTableDef{
                Name:     "users",
                Comment:  "User accounts",
                Schema:   userSchema,
                ScanFunc: scanUsers,
            }).
        Build()

    // Create and start server
    grpcServer := grpc.NewServer()
    airport.NewServer(grpcServer, airport.ServerConfig{Catalog: cat})

    lis, _ := net.Listen("tcp", ":50051")
    log.Println("Airport server listening on :50051")
    grpcServer.Serve(lis)
}
```

**Line count**: ~25 lines (excluding imports and blank lines)

## Test with DuckDB

Install DuckDB with Airport extension:

```bash
# Install DuckDB
brew install duckdb  # macOS
# or download from https://duckdb.org

# Start DuckDB
duckdb
```

Query the server:

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to your Flight server
ATTACH 'grpc://localhost:50051' AS my_server (TYPE AIRPORT);

-- Query the users table
SELECT * FROM my_server.main.users;
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

## Adding Authentication

Add bearer token authentication in 3 lines:

```go
airport.NewServer(grpcServer, airport.ServerConfig{
    Catalog: cat,
    Auth: airport.BearerAuth(func(token string) (string, error) {
        if token == "secret-api-key" {
            return "user1", nil
        }
        return "", airport.ErrUnauthorized
    }),
})
```

Test with authenticated connection:

```sql
-- DuckDB connection with auth
ATTACH 'grpc://localhost:50051' AS my_server (
    TYPE AIRPORT,
    bearer_token 'secret-api-key'
);
```

## Adding More Tables

Add multiple tables by chaining:

```go
cat, _ := airport.NewCatalogBuilder().
    Schema("main").
        SimpleTable(airport.SimpleTableDef{
            Name:     "users",
            Schema:   userSchema,
            ScanFunc: scanUsers,
        }).
        SimpleTable(airport.SimpleTableDef{
            Name:     "orders",
            Schema:   orderSchema,
            ScanFunc: scanOrders,
        }).
    Build()
```

## Adding Scalar Functions

Add custom scalar function (e.g., UPPERCASE):

```go
type UppercaseFunc struct{}

func (f *UppercaseFunc) Name() string { return "UPPERCASE" }
func (f *UppercaseFunc) Comment() string { return "Convert to uppercase" }

func (f *UppercaseFunc) Signature() airport.FunctionSignature {
    return airport.FunctionSignature{
        Parameters: []arrow.DataType{arrow.BinaryTypes.String},
        ReturnType: arrow.BinaryTypes.String,
    }
}

func (f *UppercaseFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
    // Vectorized execution on batch
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

// Register in catalog
cat, _ := airport.NewCatalogBuilder().
    Schema("main").
        SimpleTable(...).
        ScalarFunc(&UppercaseFunc{}).
    Build()
```

Test in DuckDB:

```sql
SELECT UPPERCASE(name) FROM my_server.main.users;
```

## Connecting to Real Database

Replace in-memory scan with database query:

```go
import "database/sql"

func scanUsers(ctx context.Context, opts *airport.ScanOptions) (array.RecordReader, error) {
    // Query your database
    rows, err := db.QueryContext(ctx, "SELECT id, name FROM users")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    // Convert rows to Arrow RecordReader
    return convertSQLToArrowReader(rows, userSchema)
}
```

## Project Structure

```
myproject/
├── go.mod
├── main.go          # Server entrypoint (shown above)
├── catalog.go       # Catalog builder logic
├── scanners.go      # Scan function implementations
└── functions.go     # Scalar function implementations
```

## Common Patterns

### Pattern 1: Static Catalog (shown above)
- Use builder API
- Perfect for fixed schemas
- Minimal code

### Pattern 2: Dynamic Catalog
- Implement `Catalog` interface
- Reflect live database state
- See `examples/dynamic/` in repository

### Pattern 3: Time-Travel Queries
- Implement `DynamicSchemaTable` interface
- Support `SchemaForRequest()` with TimePoint
- See `examples/timeseries/` in repository

## Next Steps

1. **Read the full documentation**: See `README.md` and `docs/` folder
2. **Explore examples**: Check `examples/` for more patterns
3. **Run integration tests**: See `tests/integration/` for DuckDB test setup
4. **Deploy with TLS**: Configure `grpc.Creds()` for production
5. **Add observability**: Integrate `slog.SetDefault()` with your logging stack

## Troubleshooting

### Port already in use
```
Error: bind: address already in use
```
Solution: Change port in `net.Listen("tcp", ":50051")` or stop other process

### DuckDB can't connect
```
Error: Connection failed
```
Solution:
1. Check server is running: `lsof -i :50051`
2. Check firewall settings
3. Try `grpc://127.0.0.1:50051` instead of `localhost`

### Authentication errors
```
Error: Unauthenticated
```
Solution:
1. Check bearer token matches: `bearer_token 'secret-api-key'`
2. Verify auth function returns nil error for valid tokens

### Memory leaks
```
Error: Memory usage growing
```
Solution:
1. Always call `defer builder.Release()` and `defer record.Release()`
2. Use `memory.NewCheckedAllocator()` in tests to detect leaks
3. Run with race detector: `go run -race main.go`

## Performance Tips

1. **Batch size**: Return 10,000-100,000 rows per batch (not single rows)
2. **Connection pooling**: Reuse database connections in scan functions
3. **Streaming**: Don't load entire result set into memory
4. **Context cancellation**: Check `ctx.Done()` in loops for early termination
5. **gRPC message size**: Set `grpc.MaxRecvMsgSize(16 * 1024 * 1024)` for large batches

## References

- **Airport Go Package**: https://github.com/hugr-lab/airport-go
- **DuckDB Airport Extension**: https://airport.query.farm
- **Apache Arrow Go**: https://github.com/apache/arrow/go
- **Arrow Flight Protocol**: https://arrow.apache.org/docs/format/Flight.html
