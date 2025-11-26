# Airport Go - Apache Arrow Flight Server for DuckDB

[![Go Reference](https://pkg.go.dev/badge/github.com/hugr-lab/airport-go.svg)](https://pkg.go.dev/github.com/hugr-lab/airport-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/hugr-lab/airport-go)](https://goreportcard.com/report/github.com/hugr-lab/airport-go)

A high-level Go package for building Apache Arrow Flight servers compatible with the [DuckDB Airport Extension](https://airport.query.farm).

## Features

- **Simple API**: Build a Flight server in under 30 lines of code
- **Fluent Catalog Builder**: Define schemas, tables, and functions with method chaining
- **Dynamic Catalogs**: Implement custom catalog logic for live schema reflection
- **Bearer Token Auth**: Built-in authentication support
- **Streaming Efficiency**: No rebatching - preserves Arrow batch sizes from your data sources
- **Context Cancellation**: Respects client disconnections and timeouts
- **gRPC Integration**: Registers on your existing `grpc.Server` - you control lifecycle and TLS

## Installation

```bash
go get github.com/hugr-lab/airport-go@latest
```

**Requirements**: Go 1.25+

## Quick Start

Build and run a basic Flight server:

```go
package main

import (
    "context"
    "log"
    "net"

    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/array"
    "github.com/apache/arrow/go/v18/arrow/memory"
    "google.golang.org/grpc"

    "github.com/hugr-lab/airport-go"
    "github.com/hugr-lab/airport-go/catalog"
)

func main() {
    // Define schema and scan function
    userSchema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
    }, nil)

    scanUsers := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
        builder := array.NewRecordBuilder(memory.DefaultAllocator, userSchema)
        defer builder.Release()

        builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
        builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

        record := builder.NewRecord()
        defer record.Release()

        return array.NewRecordReader(userSchema, []arrow.Record{record})
    }

    // Build catalog
    cat, _ := airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(airport.SimpleTableDef{
                Name:     "users",
                Comment:  "User accounts",
                Schema:   userSchema,
                ScanFunc: scanUsers,
            }).
        Build()

    // Start server
    grpcServer := grpc.NewServer()
    airport.NewServer(grpcServer, airport.ServerConfig{Catalog: cat})

    lis, _ := net.Listen("tcp", ":50051")
    log.Println("Airport server listening on :50051")
    grpcServer.Serve(lis)
}
```

## Test with DuckDB

Install DuckDB and the Airport extension:

```bash
# Install DuckDB
brew install duckdb  # macOS
# or download from https://duckdb.org

# Start DuckDB
duckdb
```

Query your Flight server:

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

## Authentication

Add bearer token authentication:

```go
auth := airport.BearerAuth(func(token string) (string, error) {
    if token == "secret-api-key" {
        return "user1", nil
    }
    return "", airport.ErrUnauthorized
})

airport.NewServer(grpcServer, airport.ServerConfig{
    Catalog: cat,
    Auth:    auth,
})
```

Test with DuckDB:

```sql
ATTACH 'grpc://localhost:50051' AS my_server (
    TYPE AIRPORT,
    bearer_token 'secret-api-key'
);
```

## Architecture

The package follows an interface-based design:

- **Catalog**: Top-level interface for querying schemas
- **Schema**: Interface for querying tables and functions
- **Table**: Interface providing Arrow schema and scan function
- **ScalarFunction**: Interface for custom scalar functions

You can either:
- Use `NewCatalogBuilder()` for static catalogs (quickest)
- Implement the `Catalog` interface for dynamic catalogs

## Documentation

- [GoDoc](https://pkg.go.dev/github.com/hugr-lab/airport-go) - Full API reference
- [Examples](examples/) - Common usage patterns
- [Specification](specs/001-001-flight-server/) - Complete design documents

## Performance Tips

1. **Batch Size**: Return 10,000-100,000 rows per batch (not single rows)
2. **Connection Pooling**: Reuse database connections in scan functions
3. **Streaming**: Don't load entire result sets into memory
4. **Context Cancellation**: Check `ctx.Done()` in loops for early termination
5. **gRPC Message Size**: Set `grpc.MaxRecvMsgSize(16 * 1024 * 1024)` for large batches

## Contributing

Contributions are welcome! Please:

1. Follow idiomatic Go style (`gofmt`, `golangci-lint`)
2. Include tests for new functionality
3. Update documentation for API changes

## License

[License TBD - To be added]

## References

- [DuckDB Airport Extension](https://airport.query.farm)
- [Apache Arrow Go](https://github.com/apache/arrow/go)
- [Arrow Flight Protocol](https://arrow.apache.org/docs/format/Flight.html)
