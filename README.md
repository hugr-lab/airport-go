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

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
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

### Batch Sizing

**Optimal batch size: 10,000-100,000 rows**

```go
// Good: Return multiple rows per batch
func scanLarge(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    records := make([]arrow.Record, 0)
    for i := 0; i < 10; i++ {  // 10 batches
        record := buildBatch(10000) // 10k rows each
        records = append(records, record)
    }
    return array.NewRecordReader(schema, records)
}

// Bad: Single row per batch
func scanSlow(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    records := make([]arrow.Record, 0)
    for i := 0; i < 100000; i++ {  // 100k batches!
        record := buildBatch(1)  // 1 row per batch - slow!
        records = append(records, record)
    }
    return array.NewRecordReader(schema, records)
}
```

### Connection Pooling

Reuse database connections across scan invocations:

```go
type MyTable struct {
    db *sql.DB  // Shared connection pool
}

func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Use existing pool, don't create new connections
    rows, err := t.db.QueryContext(ctx, "SELECT * FROM data")
    if err != nil {
        return nil, err
    }
    // Convert rows to Arrow batches...
}
```

### Streaming Large Datasets

Don't load entire results into memory:

```go
// Good: Stream batches as you read
func streamScan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    rows, _ := db.QueryContext(ctx, "SELECT * FROM large_table")

    // Create streaming reader that builds batches on-demand
    return NewStreamingReader(rows, batchSize), nil
}

// Bad: Load everything first
func bufferedScan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    rows, _ := db.QueryContext(ctx, "SELECT * FROM large_table")

    // Loads all data into memory first - OOM risk!
    allRecords := make([]arrow.Record, 0)
    for rows.Next() {
        record := convertRow(rows)
        allRecords = append(allRecords, record)
    }
    return array.NewRecordReader(schema, allRecords)
}
```

### Context Cancellation

Respect client cancellations to avoid wasted work:

```go
func scanWithCancellation(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    rows, _ := db.QueryContext(ctx, "SELECT * FROM data")

    records := make([]arrow.Record, 0)
    for rows.Next() {
        // Check if client disconnected
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
        }

        record := convertRow(rows)
        records = append(records, record)
    }

    return array.NewRecordReader(schema, records)
}
```

### gRPC Message Size

Configure larger message sizes for big Arrow batches:

```go
config := airport.ServerConfig{
    Catalog: cat,
    MaxMessageSize: 16 * 1024 * 1024,  // 16MB (default is 4MB)
}

opts := airport.ServerOptions(config)
grpcServer := grpc.NewServer(opts...)
```

### Memory Management

Release Arrow objects to avoid memory leaks:

```go
func buildRecord(schema *arrow.Schema) arrow.Record {
    builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
    defer builder.Release()  // Always release builders

    // Build record...
    record := builder.NewRecord()
    // Caller must release the record
    return record
}

func scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    record1 := buildRecord(schema)
    defer record1.Release()  // Release when done

    record2 := buildRecord(schema)
    defer record2.Release()

    // RecordReader takes ownership, don't release until reader is closed
    return array.NewRecordReader(schema, []arrow.Record{record1.NewSlice(0, record1.NumRows()), record2.NewSlice(0, record2.NumRows())})
}
```

### Parallel Processing

Process multiple tables concurrently:

```go
type ParallelCatalog struct {
    tables map[string]catalog.Table
}

func (c *ParallelCatalog) Tables(ctx context.Context) ([]catalog.Table, error) {
    // Return all tables - they can be queried in parallel
    result := make([]catalog.Table, 0, len(c.tables))
    for _, table := range c.tables {
        result = append(result, table)
    }
    return result, nil
}
```

### Benchmarking

Measure your server's performance:

```bash
# Run with benchmarks
go test -bench=. -benchmem ./tests/benchmarks/

# CPU profiling
go test -cpuprofile=cpu.prof -bench=BenchmarkScan
go tool pprof cpu.prof

# Memory profiling
go test -memprofile=mem.prof -bench=BenchmarkScan
go tool pprof mem.prof
```

## Project Structure

```
airport-go/
├── catalog/             # Catalog interfaces and types
├── auth/               # Authentication (bearer token)
├── flight/             # Flight server implementation
├── internal/           # Internal packages (serialization, etc.)
├── examples/           # Example server implementations
│   ├── basic/         # Basic server example
│   ├── auth/          # Authenticated server example
│   └── dynamic/       # Dynamic catalog example
├── tests/
│   └── integration/   # Integration tests (requires DuckDB)
└── *.go               # Root-level package files and unit tests
```

## Testing

Run unit tests:
```bash
go test ./...
```

Run integration tests (requires DuckDB with Airport extension):
```bash
go test ./tests/integration/...
```

Run all tests with race detector:
```bash
go test -race ./...
```

## Contributing

Contributions are welcome! Please:

1. Follow idiomatic Go style (`gofmt`, `golangci-lint`)
2. Include tests for new functionality
3. Update documentation for API changes
4. Place integration tests in `tests/integration/`

## License

[License TBD - To be added]

## References

- [DuckDB Airport Extension](https://airport.query.farm)
- [Apache Arrow Go](https://github.com/apache/arrow/go)
- [Arrow Flight Protocol](https://arrow.apache.org/docs/format/Flight.html)
