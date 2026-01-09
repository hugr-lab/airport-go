# Airport Go - Apache Arrow Flight Server for DuckDB

[![Go Reference](https://pkg.go.dev/badge/github.com/hugr-lab/airport-go.svg)](https://pkg.go.dev/github.com/hugr-lab/airport-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/hugr-lab/airport-go)](https://goreportcard.com/report/github.com/hugr-lab/airport-go)

A high-level Go package for building Apache Arrow Flight servers compatible with the [DuckDB Airport Extension](https://airport.query.farm).

## Features

- **Simple API**: Build a Flight server in under 30 lines of code
- **Fluent Catalog Builder**: Define schemas, tables, and functions with method chaining
- **Dynamic Catalogs**: Implement custom catalog logic for live schema reflection
- **Multi-Catalog Server**: Serve multiple catalogs from a single endpoint with dynamic add/remove
- **Bearer Token Auth**: Built-in authentication with per-catalog authorization support
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
        Schema("test").
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
ATTACH '' AS my_server (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

-- Query the users table
SELECT * FROM my_server.test.users;
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

Test with DuckDB using a persistent secret:

```sql
-- Create a persistent secret for authentication
CREATE PERSISTENT SECRET my_auth (
    TYPE airport,
    auth_token 'secret-api-key',
    scope 'grpc://localhost:50051'
);

-- Attach using the secret (automatically applies to matching scope)
ATTACH 'my_server' AS my_server (
    TYPE AIRPORT,
    location 'grpc://localhost:50051'
);
```

Alternatively, for inline authentication headers:

```sql
SELECT * FROM airport_take_flight(
    'grpc://localhost:50051',
    'SELECT * FROM my_schema.users',
    headers := MAP{'authorization': 'secret-api-key'}
);
```

## DDL Operations (CREATE/DROP/ALTER)

Support schema and table management via SQL DDL statements:

```go
// Implement DynamicCatalog for CREATE/DROP SCHEMA
type MyCatalog struct { /* ... */ }

func (c *MyCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
    // Create new schema
    return newSchema, nil
}

func (c *MyCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
    // Drop schema (fails if contains tables)
    return nil
}

// Implement DynamicSchema for CREATE/DROP TABLE
type MySchema struct { /* ... */ }

func (s *MySchema) CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts catalog.CreateTableOptions) (catalog.Table, error) {
    // Create new table with given Arrow schema
    return newTable, nil
}

func (s *MySchema) DropTable(ctx context.Context, name string, opts catalog.DropTableOptions) error {
    // Drop table
    return nil
}

// Implement DynamicTable for ALTER TABLE operations
type MyTable struct { /* ... */ }

func (t *MyTable) AddColumn(ctx context.Context, columnSchema *arrow.Schema, opts catalog.AddColumnOptions) error {
    // Add column to table
    return nil
}

func (t *MyTable) RemoveColumn(ctx context.Context, name string, opts catalog.RemoveColumnOptions) error {
    // Remove column from table
    return nil
}

func (t *MyTable) RenameColumn(ctx context.Context, oldName, newName string, opts catalog.RenameColumnOptions) error {
    // Rename column
    return nil
}
```

Test with DuckDB:

```sql
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');

-- Schema operations
CREATE SCHEMA demo.analytics;
DROP SCHEMA demo.analytics;

-- Table operations
CREATE TABLE demo.test.users (id INTEGER, name VARCHAR);
ALTER TABLE demo.test.users ADD COLUMN email VARCHAR;
ALTER TABLE demo.test.users RENAME COLUMN name TO full_name;
DROP TABLE demo.test.users;

-- CREATE TABLE AS SELECT (requires InsertableTable)
CREATE TABLE demo.test.backup AS SELECT * FROM demo.test.users;
```

See [examples/ddl](examples/ddl/) for a complete implementation.

## Filter Pushdown (Predicate Pushdown)

DuckDB can push filter predicates to the server for optimized query execution. The `filter` package provides parsing and SQL encoding for DuckDB's filter JSON format:

```go
import "github.com/hugr-lab/airport-go/filter"

func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    if opts.Filter != nil {
        // Parse filter JSON
        fp, err := filter.Parse(opts.Filter)
        if err != nil {
            return nil, err
        }

        // Encode to SQL WHERE clause
        enc := filter.NewDuckDBEncoder(nil)
        whereClause := enc.EncodeFilters(fp)
        // Use whereClause with your database query
    }
    // Return filtered data...
}
```

Supported expressions:
- Comparisons: `=`, `<>`, `<`, `>`, `<=`, `>=`, `IN`, `NOT IN`, `BETWEEN`
- Logical: `AND`, `OR`, `NOT`
- Functions: `LOWER`, `UPPER`, `LENGTH`, etc.
- Operators: `IS NULL`, `IS NOT NULL`
- Type casts and CASE expressions

See [examples/filter](examples/filter/) for complete examples and [Airport Extension docs](https://airport.query.farm/server_predicate_pushdown.html) for filter format specification.

## Named Catalogs

Implement `NamedCatalog` to give your catalog a name for DuckDB ATTACH statements:

```go
type MyCatalog struct { /* ... */ }

func (c *MyCatalog) Name() string {
    return "analytics"
}

// Also implement Catalog interface methods...
```

In DuckDB:

```sql
-- Server returns catalog name "analytics"
ATTACH 'analytics' AS my_db (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

SELECT * FROM my_db.schema.table;
```

## Multi-Catalog Server

Serve multiple catalogs from a single endpoint with dynamic management:

```go
// Create named catalogs
type SalesCatalog struct { /* ... */ }
func (c *SalesCatalog) Name() string { return "sales" }

type AnalyticsCatalog struct { /* ... */ }
func (c *AnalyticsCatalog) Name() string { return "analytics" }

// Create multi-catalog server
config := airport.MultiCatalogServerConfig{
    Catalogs: []catalog.Catalog{&SalesCatalog{}, &AnalyticsCatalog{}},
}

opts := airport.MultiCatalogServerOptions(config)
grpcServer := grpc.NewServer(opts...)

// Returns *MultiCatalogServer for dynamic management
mcs, _ := airport.NewMultiCatalogServer(grpcServer, config)

// Add/remove catalogs at runtime
mcs.AddCatalog(&InventoryCatalog{})
mcs.RemoveCatalog("inventory")
```

Clients specify the target catalog via the `airport-catalog` gRPC metadata header.
Requests without the header route to the default catalog (empty name).

For per-catalog authorization, implement `auth.CatalogAuthorizer`:

```go
type MultiCatalogAuth struct{}

func (a *MultiCatalogAuth) Authenticate(ctx context.Context, token string) (string, error) {
    // Validate token, return identity
    return "user1", nil
}

func (a *MultiCatalogAuth) AuthorizeCatalog(ctx context.Context, catalogName string) (context.Context, error) {
    identity := auth.IdentityFromContext(ctx)
    // Check if identity can access catalogName
    return ctx, nil
}
```

See [examples/multicatalog](examples/multicatalog/) for a complete implementation.

## Architecture

The package follows an interface-based design:

- **Catalog**: Top-level interface for querying schemas
- **NamedCatalog**: Extends Catalog with a name for DuckDB ATTACH
- **Schema**: Interface for querying tables and functions
- **Table**: Interface providing Arrow schema and scan function
- **ScalarFunction/TableFunction**: Interfaces for custom functions
- **DynamicCatalog**: Extends Catalog with CREATE/DROP SCHEMA
- **DynamicSchema**: Extends Schema with CREATE/DROP/RENAME TABLE
- **DynamicTable**: Extends Table with ADD/DROP/RENAME COLUMN and field operations
- **MultiCatalogServer**: Serves multiple catalogs from a single endpoint
- **CatalogAuthorizer**: Per-catalog authorization interface
- **DML Interfaces**:
  - `InsertableTable`: INSERT operations
  - `UpdatableTable`/`UpdatableBatchTable`: UPDATE operations (batch interface preferred)
  - `DeletableTable`/`DeletableBatchTable`: DELETE operations (batch interface preferred)

You can either:
- Use `NewCatalogBuilder()` for static catalogs (quickest)
- Implement the `Catalog` interface for dynamic catalogs
- Use `NewMultiCatalogServer()` to serve multiple catalogs from one endpoint
- Implement `DynamicCatalog`/`DynamicSchema`/`DynamicTable` for DDL support

## Documentation

- [GoDoc](https://pkg.go.dev/github.com/hugr-lab/airport-go) - Full API reference
- [Protocol Overview](docs/protocol.md) - Airport protocol, Flight actions, message formats
- [API Guide](docs/api-guide.md) - Interface documentation and server configuration
- [Implementation Guide](docs/implementation.md) - Guide for implementing custom catalogs
- [Examples](examples/) - Common usage patterns

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

The repository uses Go workspaces with three modules:

```
airport-go/
├── go.mod               # Main library (no DuckDB dependency)
├── go.work              # Workspace configuration
├── *.go                 # Root package files and unit tests
├── catalog/             # Catalog interfaces and types
├── auth/                # Authentication (bearer token)
├── filter/              # Filter pushdown parsing and SQL encoding
├── flight/              # Flight server implementation
├── internal/            # Internal packages (serialization, etc.)
├── docs/                # Protocol and API documentation
│   ├── protocol.md     # Airport protocol overview
│   ├── api-guide.md    # Interface documentation
│   └── implementation.md # Implementation guide
├── examples/            # Example implementations (separate module)
│   ├── go.mod          # Examples module
│   ├── basic/          # Basic server example
│   ├── auth/           # Authenticated server example
│   ├── ddl/            # DDL operations (CREATE/DROP/ALTER)
│   ├── dml/            # DML operations (INSERT/UPDATE/DELETE)
│   ├── dynamic/        # Dynamic catalog example
│   ├── filter/         # Filter pushdown example
│   ├── functions/      # Scalar and table functions example
│   ├── multicatalog/   # Multi-catalog server with dynamic add/remove
│   └── timetravel/     # Time travel queries example
└── tests/               # Tests (separate module, with DuckDB)
    ├── go.mod          # Tests module
    ├── integration/    # Integration tests
    └── benchmarks/     # Performance benchmarks
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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## References

- [DuckDB Airport Extension](https://airport.query.farm)
- [Apache Arrow Go](https://github.com/apache/arrow/go)
- [Arrow Flight Protocol](https://arrow.apache.org/docs/format/Flight.html)
