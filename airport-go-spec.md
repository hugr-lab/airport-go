# Airport Go Package — Apache Arrow Flight Server for DuckDB Airport Extension

## Overview

A Go package `airport` providing a high-level API for building Arrow Flight servers compatible with the DuckDB Airport Extension.

The package abstracts low-level Arrow Flight protocol details, allowing developers to focus on business logic: serving data, handling DML operations, and implementing functions.

**Design Philosophy:**
- High-level API hides Flight protocol complexity (tickets, FlightInfo, descriptors)
- Low-level API available for advanced use cases
- Users should be able to implement servers without importing `arrow/flight` package directly

**Reference Documentation:** https://airport.query.farm

---

## Code Style Requirements

### General Principles

1. **Idiomatic Go** — follow [Effective Go](https://go.dev/doc/effective_go) and [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
2. **Simplicity** — prefer explicit over implicit, avoid over-engineering
3. **Composition** — use interfaces for optional capabilities
4. **Testability** — all public APIs must be easily testable

### Naming Conventions

```go
// ✅ Correct: no Get prefix for getters
type Table interface {
    Name() string
    Schema() *arrow.Schema
    Comment() string
}

// ❌ Wrong: redundant prefixes
type Table interface {
    GetName() string
    GetSchema() *arrow.Schema
}

// ✅ Correct: uppercase acronyms
type HTTPClient struct{}
type URLParser struct{}
var userID string

// ❌ Wrong
type HttpClient struct{}
type UrlParser struct{}
var userId string

// ✅ Correct: -er suffix for single-method interfaces
type Reader interface {
    Read(ctx context.Context) (arrow.Record, error)
}

// ✅ Correct: descriptive names for multi-method interfaces
type TableWriter interface {
    Insert(ctx context.Context, data arrow.RecordReader) (*DMLResult, error)
    Update(ctx context.Context, data arrow.RecordReader) (*DMLResult, error)
    Delete(ctx context.Context, rowIDs arrow.RecordReader) (*DMLResult, error)
}
```

### Error Handling

```go
// ✅ Correct: typed sentinel errors
var (
    ErrTableNotFound    = errors.New("airport: table not found")
    ErrSchemaNotFound   = errors.New("airport: schema not found")
    ErrFunctionNotFound = errors.New("airport: function not found")
    ErrNotSupported     = errors.New("airport: operation not supported")
    ErrInvalidRowID     = errors.New("airport: invalid row id")
    ErrUnauthorized     = errors.New("airport: unauthorized")
)

// ✅ Correct: wrap errors with context
func (s *schema) Table(name string) (Table, error) {
    t, ok := s.tables[name]
    if !ok {
        return nil, fmt.Errorf("%w: %s.%s", ErrTableNotFound, s.name, name)
    }
    return t, nil
}
```

### Context and Cancellation

```go
// ✅ Correct: context.Context is always the first parameter
func (t *table) Scan(ctx context.Context, opts ScanOptions) (arrow.RecordReader, error)

// ✅ Correct: check cancellation in long operations
func (t *table) Scan(ctx context.Context, opts ScanOptions) (arrow.RecordReader, error) {
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    // ... continue work
}
```

### Documentation

All public types and methods must be documented in English.

```go
// Table represents a data table accessible through Airport.
// Tables may support reading (Scan), writing (Insert/Update/Delete),
// and schema modifications depending on the implementation.
type Table interface {
    // Name returns the table name without schema qualification.
    Name() string
    
    // ArrowSchema returns the Arrow schema of the table including metadata.
    ArrowSchema() *arrow.Schema
}
```

---

## Architecture

### Two-Level API Design

The package provides two levels of abstraction:

**High-Level API (Recommended)**
- Simple interfaces without Flight-specific types
- Automatic handling of tickets, FlightInfo, serialization
- Users implement business logic only
- No need to import `arrow/flight` package

**Low-Level API (Advanced)**
- Direct access to Flight protocol primitives
- Custom ticket encoding/decoding
- Custom FlightInfo generation
- For complex distributed scenarios

### Airport Protocol (Internal)

The Airport extension uses these Flight RPC methods (handled internally):

| SQL Operation | Flight RPC | Internal Handler |
|--------------|------------|------------------|
| `SELECT` | `DoGet` | `tableHandler.doGet()` |
| `INSERT` | `DoExchange` | `tableHandler.doExchange()` |
| `UPDATE` | `DoExchange` | `tableHandler.doExchange()` |
| `DELETE` | `DoExchange` | `tableHandler.doExchange()` |
| Scalar Function | `DoExchange` | `functionHandler.doExchange()` |
| Table Function | `DoAction` + `DoGet` | `functionHandler.doAction/doGet()` |
| DDL | `DoAction` | `catalogHandler.doAction()` |

### Serialization (Internal)

- **Parameters**: MessagePack
- **Data streaming**: Arrow IPC format
- **Compression**: ZStandard for catalog serialization
- **DML result metadata**: MessagePack

---

## Core Types

### Request Context

```go
// Request contains metadata about the current request.
// Passed to all handler methods for authentication and context.
type Request struct {
    // Token is the bearer token from Authorization header.
    // Use this as API key for authentication.
    Token string
    
    // SessionID is the unique client session identifier.
    SessionID string
    
    // TransactionID is the transaction identifier (empty if no transaction).
    TransactionID string
    
    // UserAgent identifies the Airport client version.
    UserAgent string
}
```

### Filters — Predicate Pushdown

```go
// Filters represents serialized DuckDB filter expressions.
// Provides both raw access and parsed structure.
type Filters struct {
    raw []byte
}

// Raw returns the raw JSON bytes of the filters.
func (f *Filters) Raw() []byte

// IsEmpty returns true if no filters are present.
func (f *Filters) IsEmpty() bool

// Parse deserializes the filters into a structured format.
// Returns a slice of filter expressions and column name mapping.
//
// Each filter is a map with keys like:
//   - "expression_class": "BOUND_COMPARISON", "BOUND_CONJUNCTION", etc.
//   - "type": "COMPARE_EQUAL", "CONJUNCTION_OR", etc.
//   - "left", "right": operands for binary expressions
//   - "children": operands for n-ary expressions
//
// columnNames maps column binding indices to column names.
func (f *Filters) Parse() (filters []map[string]any, columnNames []string, err error)

// NewFilters creates a Filters instance from raw JSON bytes.
func NewFilters(raw []byte) *Filters
```

### TimePoint — Time Travel

```go
// TimePoint represents a point-in-time for time travel queries.
type TimePoint struct {
    // Unit specifies the type: "timestamp" or "version".
    Unit string
    
    // Value is the timestamp (ISO 8601) or version number as string.
    Value string
}

// IsZero returns true if no time point is specified.
func (t *TimePoint) IsZero() bool
```

### Scan Options

```go
// ScanOptions defines parameters for Scan operations.
type ScanOptions struct {
    // Columns lists column names for projection (nil = all columns).
    Columns []string
    
    // Filters contains DuckDB filter expressions for pushdown.
    // Server may use for optimization; DuckDB applies final filtering.
    Filters *Filters
    
    // Limit is the maximum number of rows (0 = no limit).
    Limit int64
    
    // TimePoint specifies point-in-time for time travel queries.
    TimePoint *TimePoint
}
```

### DML Result

```go
// DMLResult contains the result of INSERT/UPDATE/DELETE operations.
type DMLResult struct {
    // RowsAffected is the number of rows affected.
    RowsAffected int64
    
    // Returning contains data for RETURNING clause (nil if not requested).
    // Caller is responsible for proper cleanup.
    Returning arrow.RecordReader
}
```

### Function Parameters

```go
// Parameter describes a scalar function parameter.
type Parameter struct {
    Name     string
    Type     arrow.DataType
    Required bool
    Comment  string
    IsAny    bool // true if parameter accepts any type
}

// TableFunctionParameter describes a table function parameter.
type TableFunctionParameter struct {
    Name       string
    Type       arrow.DataType // for scalar parameter (nil if table)
    IsTable    bool           // true if parameter accepts table input
    Required   bool
    Comment    string
    IsAny      bool // true if parameter accepts any type
}
```

---

## High-Level API (Recommended)

### Table Interface

```go
// Table represents a readable data table.
type Table interface {
    // Name returns the table name.
    Name() string
    
    // Comment returns the table description.
    Comment() string
    
    // ArrowSchema returns the Arrow schema of the table.
    // Field metadata may contain:
    //   - "is_any_type": marks field as DuckDB ANY type
    ArrowSchema() *arrow.Schema
    
    // Scan reads data from the table.
    Scan(ctx context.Context, req *Request, opts ScanOptions) (arrow.RecordReader, error)
}

// WritableTable extends Table with INSERT support.
type WritableTable interface {
    Table
    
    // Insert adds rows to the table.
    // returningCols lists column names for RETURNING (nil = none).
    Insert(
        ctx context.Context,
        req *Request,
        data arrow.RecordReader,
        returningCols []string,
    ) (*DMLResult, error)
}

// ModifiableTable extends WritableTable with UPDATE/DELETE support.
// Requires rowid column in the schema.
type ModifiableTable interface {
    WritableTable
    
    // RowIDColumn returns the rowid column name (e.g., "rowid").
    // Empty string means UPDATE/DELETE not supported.
    RowIDColumn() string
    
    // Update modifies rows. Data contains rowid + new values.
    Update(
        ctx context.Context,
        req *Request,
        data arrow.RecordReader,
        returningCols []string,
    ) (*DMLResult, error)
    
    // Delete removes rows. Data contains only rowid column.
    Delete(
        ctx context.Context,
        req *Request,
        rowIDs arrow.RecordReader,
        returningCols []string,
    ) (*DMLResult, error)
}

// AlterableTable supports schema modifications.
type AlterableTable interface {
    Table
    
    AddColumn(ctx context.Context, req *Request, name string, dataType arrow.DataType) error
    DropColumn(ctx context.Context, req *Request, name string) error
    RenameColumn(ctx context.Context, req *Request, oldName, newName string) error
    ChangeColumnType(ctx context.Context, req *Request, name string, newType arrow.DataType) error
    SetNotNull(ctx context.Context, req *Request, column string) error
    DropNotNull(ctx context.Context, req *Request, column string) error
    SetDefault(ctx context.Context, req *Request, column string, defaultExpr string) error
}
```

### Function Interfaces

```go
// ScalarFunction represents a function returning one value per row.
type ScalarFunction interface {
    // Name returns the function name.
    Name() string
    
    // Comment returns the function description.
    Comment() string
    
    // Parameters returns parameter definitions.
    Parameters() []Parameter
    
    // ReturnType returns the result type.
    ReturnType() arrow.DataType
    
    // Execute processes batches of input values.
    // Input contains parameter columns; returns single result column.
    Execute(
        ctx context.Context,
        req *Request,
        input arrow.RecordReader,
    ) (arrow.RecordReader, error)
}

// TableFunction represents a function returning rows.
type TableFunction interface {
    // Name returns the function name.
    Name() string
    
    // Comment returns the function description.
    Comment() string
    
    // Parameters returns parameter definitions.
    Parameters() []TableFunctionParameter
    
    // ReturnSchema returns result schema (nil if dynamic).
    ReturnSchema() *arrow.Schema
    
    // Execute runs the function with scalar parameters.
    // params contains parameter values from function call.
    Execute(
        ctx context.Context,
        req *Request,
        params map[string]any,
        opts ScanOptions,
    ) (arrow.RecordReader, error)
}

// InOutTableFunction processes streaming table input.
type InOutTableFunction interface {
    TableFunction
    
    // TableInputParam returns the table parameter name.
    TableInputParam() string
    
    // ExecuteWithInput processes table input.
    // For each input batch, may produce zero or more output batches.
    ExecuteWithInput(
        ctx context.Context,
        req *Request,
        input arrow.RecordReader,
        params map[string]any,
        opts ScanOptions,
    ) (arrow.RecordReader, error)
}
```

### Schema Interface

```go
// Schema represents a namespace containing tables and functions.
type Schema interface {
    // Name returns the schema name.
    Name() string
    
    // Comment returns the schema description.
    Comment() string
    
    // Tables returns all tables.
    Tables(ctx context.Context, req *Request) ([]Table, error)
    
    // Table returns a table by name.
    Table(ctx context.Context, req *Request, name string) (Table, error)
    
    // ScalarFunctions returns all scalar functions.
    ScalarFunctions(ctx context.Context, req *Request) ([]ScalarFunction, error)
    
    // ScalarFunction returns a function by name.
    ScalarFunction(ctx context.Context, req *Request, name string) (ScalarFunction, error)
    
    // TableFunctions returns all table functions.
    TableFunctions(ctx context.Context, req *Request) ([]TableFunction, error)
    
    // TableFunction returns a function by name.
    TableFunction(ctx context.Context, req *Request, name string) (TableFunction, error)
}

// MutableSchema supports DDL operations.
type MutableSchema interface {
    Schema
    
    // CreateTable creates a new table.
    CreateTable(ctx context.Context, req *Request, name string, schema *arrow.Schema) (Table, error)
    
    // DropTable removes a table.
    DropTable(ctx context.Context, req *Request, name string) error
    
    // RenameTable renames a table.
    RenameTable(ctx context.Context, req *Request, oldName, newName string) error
}
```

### Catalog Interface

```go
// Catalog represents the root database catalog.
type Catalog interface {
    // Version returns the current catalog version.
    // DuckDB caches catalog and checks version before each query.
    // DDL operations should increment this version.
    Version(ctx context.Context, req *Request) (int64, error)
    
    // Schemas returns all schemas.
    Schemas(ctx context.Context, req *Request) ([]Schema, error)
    
    // Schema returns a schema by name.
    Schema(ctx context.Context, req *Request, name string) (Schema, error)
}

// MutableCatalog supports schema creation/deletion.
type MutableCatalog interface {
    Catalog
    
    // CreateSchema creates a new schema.
    CreateSchema(ctx context.Context, req *Request, name string) (Schema, error)
    
    // DropSchema removes a schema.
    DropSchema(ctx context.Context, req *Request, name string) error
}

// TransactionalCatalog supports transactions.
type TransactionalCatalog interface {
    Catalog
    
    // CreateTransaction starts a transaction and returns its ID.
    CreateTransaction(ctx context.Context, req *Request) (string, error)
}
```

---

## Simplified Builder API

For simple use cases, a builder API provides even simpler registration:

```go
// CatalogBuilder provides fluent API for building catalogs.
type CatalogBuilder struct {
    // private fields
}

// NewCatalogBuilder creates a new catalog builder.
func NewCatalogBuilder() *CatalogBuilder

// Schema adds or gets a schema builder.
func (b *CatalogBuilder) Schema(name string) *SchemaBuilder

// Build creates the catalog.
func (b *CatalogBuilder) Build() Catalog

// SchemaBuilder provides fluent API for building schemas.
type SchemaBuilder struct {
    // private fields
}

// Comment sets schema description.
func (b *SchemaBuilder) Comment(comment string) *SchemaBuilder

// Table registers a table.
func (b *SchemaBuilder) Table(table Table) *SchemaBuilder

// ScalarFunc registers a scalar function.
func (b *SchemaBuilder) ScalarFunc(fn ScalarFunction) *SchemaBuilder

// TableFunc registers a table function.
func (b *SchemaBuilder) TableFunc(fn TableFunction) *SchemaBuilder

// SimpleTable registers a table from a simple definition.
func (b *SchemaBuilder) SimpleTable(def SimpleTableDef) *SchemaBuilder

// SimpleTableDef provides minimal table definition.
type SimpleTableDef struct {
    Name    string
    Comment string
    Schema  *arrow.Schema
    
    // ScanFunc is called for each SELECT.
    ScanFunc func(ctx context.Context, req *Request, opts ScanOptions) (arrow.RecordReader, error)
    
    // InsertFunc is called for INSERT (optional).
    InsertFunc func(ctx context.Context, req *Request, data arrow.RecordReader) (*DMLResult, error)
}
```

### Usage Example

```go
package main

import (
    "context"
    "log"
    "net"
    
    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/array"
    "github.com/hugr-lab/airport-go"
    "google.golang.org/grpc"
)

func main() {
    // Build catalog using builder API
    catalog := airport.NewCatalogBuilder().
        Schema("main").
            Comment("Main schema").
            SimpleTable(airport.SimpleTableDef{
                Name:    "users",
                Comment: "User accounts",
                Schema: arrow.NewSchema([]arrow.Field{
                    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
                    {Name: "name", Type: arrow.BinaryTypes.String},
                    {Name: "email", Type: arrow.BinaryTypes.String},
                }, nil),
                ScanFunc: scanUsers,
            }).
            ScalarFunc(&UppercaseFunc{}).
        Build()
    
    // Create and start server
    grpcServer := grpc.NewServer()
    
    srv, err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: catalog,
        Auth: airport.BearerAuth(func(token string) (string, error) {
            if token == "secret-api-key" {
                return "user1", nil
            }
            return "", airport.ErrUnauthorized
        }),
    })
    if err != nil {
        log.Fatal(err)
    }
    
    lis, _ := net.Listen("tcp", ":50051")
    log.Println("Server listening on :50051")
    grpcServer.Serve(lis)
}

func scanUsers(ctx context.Context, req *airport.Request, opts airport.ScanOptions) (arrow.RecordReader, error) {
    // Return your data as arrow.RecordReader
    // ...
}

type UppercaseFunc struct{}

func (f *UppercaseFunc) Name() string    { return "uppercase" }
func (f *UppercaseFunc) Comment() string { return "Converts string to uppercase" }

func (f *UppercaseFunc) Parameters() []airport.Parameter {
    return []airport.Parameter{
        {Name: "input", Type: arrow.BinaryTypes.String, Required: true},
    }
}

func (f *UppercaseFunc) ReturnType() arrow.DataType {
    return arrow.BinaryTypes.String
}

func (f *UppercaseFunc) Execute(
    ctx context.Context,
    req *airport.Request,
    input arrow.RecordReader,
) (arrow.RecordReader, error) {
    // Process input batches, return uppercase results
    // ...
}
```

---

## Server API

### Server Configuration

```go
// Server represents an Airport Flight server.
type Server struct {
    // private fields
}

// ServerConfig contains server configuration.
type ServerConfig struct {
    // Catalog is the catalog implementation (required).
    Catalog Catalog
    
    // Allocator is Arrow memory allocator (default: memory.DefaultAllocator).
    Allocator memory.Allocator
    
    // Logger for diagnostics (default: no-op).
    Logger *slog.Logger
    
    // Auth is the authenticator (nil = allow all).
    Auth Authenticator
    
    // CompressionLevel for ZStandard (default: 3).
    CompressionLevel int
}

// Authenticator validates bearer tokens.
type Authenticator interface {
    // Authenticate validates token and returns user identity.
    Authenticate(ctx context.Context, token string) (identity string, err error)
}

// BearerAuth creates an Authenticator from a validation function.
func BearerAuth(validate func(token string) (identity string, err error)) Authenticator

// NewServer creates a new Airport server.
func NewServer(grpcServer *grpc.Server, config ServerConfig) (*Server, error)

// IncrementCatalogVersion increments catalog version (thread-safe).
// Call after DDL operations to invalidate client caches.
func (s *Server) IncrementCatalogVersion() int64

// SetCatalogVersion sets specific version value.
func (s *Server) SetCatalogVersion(version int64)

// CatalogVersion returns current version.
func (s *Server) CatalogVersion() int64
```

---

## Low-Level API (Advanced)

For advanced use cases requiring direct Flight protocol control.
This API uses native Arrow Flight types — users must import `github.com/apache/arrow/go/v18/arrow/flight`.

```go
import "github.com/apache/arrow/go/v18/arrow/flight"

// LowLevelCatalog provides direct Flight protocol access.
// Implement this instead of Catalog for custom ticket/FlightInfo handling.
type LowLevelCatalog interface {
    // ListFlights returns FlightInfo for all objects.
    // criteria is the raw criteria bytes from the client.
    ListFlights(ctx context.Context, req *Request, criteria []byte) ([]*flight.FlightInfo, error)
    
    // GetFlightInfo returns metadata for a specific object.
    GetFlightInfo(ctx context.Context, req *Request, desc *flight.FlightDescriptor) (*flight.FlightInfo, error)
    
    // GetSchema returns schema without executing.
    GetSchema(ctx context.Context, req *Request, desc *flight.FlightDescriptor) (*arrow.Schema, error)
    
    // DoGet handles data retrieval with custom ticket.
    DoGet(ctx context.Context, req *Request, ticket *flight.Ticket) (arrow.RecordReader, error)
    
    // DoExchange handles bidirectional streaming.
    // Returns a handler that processes the exchange.
    DoExchange(ctx context.Context, req *Request, desc *flight.FlightDescriptor) (ExchangeHandler, error)
    
    // DoAction handles actions with custom serialization.
    // Returns iterator over results.
    DoAction(ctx context.Context, req *Request, action *flight.Action) (ActionResultIterator, error)
}

// ExchangeHandler processes bidirectional DoExchange streams.
type ExchangeHandler interface {
    // Schema returns the output schema.
    Schema() *arrow.Schema
    
    // Process handles input records and produces output.
    // Called for each input batch. May produce zero or more output batches.
    // done is true when input stream is exhausted.
    Process(record arrow.Record, done bool) ([]arrow.Record, error)
    
    // FinalMetadata returns the final msgpack metadata message.
    // Called after all processing is complete.
    FinalMetadata() ([]byte, error)
    
    // Close releases resources.
    Close() error
}

// ActionResultIterator iterates over action results.
type ActionResultIterator interface {
    // Next returns the next result.
    // Returns nil, io.EOF when exhausted.
    Next() (*flight.Result, error)
    
    // Close releases resources.
    Close() error
}
```

### Low-Level Usage Example

```go
package main

import (
    "context"
    
    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/flight"
    "github.com/hugr-lab/airport-go"
)

type MyLowLevelCatalog struct {
    // your implementation
}

func (c *MyLowLevelCatalog) GetFlightInfo(
    ctx context.Context,
    req *airport.Request,
    desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
    // Custom ticket encoding
    ticket := encodeMyCustomTicket(desc)
    
    // Build FlightInfo with custom endpoints
    info := &flight.FlightInfo{
        Schema: mySchema,
        Endpoint: []*flight.FlightEndpoint{
            {
                Ticket:   &flight.Ticket{Ticket: ticket},
                Location: []*flight.Location{{Uri: "grpc://node1:50051"}},
            },
            {
                Ticket:   &flight.Ticket{Ticket: ticket},
                Location: []*flight.Location{{Uri: "grpc://node2:50051"}},
            },
        },
        TotalRecords: -1,
        TotalBytes:   -1,
    }
    return info, nil
}

func (c *MyLowLevelCatalog) DoGet(
    ctx context.Context,
    req *airport.Request,
    ticket *flight.Ticket,
) (arrow.RecordReader, error) {
    // Decode custom ticket
    params := decodeMyCustomTicket(ticket.Ticket)
    
    // Return data based on decoded parameters
    return fetchData(ctx, params)
}

// ... implement other methods
```

---

## Internal Implementation Notes

### Compression

The package internally uses ZStandard compression for catalog serialization as required by Airport protocol:

```go
// internal/encoding/compress.go

// CompressZstd compresses data using ZStandard.
func CompressZstd(data []byte, level int) ([]byte, error)

// DecompressZstd decompresses ZStandard data.
func DecompressZstd(data []byte) ([]byte, error)
```

### Ticket Encoding

Tickets are internally encoded to identify objects:

```go
// internal/protocol/ticket.go

type TicketData struct {
    Schema    string
    Object    string
    Type      ObjectType // table, function, etc.
    Columns   []string
    Filters   []byte
    TimePoint *TimePoint
    Extra     []byte // for custom data
}

func EncodeTicket(t *TicketData) ([]byte, error)
func DecodeTicket(data []byte) (*TicketData, error)
```

### Action Handlers

All Airport actions are handled internally:

| Action | Handler |
|--------|---------|
| `catalog_version` | Returns `Catalog.Version()` |
| `list_schemas` | Serializes `Catalog.Schemas()` with ZStandard |
| `endpoints` | Returns server locations |
| `create_schema` | Calls `MutableCatalog.CreateSchema()` |
| `drop_schema` | Calls `MutableCatalog.DropSchema()` |
| `create_table` | Calls `MutableSchema.CreateTable()` |
| `drop_table` | Calls `MutableSchema.DropTable()` |
| `rename_table` | Calls `MutableSchema.RenameTable()` |
| `add_column` | Calls `AlterableTable.AddColumn()` |
| `remove_column` | Calls `AlterableTable.DropColumn()` |
| `rename_column` | Calls `AlterableTable.RenameColumn()` |
| `change_column_type` | Calls `AlterableTable.ChangeColumnType()` |
| `set_not_null` | Calls `AlterableTable.SetNotNull()` |
| `drop_not_null` | Calls `AlterableTable.DropNotNull()` |
| `set_default` | Calls `AlterableTable.SetDefault()` |
| `table_function_flight_info` | Generates FlightInfo for table function |
| `create_transaction` | Calls `TransactionalCatalog.CreateTransaction()` |

---

## Package Structure

```
airport/
├── server.go              # Server, ServerConfig, NewServer
├── catalog.go             # Catalog, Schema interfaces
├── table.go               # Table, WritableTable, ModifiableTable
├── function.go            # ScalarFunction, TableFunction
├── options.go             # ScanOptions, Filters, TimePoint
├── context.go             # Request
├── result.go              # DMLResult
├── errors.go              # Standard errors
├── auth.go                # Authenticator, BearerAuth
├── builder.go             # CatalogBuilder, SchemaBuilder
├── lowlevel.go            # LowLevelCatalog, ExchangeHandler, etc.
│
├── internal/
│   ├── flight/
│   │   ├── server.go      # Flight server implementation
│   │   ├── doget.go       # DoGet handler
│   │   ├── doexchange.go  # DoExchange handler
│   │   ├── doaction.go    # DoAction handler
│   │   └── listflights.go # ListFlights, GetFlightInfo
│   │
│   ├── encoding/
│   │   ├── msgpack.go     # MessagePack codec
│   │   ├── compress.go    # ZStandard compression
│   │   └── filters.go     # Filter expression parsing
│   │
│   └── protocol/
│       ├── ticket.go      # Ticket encoding/decoding
│       ├── headers.go     # gRPC header constants
│       └── actions.go     # Action handlers
│
├── examples/
│   ├── simple/            # Minimal example with builder
│   ├── postgres/          # PostgreSQL backend
│   └── advanced/          # Low-level API example
│
└── _test/
    ├── server_test.go
    ├── builder_test.go
    └── integration_test.go
```

---

## Dependencies

```go
require (
    github.com/apache/arrow/go/v18
    google.golang.org/grpc
    github.com/vmihailenco/msgpack/v5
    github.com/klauspost/compress  // for zstd
)
```

---

## Quality Requirements

1. **Tests**: minimum 80% coverage for public API
2. **Benchmarks**: for critical paths (Scan, encoding, compression)
3. **Documentation**: all public types and methods documented in English
4. **Examples**: working examples for each API level
5. **Linting**: `golangci-lint` with no warnings
6. **Race detector**: all tests pass with `-race`
7. **API separation**: 
   - High-level API must not require importing `arrow/flight`
   - Low-level API uses native `arrow/flight` types directly

## Integration Testing

All functionality must be verified against real DuckDB with Airport extension using
[github.com/duckdb/duckdb-go](https://github.com/duckdb/duckdb-go).

### Test Setup

```go
package airport_test

import (
    "context"
    "database/sql"
    "net"
    "testing"
    
    "github.com/hugr-lab/airport-go"
    _ "github.com/duckdb/duckdb-go" // DuckDB driver
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials"
)

func setupTestServer(t *testing.T, catalog airport.Catalog) (string, func()) {
    t.Helper()
    
    // Create listener on random port
    lis, err := net.Listen("tcp", "localhost:0")
    if err != nil {
        t.Fatalf("failed to listen: %v", err)
    }
    
    // Create gRPC server with TLS (required for DuckDB)
    creds, err := credentials.NewServerTLSFromFile("testdata/server.crt", "testdata/server.key")
    if err != nil {
        t.Fatalf("failed to load TLS: %v", err)
    }
    grpcServer := grpc.NewServer(grpc.Creds(creds))
    
    // Create Airport server
    srv, err := airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: catalog,
    })
    if err != nil {
        t.Fatalf("failed to create server: %v", err)
    }
    _ = srv
    
    go grpcServer.Serve(lis)
    
    addr := lis.Addr().String()
    cleanup := func() {
        grpcServer.Stop()
        lis.Close()
    }
    
    return addr, cleanup
}

func setupDuckDB(t *testing.T, serverAddr string) *sql.DB {
    t.Helper()
    
    db, err := sql.Open("duckdb", "")
    if err != nil {
        t.Fatalf("failed to open duckdb: %v", err)
    }
    
    // Install and load Airport extension
    _, err = db.Exec(`
        INSTALL airport FROM community;
        LOAD airport;
    `)
    if err != nil {
        t.Fatalf("failed to load airport extension: %v", err)
    }
    
    // Attach to test server
    _, err = db.Exec(`ATTACH 'test' (TYPE AIRPORT, location 'grpc+tls://` + serverAddr + `')`)
    if err != nil {
        t.Fatalf("failed to attach: %v", err)
    }
    
    return db
}
```

### Test Categories

#### 1. Catalog Discovery Tests

```go
func TestCatalogDiscovery(t *testing.T) {
    catalog := buildTestCatalog()
    addr, cleanup := setupTestServer(t, catalog)
    defer cleanup()
    
    db := setupDuckDB(t, addr)
    defer db.Close()
    
    // Test schema listing
    rows, err := db.Query("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'test'")
    // ... verify schemas
    
    // Test table listing
    rows, err = db.Query("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'test'")
    // ... verify tables
    
    // Test column info
    rows, err = db.Query("DESCRIBE test.main.users")
    // ... verify columns
}
```

#### 2. Query Execution Tests

```go
func TestSelectQuery(t *testing.T) {
    catalog := buildTestCatalog()
    addr, cleanup := setupTestServer(t, catalog)
    defer cleanup()
    
    db := setupDuckDB(t, addr)
    defer db.Close()
    
    // Simple SELECT
    rows, err := db.Query("SELECT id, name FROM test.main.users WHERE id = 1")
    // ... verify results
    
    // Aggregation
    var count int
    err = db.QueryRow("SELECT COUNT(*) FROM test.main.users").Scan(&count)
    // ... verify count
    
    // Projection pushdown
    rows, err = db.Query("SELECT name FROM test.main.users")
    // ... verify only requested columns returned
    
    // Filter pushdown
    rows, err = db.Query("SELECT * FROM test.main.users WHERE age > 30")
    // ... verify filter applied at source
}
```

#### 3. DML Tests

```go
func TestInsert(t *testing.T) {
    catalog := buildTestCatalog()
    addr, cleanup := setupTestServer(t, catalog)
    defer cleanup()
    
    db := setupDuckDB(t, addr)
    defer db.Close()
    
    // Single INSERT
    result, err := db.Exec("INSERT INTO test.main.users (id, name) VALUES (100, 'New User')")
    affected, _ := result.RowsAffected()
    // ... verify affected == 1
    
    // INSERT with RETURNING
    var id int
    err = db.QueryRow("INSERT INTO test.main.users (name) VALUES ('Another') RETURNING id").Scan(&id)
    // ... verify id returned
    
    // Bulk INSERT
    result, err = db.Exec("INSERT INTO test.main.users SELECT * FROM generate_series(1, 1000)")
    // ... verify 1000 rows affected
}

func TestUpdate(t *testing.T) {
    // UPDATE with WHERE clause
    // UPDATE with RETURNING
}

func TestDelete(t *testing.T) {
    // DELETE with WHERE clause
    // DELETE with RETURNING
}
```

#### 4. Function Tests

```go
func TestScalarFunction(t *testing.T) {
    catalog := buildTestCatalog()
    addr, cleanup := setupTestServer(t, catalog)
    defer cleanup()
    
    db := setupDuckDB(t, addr)
    defer db.Close()
    
    // Call scalar function
    var result string
    err := db.QueryRow("SELECT test.main.upper_case('hello')").Scan(&result)
    // ... verify result == "HELLO"
    
    // Function in SELECT
    rows, err := db.Query("SELECT id, test.main.upper_case(name) FROM test.main.users")
    // ... verify results
}

func TestTableFunction(t *testing.T) {
    // Regular table function
    rows, err := db.Query("SELECT * FROM test.main.generate_data(100)")
    // ... verify 100 rows
    
    // In-out table function
    rows, err = db.Query("SELECT * FROM test.main.transform_data(SELECT * FROM test.main.users)")
    // ... verify results
}
```

#### 5. Transaction Tests

```go
func TestTransactions(t *testing.T) {
    // Begin/commit
    // Begin/rollback
    // Transaction isolation
}
```

#### 6. Error Handling Tests

```go
func TestErrors(t *testing.T) {
    // Table not found
    // Permission denied
    // Invalid data types
    // Connection errors
}
```

### Test Data Generators

```go
// testdata/generators.go

func buildTestCatalog() airport.Catalog {
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
        {Name: "age", Type: arrow.PrimitiveTypes.Int32},
    }, nil)
    
    return airport.NewCatalogBuilder().
        Schema("main").
            SimpleTable(airport.SimpleTableDef{
                Name:   "users",
                Schema: schema,
                ScanFunc: func(ctx context.Context, req *airport.Request, params airport.ScanParams) (arrow.RecordReader, error) {
                    // Return test data
                    return generateTestData(schema, 100), nil
                },
            }).
            WritableTable(&TestWritableTable{}).
            ScalarFunc(&UpperCaseFunc{}).
        Build()
}
```

### Running Integration Tests

```bash
# Run all tests
go test -v ./...

# Run only integration tests (requires DuckDB)
go test -v -tags=integration ./...

# Run with race detector
go test -v -race ./...

# Run benchmarks
go test -bench=. ./...
```

### CI Configuration

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-go@v5
        with:
          go-version: '1.22'
      
      - name: Run unit tests
        run: go test -v -race -coverprofile=coverage.txt ./...
      
      - name: Run integration tests
        run: go test -v -tags=integration ./...
      
      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          files: coverage.txt
```
