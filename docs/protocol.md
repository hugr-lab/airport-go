# Airport Protocol Overview

This document describes the Airport protocol, which extends Apache Arrow Flight to support SQL-like catalog discovery and data manipulation operations.

## Background

[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) is a protocol for high-performance data transfer built on gRPC. It defines RPC methods for:

- **GetFlightInfo**: Get metadata about a dataset
- **DoGet**: Retrieve data as Arrow record batches
- **DoPut**: Send data to a server
- **DoExchange**: Bidirectional data streaming
- **DoAction**: Execute custom server actions

Airport builds on Flight by defining specific action types that enable SQL catalog semantics.

## Protocol Flow

### 1. Catalog Discovery

When DuckDB attaches an Airport server, it discovers the catalog structure via `GetFlightInfo`:

```
Client                                  Server
  │                                       │
  │──── GetFlightInfo(empty) ────────────>│
  │                                       │
  │<──── FlightInfo with catalog ─────────│
  │      (msgpack + zstd compressed)      │
```

The catalog metadata is returned as a compressed msgpack payload containing:
- Schema names and comments
- Table names, comments, and Arrow schemas
- Function signatures and types

### 2. Table Scan

When querying a table, DuckDB follows the Flight pattern:

```
Client                                  Server
  │                                       │
  │──── GetFlightInfo(table_ref) ────────>│
  │                                       │
  │<──── FlightInfo with endpoints ───────│
  │                                       │
  │──── DoGet(ticket) ───────────────────>│
  │                                       │
  │<──── Arrow RecordBatch stream ────────│
  │                                       │
```

### 3. Function Execution

Scalar and table functions are invoked via DoExchange for bidirectional streaming:

```
Client                                  Server
  │                                       │
  │──── DoExchange(function_ref) ────────>│
  │         (input batches)               │
  │                                       │
  │<──── Arrow RecordBatch result ────────│
  │                                       │
```

## Flight Actions (DoAction)

Airport defines the following DoAction types. Action names use `snake_case`.

### Metadata Actions

| Action Type | Description | Request | Response |
|-------------|-------------|---------|----------|
| `flight_info` | Get flight info for a table | Table reference | FlightInfo |
| `table_function_flight_info` | Get flight info for table function | Function ref + params | FlightInfo |
| `list_schemas` | List all schemas | Empty | Schema list |
| `endpoints` | Get endpoints with time-travel support | Table ref + time point | FlightInfo |
| `catalog_version` | Get catalog version | Empty | Version info |
| `column_statistics` | Get column statistics | Table ref + column | Statistics data |

### DDL Actions

| Action Type | Description | Request | Response |
|-------------|-------------|---------|----------|
| `create_schema` | Create a new schema | Schema name, options | Empty |
| `drop_schema` | Drop an existing schema | Schema name, options | Empty |
| `create_table` | Create a new table | Table ref, Arrow schema | Empty |
| `drop_table` | Drop an existing table | Table ref, options | Empty |
| `add_column` | Add column to table | Table ref, column schema | Empty |
| `remove_column` | Remove column from table | Table ref, column name | Empty |
| `rename_column` | Rename a column | Table ref, old/new names | Empty |
| `rename_table` | Rename a table | Old/new table refs | Empty |
| `change_column_type` | Change column type | Table ref, new type | Empty |
| `set_not_null` | Add NOT NULL constraint | Table ref, column | Empty |
| `drop_not_null` | Remove NOT NULL constraint | Table ref, column | Empty |
| `set_default` | Set column default value | Table ref, column, expr | Empty |
| `add_field` | Add field to struct column | Table ref, field schema | Empty |
| `rename_field` | Rename field in struct column | Table ref, old/new names | Empty |

### Transaction Actions

| Action Type | Description | Request | Response |
|-------------|-------------|---------|----------|
| `create_transaction` | Start a transaction | Empty | Transaction ID |
| `get_transaction_status` | Get transaction state | Transaction ID | Status |

Note: Commit and rollback are handled automatically by the server based on operation success/failure.

## DoExchange Operations

DML and function operations use bidirectional DoExchange streaming:

### DML Operations

| Operation Type | Description | Input | Output |
|----------------|-------------|-------|--------|
| `insert` | Insert rows into table | Arrow records | Affected count |
| `update` | Update rows in table | Arrow records with row IDs | Affected count |
| `delete` | Delete rows from table | Row IDs | Affected count |

### Function Operations

| Operation Type | Description | Input | Output |
|----------------|-------------|-------|--------|
| `scalar_function` | Execute scalar function | Input batches | Result arrays |
| `table_function` | Execute table function | Parameters | Result batches |
| `table_function_in_out` | Execute in-out table function | Input rows | Output rows |

## Message Formats

### Catalog Serialization

The catalog is serialized using msgpack with ZStandard compression:

```go
// Catalog structure
type CatalogInfo struct {
    Schemas []SchemaInfo `msgpack:"schemas"`
}

type SchemaInfo struct {
    Name                 string         `msgpack:"name"`
    Comment              string         `msgpack:"comment,omitempty"`
    Tables               []TableInfo    `msgpack:"tables"`
    ScalarFunctions      []FunctionInfo `msgpack:"scalar_functions,omitempty"`
    TableFunctions       []FunctionInfo `msgpack:"table_functions,omitempty"`
    TableFunctionsInOut  []FunctionInfo `msgpack:"table_functions_in_out,omitempty"`
}

type TableInfo struct {
    Name    string `msgpack:"name"`
    Comment string `msgpack:"comment,omitempty"`
    Schema  []byte `msgpack:"schema"` // Arrow IPC schema bytes
}

type FunctionInfo struct {
    Name       string              `msgpack:"name"`
    Comment    string              `msgpack:"comment,omitempty"`
    Signature  FunctionSignature   `msgpack:"signature"`
}
```

### Table Reference

Tables are identified using a three-part reference:

```
catalog.schema.table
```

In FlightDescriptor, this is encoded as a path:
```
FlightDescriptor{
    Type: FlightDescriptor_PATH,
    Path: []string{"catalog_name", "schema_name", "table_name"},
}
```

### Function Arguments

Table function arguments are passed as msgpack-encoded values in the ticket.

## Authentication

Airport uses Flight's built-in authentication mechanism:

### Bearer Token

The most common authentication method is bearer token:

1. Client sends token in `authorization` header
2. Server validates token in `BearerAuth` handler
3. Handler returns identity string for authorized users
4. Identity is available in context for all subsequent calls

```go
auth := airport.BearerAuth(func(token string) (string, error) {
    if valid, identity := validateToken(token); valid {
        return identity, nil
    }
    return "", airport.ErrUnauthorized
})
```

## Column Projection

Airport supports column projection to minimize data transfer:

```
GetFlightInfo request includes:
- Table reference
- List of requested columns

DoGet returns data for all columns - DuckDB handles projection client-side.
```

The ScanOptions structure carries projection information:

```go
type ScanOptions struct {
    Columns   []string   // Requested columns (nil = all)
    Filter    []byte     // Optional filter expression
    Limit     int64      // Optional row limit
    TimePoint *TimePoint // Optional time-travel point
}
```

Note: Table implementations receive column hints but must return full schema data. The server validates that returned data matches the table's declared schema.

## Filter Pushdown (Predicate Pushdown)

DuckDB can push WHERE clause predicates to the server via `ScanOptions.Filter`. The filter is a JSON-serialized expression tree.

### JSON Format

```json
{
  "filters": [
    {
      "expression_class": "BOUND_COMPARISON",
      "type": "COMPARE_EQUAL",
      "return_type": {"id": "BOOLEAN", "type_info": null},
      "children": [
        {
          "expression_class": "BOUND_COLUMN_REF",
          "binding": {"table_index": 0, "column_index": 1},
          "return_type": {"id": "VARCHAR", "type_info": null}
        },
        {
          "expression_class": "BOUND_CONSTANT",
          "value": {"is_null": false, "value": "active"},
          "return_type": {"id": "VARCHAR", "type_info": null}
        }
      ]
    }
  ],
  "column_binding_names_by_index": ["id", "status", "created_at"]
}
```

### Expression Classes

| Class | Description |
|-------|-------------|
| `BOUND_COMPARISON` | Comparison operators: COMPARE_EQUAL, COMPARE_GREATERTHAN, COMPARE_LESSTHAN, COMPARE_GREATERTHANOREQUALTO, COMPARE_LESSTHANOREQUALTO, COMPARE_NOTEQUAL |
| `BOUND_COLUMN_REF` | Column reference with table_index and column_index binding |
| `BOUND_CONSTANT` | Literal value with type information |
| `BOUND_CONJUNCTION` | Logical operators: CONJUNCTION_AND, CONJUNCTION_OR |
| `BOUND_OPERATOR` | Special operators like COMPARE_IN |
| `BOUND_FUNCTION` | Function calls with arguments and return types |

### Type IDs

Common `return_type.id` values:
- `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`
- `FLOAT`, `DOUBLE`, `DECIMAL`
- `VARCHAR`, `BLOB`
- `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_TZ`
- `LIST`, `STRUCT`, `MAP`

For the complete format specification, see the [DuckDB Airport Extension documentation](https://airport.query.farm/server_predicate_pushdown.html).

**Note**: Currently, server implementations must parse the raw JSON. Future versions of airport-go will provide helper types and functions for filter interpretation.

## Table Reference Endpoints (data:// URIs)

Table references use `data://` URIs instead of `grpc://` URIs in endpoint responses. When DuckDB receives a `data://` endpoint, it decodes the embedded function call and executes it locally rather than fetching data from the server.

### Protocol Flow

```
Client                                  Server
  |                                       |
  |---- DoAction(endpoints) ------------>|
  |      (table ref descriptor)           |
  |                                       |
  |<---- data:// URI endpoints ----------|
  |      (encoded function calls)         |
  |                                       |
  |---- Execute function locally ------->|
  |      (e.g., read_csv, read_parquet)   |
```

### data:// URI Format

```
data:application/x-msgpack-duckdb-function-call;base64,{BASE64_ENCODED_MSGPACK}
```

The base64-decoded content is a msgpack map with two fields:

```
{
    "function_name": "read_csv",        // DuckDB function to execute (string)
    "data": <raw Arrow IPC bytes>       // Function arguments (binary)
}
```

### Arrow IPC Argument Encoding

Function arguments are encoded as a single-row Arrow IPC stream:

- Positional arguments use field names `arg_0`, `arg_1`, etc.
- Named arguments use their parameter name as the field name
- The Arrow IPC uses stream format (not file format)

Example: `read_csv('/data/file.csv', header=true)` encodes as:

| arg_0 (VARCHAR) | header (BOOLEAN) |
|-----------------|------------------|
| /data/file.csv  | true             |

### Multiple Endpoints

A single table reference can return multiple function calls, each encoded as a separate `data://` URI in the endpoint response. This enables parallel reads (e.g., one endpoint per partition or file).

## Time Travel

Airport supports point-in-time queries via the `endpoints` action:

```go
type TimePoint struct {
    Unit  string // "timestamp", "version", or "snapshot"
    Value string // Time value in appropriate format
}
```

Example time points:
- `Unit="timestamp", Value="2024-01-15T10:30:00Z"`
- `Unit="version", Value="42"`
- `Unit="snapshot", Value="abc123def"`

## Error Handling

Errors are returned as gRPC status codes:

| Condition | gRPC Code | Description |
|-----------|-----------|-------------|
| Schema not found | NOT_FOUND | Requested schema doesn't exist |
| Table not found | NOT_FOUND | Requested table doesn't exist |
| Not authenticated | UNAUTHENTICATED | Missing or invalid auth |
| Permission denied | PERMISSION_DENIED | Valid auth, insufficient perms |
| Invalid argument | INVALID_ARGUMENT | Bad request format |
| Not implemented | UNIMPLEMENTED | Feature not supported |
| Already exists | ALREADY_EXISTS | DDL conflict |
| Internal error | INTERNAL | Server-side failure |

## Transport

Airport uses gRPC over HTTP/2 with these characteristics:

- **Streaming**: Large datasets are streamed as multiple Arrow batches
- **Compression**: ZStandard for metadata, optional for data streams
- **TLS**: Optional but recommended for production
- **Max message size**: Configurable (default 4MB, recommended 16MB for large batches)

## Compatibility

Airport aims for compatibility with:

- **DuckDB Airport Extension**: Primary client implementation
- **Standard Flight clients**: Basic GetFlightInfo/DoGet operations
- **Arrow Flight SQL**: Partial compatibility for standard operations

For full SQL functionality (DDL, DML, transactions), the DuckDB Airport extension is required.
