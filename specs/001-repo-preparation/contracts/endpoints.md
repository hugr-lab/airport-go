# Endpoint Discovery Contract

**Version**: 1.0.0 | **Date**: 2025-11-26
**Protocol**: Apache Arrow Flight RPC (GetFlightInfo, ListActions, DoAction)

## Overview

Endpoint discovery enables clients to discover available data sources, query metadata, and route queries to appropriate endpoints. This is particularly useful for distributed systems and query planning.

## RPC: GetFlightInfo

**Purpose**: Get metadata for a table or query

**Protocol**: Flight GetFlightInfo RPC

**Request**: FlightDescriptor
```go
descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorPATH,
    Path: []string{"schema_name", "table_name"},
}
info, err := client.GetFlightInfo(ctx, descriptor)
```

**Response**: FlightInfo
```go
type FlightInfo struct {
    Schema           *arrow.Schema    // Table Arrow schema
    FlightDescriptor *FlightDescriptor // Original request
    Endpoint         []*FlightEndpoint // Data source endpoints
    TotalRecords     int64            // Total rows (-1 if unknown)
    TotalBytes       int64            // Total bytes (-1 if unknown)
}

type FlightEndpoint struct {
    Ticket   *Ticket   // Ticket for DoGet to fetch data
    Location []*Location // URIs where data can be accessed
}
```

**Use Cases**:
- Query planning: Client examines schema before fetching data
- Distributed routing: Client selects endpoint based on location
- Metadata caching: Client caches schema for reuse

**Example**:
```go
descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorPATH,
    Path: []string{"analytics", "events"},
}

info, err := client.GetFlightInfo(ctx, descriptor)
if err != nil {
    return err
}

// Examine schema
fmt.Printf("Table has %d columns\n", len(info.Schema.Fields()))

// Fetch data from first endpoint
if len(info.Endpoint) > 0 {
    ticket := info.Endpoint[0].Ticket
    stream, err := client.DoGet(ctx, ticket)
    // ... read data
}
```

---

## RPC: ListActions

**Purpose**: List all supported DoAction types

**Protocol**: Flight ListActions RPC

**Request**: Empty criteria
```go
stream, err := client.ListActions(ctx, &flight.Empty{})
```

**Response**: Stream of ActionType
```go
type ActionType struct {
    Type        string // Action name (e.g., "CreateSchema")
    Description string // Human-readable description
}
```

**Use Cases**:
- Client capability discovery
- CLI command generation
- API documentation

**Example**:
```go
stream, err := client.ListActions(ctx, &flight.Empty{})
if err != nil {
    return err
}

fmt.Println("Supported Actions:")
for {
    actionType, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }
    fmt.Printf("  %s: %s\n", actionType.Type, actionType.Description)
}
```

**Expected Actions** (this feature):
```
CreateSchema: Create a new schema
DropSchema: Drop an existing schema
CreateTable: Create a new table with Arrow schema
DropTable: Drop an existing table
AlterTableAddColumn: Add a column to a table
AlterTableDropColumn: Drop a column from a table
Delete: Delete rows by rowid
GetFlightInfo: Get table metadata and endpoints
GetSchemas: List all schemas in catalog
GetTables: List all tables in a schema
```

---

## Action: GetSchemas

**Purpose**: List all schemas in the catalog

**Action Type**: `"GetSchemas"`

**Request Payload**:
```json
{
  "catalog_name": "string (optional, default: default catalog)"
}
```

**Response**:
```json
{
  "schemas": [
    {
      "schema_name": "string",
      "comment": "string (optional)"
    }
  ]
}
```

**Example**:
```go
action := &flight.Action{
    Type: "GetSchemas",
    Body: []byte(`{}`),
}
stream, err := client.DoAction(ctx, action)

for {
    result, err := stream.Recv()
    if err == io.EOF {
        break
    }
    // Parse result.Body as JSON
}
```

---

## Action: GetTables

**Purpose**: List all tables in a schema

**Action Type**: `"GetTables"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "table_name_pattern": "string (optional, glob pattern)"
}
```

**Response**:
```json
{
  "tables": [
    {
      "table_name": "string",
      "comment": "string (optional)",
      "column_count": "integer"
    }
  ]
}
```

**Example**:
```go
action := &flight.Action{
    Type: "GetTables",
    Body: []byte(`{"schema_name": "analytics", "table_name_pattern": "events_*"}`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Point-in-Time Query Parameters

**Purpose**: Query historical data state using time-travel parameters

**Protocol**: Passed via FlightDescriptor or Ticket

**Parameters**:
- `ts`: Unix timestamp in seconds (int64)
- `ts_ns`: Unix timestamp in nanoseconds (int64)

**Encoding**: JSON in FlightDescriptor.Cmd or Ticket
```json
{
  "schema_name": "string",
  "table_name": "string",
  "ts": "int64 (optional)",
  "ts_ns": "int64 (optional)"
}
```

**Validation**:
- At most one of `ts` or `ts_ns` can be set
- Timestamp must be non-negative
- Future timestamps return empty result set
- If both nil, query returns current data

**Example**:
```go
// Query data as of 2024-01-01 00:00:00 UTC
ts := int64(1704067200) // Unix timestamp for 2024-01-01

descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorCMD,
    Cmd:  []byte(fmt.Sprintf(`{"schema_name":"analytics","table_name":"events","ts":%d}`, ts)),
}

info, err := client.GetFlightInfo(ctx, descriptor)
// Data reflects state as of 2024-01-01
```

**Storage Requirements**:
- Backing storage must maintain historical versions
- Implementation-defined retention policy
- Queries before data existence return empty set

---

## Table Function Flight Info

**Purpose**: Get metadata for parameterized table functions

**Action Type**: `"GetTableFunctionFlightInfo"`

**Request Payload**:
```json
{
  "function_name": "string (required)",
  "parameters": {
    "key": "value (function-specific parameters)"
  }
}
```

**Response**: Same as GetFlightInfo (FlightInfo structure)

**Use Cases**:
- Parameterized queries (e.g., `generate_series(1, 100)`)
- Dynamic data sources (e.g., `read_csv('path')`)
- Custom table functions

**Example**:
```go
action := &flight.Action{
    Type: "GetTableFunctionFlightInfo",
    Body: []byte(`{
        "function_name": "generate_series",
        "parameters": {"start": 1, "stop": 100}
    }`),
}
stream, err := client.DoAction(ctx, action)

// Parse FlightInfo from result
for {
    result, err := stream.Recv()
    if err == io.EOF {
        break
    }
    // result.Body contains serialized FlightInfo
}
```

---

## Endpoint Location Format

**URI Format**: `grpc://host:port` or `grpc+tls://host:port`

**Examples**:
- `grpc://localhost:8815` - Local server, no TLS
- `grpc+tls://server.example.com:8815` - Remote server with TLS
- `grpc://10.0.1.5:8815` - IP address

**Multi-Endpoint Scenarios** (future):
- Partitioned data: Each endpoint serves a partition
- Replicated data: Endpoints serve same data (client chooses for locality)
- Federated queries: Endpoints span multiple physical servers

**Current Implementation** (this feature):
- Single endpoint: Server returns one endpoint pointing to itself
- No data partitioning: All data accessible from single endpoint
- Future-proof: Contract supports multiple endpoints for later expansion

---

## Error Handling

**Error Codes**:
- `NotFound`: Table/schema doesn't exist
- `InvalidArgument`: Malformed descriptor, invalid time parameters
- `Unimplemented`: Unsupported action type
- `Internal`: Server-side error

**Example**:
```go
info, err := client.GetFlightInfo(ctx, descriptor)
if err != nil {
    switch status.Code(err) {
    case codes.NotFound:
        fmt.Println("Table not found")
    case codes.InvalidArgument:
        fmt.Println("Invalid request")
    default:
        fmt.Printf("Error: %v\n", err)
    }
}
```

---

## Testing

Integration tests must cover:
1. **GetFlightInfo**: Valid table, non-existent table
2. **ListActions**: Verify all expected actions present
3. **GetSchemas/GetTables**: List operations
4. **Point-in-time**: Query with ts/ts_ns parameters
5. **Table Functions**: Parameterized queries

**Example Test**:
```go
func TestGetFlightInfo(t *testing.T) {
    server := startTestServer(t)
    defer server.Stop()

    client := connectFlightClient(t, server.Address())
    ctx := context.Background()

    // Create test table
    createTable(t, client, "test", "users")

    // Get FlightInfo
    descriptor := &flight.FlightDescriptor{
        Type: flight.DescriptorPATH,
        Path: []string{"test", "users"},
    }

    info, err := client.GetFlightInfo(ctx, descriptor)
    require.NoError(t, err)
    assert.NotNil(t, info.Schema)
    assert.Greater(t, len(info.Endpoint), 0)

    // Verify endpoint ticket works
    ticket := info.Endpoint[0].Ticket
    stream, err := client.DoGet(ctx, ticket)
    require.NoError(t, err)
    // ... verify data stream
}
```

## Performance Targets

- GetFlightInfo: <100ms
- ListActions: <50ms
- GetSchemas/GetTables: <500ms (depends on catalog size)
- Point-in-time queries: <10% overhead vs current queries

## Compatibility

This contract aligns with Apache Arrow Flight SQL specification and DuckDB Airport extension discovery patterns.

