# DDL Actions Contract

**Version**: 1.0.0 | **Date**: 2025-11-26
**Protocol**: Apache Arrow Flight RPC DoAction

## Overview

DDL (Data Definition Language) operations are implemented as Flight DoAction RPC calls. Each action type has a specific JSON payload format and returns a result stream.

## Action: CreateSchema

**Purpose**: Create a new schema in the catalog

**Action Type**: `"CreateSchema"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "if_not_exists": "boolean (optional, default: false)",
  "comment": "string (optional)"
}
```

**Response**:
```json
{
  "status": "success",
  "schema_name": "string"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON or missing required fields
- `AlreadyExists`: Schema exists and `if_not_exists` is false
- `Internal`: Server-side error

**Example**:
```go
action := &flight.Action{
    Type: "CreateSchema",
    Body: []byte(`{
        "schema_name": "analytics",
        "if_not_exists": true,
        "comment": "Analytics data schema"
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Action: DropSchema

**Purpose**: Drop an existing schema from the catalog

**Action Type**: `"DropSchema"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "if_exists": "boolean (optional, default: false)",
  "cascade": "boolean (optional, default: false)"
}
```

**Response**:
```json
{
  "status": "success",
  "schema_name": "string",
  "tables_dropped": "integer (if cascade=true)"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON or missing required fields
- `NotFound`: Schema doesn't exist and `if_exists` is false
- `FailedPrecondition`: Schema has tables and `cascade` is false
- `Internal`: Server-side error

**Example**:
```go
action := &flight.Action{
    Type: "DropSchema",
    Body: []byte(`{
        "schema_name": "analytics",
        "if_exists": true,
        "cascade": false
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Action: CreateTable

**Purpose**: Create a new table in a schema

**Action Type**: `"CreateTable"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "table_name": "string (required)",
  "if_not_exists": "boolean (optional, default: false)",
  "schema": {
    "fields": [
      {
        "name": "string (required)",
        "type": "string (required, Arrow type)",
        "nullable": "boolean (optional, default: true)"
      }
    ],
    "metadata": {
      "key": "value (optional)"
    }
  },
  "comment": "string (optional)"
}
```

**Arrow Type Strings**:
- Numeric: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float64`
- String: `utf8`, `large_utf8`, `binary`, `large_binary`
- Temporal: `date32`, `date64`, `timestamp`, `time32`, `time64`, `duration`, `interval`
- Boolean: `bool`
- Nested: `list`, `struct`, `map`, `union`, `dictionary`
- Geospatial: `extension<airport.geometry>` (WKB-encoded, uses github.com/paulmach/orb)

**Response**:
```json
{
  "status": "success",
  "schema_name": "string",
  "table_name": "string",
  "columns": "integer (field count)"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON, missing fields, or invalid Arrow type
- `NotFound`: Schema doesn't exist
- `AlreadyExists`: Table exists and `if_not_exists` is false
- `Internal`: Server-side error

**Example**:
```go
action := &flight.Action{
    Type: "CreateTable",
    Body: []byte(`{
        "schema_name": "analytics",
        "table_name": "events",
        "if_not_exists": true,
        "schema": {
            "fields": [
                {"name": "id", "type": "int64", "nullable": false},
                {"name": "timestamp", "type": "timestamp", "nullable": false},
                {"name": "event_type", "type": "utf8", "nullable": false},
                {"name": "user_id", "type": "int64", "nullable": true},
                {"name": "properties", "type": "utf8", "nullable": true}
            ]
        },
        "comment": "Event tracking table"
    }`),
}
stream, err := client.DoAction(ctx, action)
```

**Geometry Column Example**:
```go
action := &flight.Action{
    Type: "CreateTable",
    Body: []byte(`{
        "schema_name": "gis",
        "table_name": "places",
        "schema": {
            "fields": [
                {"name": "id", "type": "int64", "nullable": false},
                {"name": "name", "type": "utf8", "nullable": false},
                {"name": "location", "type": "extension<airport.geometry>", "nullable": true},
                {"name": "boundary", "type": "extension<airport.geometry>", "nullable": true}
            ],
            "metadata": {
                "location.srid": "4326",
                "location.geometry_type": "POINT",
                "boundary.srid": "4326",
                "boundary.geometry_type": "POLYGON"
            }
        },
        "comment": "Places with geospatial data"
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Action: DropTable

**Purpose**: Drop an existing table from a schema

**Action Type**: `"DropTable"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "table_name": "string (required)",
  "if_exists": "boolean (optional, default: false)"
}
```

**Response**:
```json
{
  "status": "success",
  "schema_name": "string",
  "table_name": "string"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON or missing required fields
- `NotFound`: Table doesn't exist and `if_exists` is false
- `Internal`: Server-side error

**Example**:
```go
action := &flight.Action{
    Type: "DropTable",
    Body: []byte(`{
        "schema_name": "analytics",
        "table_name": "events",
        "if_exists": true
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Action: AlterTableAddColumn

**Purpose**: Add a column to an existing table

**Action Type**: `"AlterTableAddColumn"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "table_name": "string (required)",
  "if_exists": "boolean (optional, default: false)",
  "column": {
    "name": "string (required)",
    "type": "string (required, Arrow type)",
    "nullable": "boolean (optional, default: true)"
  }
}
```

**Response**:
```json
{
  "status": "success",
  "schema_name": "string",
  "table_name": "string",
  "column_name": "string"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON, missing fields, or invalid Arrow type
- `NotFound`: Table doesn't exist and `if_exists` is false
- `AlreadyExists`: Column with same name already exists
- `Internal`: Server-side error

**Example**:
```go
action := &flight.Action{
    Type: "AlterTableAddColumn",
    Body: []byte(`{
        "schema_name": "analytics",
        "table_name": "events",
        "if_exists": true,
        "column": {
            "name": "session_id",
            "type": "utf8",
            "nullable": true
        }
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Action: AlterTableDropColumn

**Purpose**: Drop a column from an existing table

**Action Type**: `"AlterTableDropColumn"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "table_name": "string (required)",
  "if_exists": "boolean (optional, default: false)",
  "column_name": "string (required)"
}
```

**Response**:
```json
{
  "status": "success",
  "schema_name": "string",
  "table_name": "string",
  "column_name": "string"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON or missing required fields
- `NotFound`: Table or column doesn't exist and `if_exists` is false
- `FailedPrecondition`: Attempting to drop last column in table
- `Internal`: Server-side error

**Example**:
```go
action := &flight.Action{
    Type: "AlterTableDropColumn",
    Body: []byte(`{
        "schema_name": "analytics",
        "table_name": "events",
        "if_exists": true,
        "column_name": "session_id"
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Common Response Format

All DDL actions return a stream of Result messages:

```go
type Result struct {
    Body []byte // JSON-encoded response
}
```

The response body follows this general structure:
```json
{
  "status": "success" | "error",
  "message": "string (optional, for errors)",
  "error_code": "string (optional, for errors)",
  ...additional fields specific to action...
}
```

## Error Handling

Clients should handle these error scenarios:

1. **Network Errors**: Connection failures, timeouts
2. **Protocol Errors**: Invalid Action type, malformed payloads
3. **Business Logic Errors**: Constraints violations, missing resources
4. **Server Errors**: Internal failures, storage errors

**Example Error Handler**:
```go
stream, err := client.DoAction(ctx, action)
if err != nil {
    if status.Code(err) == codes.AlreadyExists {
        // Handle duplicate resource
    } else if status.Code(err) == codes.NotFound {
        // Handle missing resource
    } else {
        // Handle other errors
    }
    return err
}

for {
    result, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return fmt.Errorf("stream error: %w", err)
    }
    // Process result.Body
}
```

## Testing

Each DDL action must have:
1. **Unit tests**: Payload validation, error cases
2. **Integration tests**: End-to-end action execution against real Flight server
3. **Idempotency tests**: Verify IF EXISTS/IF NOT EXISTS behavior

**Example Integration Test**:
```go
func TestCreateSchemaAction(t *testing.T) {
    server := startTestServer(t)
    defer server.Stop()

    client := connectFlightClient(t, server.Address())
    ctx := context.Background()

    // Test: Create schema
    action := &flight.Action{
        Type: "CreateSchema",
        Body: []byte(`{"schema_name": "test", "if_not_exists": false}`),
    }
    stream, err := client.DoAction(ctx, action)
    require.NoError(t, err)
    assertActionSuccess(t, stream)

    // Test: Duplicate without IF NOT EXISTS
    stream, err = client.DoAction(ctx, action)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))

    // Test: Duplicate with IF NOT EXISTS
    action.Body = []byte(`{"schema_name": "test", "if_not_exists": true}`)
    stream, err = client.DoAction(ctx, action)
    require.NoError(t, err)
    assertActionSuccess(t, stream) // Should succeed silently
}
```

## Compatibility

This contract aligns with DuckDB Airport extension v1.0+ action formats. Clients using DuckDB 1.4+ can generate these payloads automatically via SQL DDL statements.

