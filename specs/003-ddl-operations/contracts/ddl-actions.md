# DDL Action Contracts

**Feature**: 003-ddl-operations
**Protocol**: Arrow Flight RPC DoAction
**Encoding**: MessagePack

---

## create_schema

Creates a new schema in the catalog.

### Request

**Action Type**: `create_schema`

```msgpack
{
    "catalog_name": string,       // Target catalog (empty for default)
    "schema": string,             // Schema name to create (required)
    "comment": string | null,     // Optional description
    "tags": {string: string}      // Optional metadata tags
}
```

### Response

**Success**: Single `flight.Result` with msgpack-encoded `AirportSerializedContentsWithSHA256Hash`

```msgpack
{
    "sha256": string,      // SHA256 hash of serialized contents
    "url": string | null,  // Optional URL for contents
    "serialized": bytes    // Serialized schema contents
}
```

**Errors**:
- `ALREADY_EXISTS` - Schema already exists
- `UNIMPLEMENTED` - Catalog does not support dynamic operations
- `INVALID_ARGUMENT` - Empty schema name

---

## drop_schema

Deletes an existing schema from the catalog.

### Request

**Action Type**: `drop_schema`

```msgpack
{
    "type": "schema",             // Always "schema"
    "catalog_name": string,       // Target catalog
    "schema_name": string,        // Schema name
    "name": string,               // Schema identifier
    "ignore_not_found": bool      // If true, don't error on missing schema
}
```

### Response

**Success**: Empty response (no `flight.Result`)

**Errors**:
- `NOT_FOUND` - Schema doesn't exist (when ignore_not_found=false)
- `FAILED_PRECONDITION` - Schema contains tables
- `UNIMPLEMENTED` - Catalog does not support dynamic operations

---

## create_table

Creates a new table in a schema.

### Request

**Action Type**: `create_table`

```msgpack
{
    "catalog_name": string,           // Target catalog
    "schema_name": string,            // Target schema (required)
    "table_name": string,             // Table name to create (required)
    "arrow_schema": bytes,            // Arrow IPC serialized schema
    "on_conflict": string,            // "error" | "ignore" | "replace"
    "not_null_constraints": [uint64], // Column indices with NOT NULL
    "unique_constraints": [uint64],   // Column indices with UNIQUE
    "check_constraints": [string]     // CHECK constraint expressions
}
```

### Response

**Success**: Single `flight.Result` with protobuf-encoded `FlightInfo`

The FlightInfo contains:
- `schema`: Serialized Arrow schema of the created table
- `flight_descriptor`: Path descriptor `[schema_name, table_name]`
- `endpoint`: Ticket for DoGet operations
- `app_metadata`: Msgpack-encoded table metadata

**Errors**:
- `ALREADY_EXISTS` - Table already exists (when on_conflict="error")
- `NOT_FOUND` - Schema doesn't exist
- `INVALID_ARGUMENT` - Invalid Arrow schema or empty table name
- `UNIMPLEMENTED` - Schema does not support dynamic operations

---

## drop_table

Deletes an existing table from a schema.

### Request

**Action Type**: `drop_table`

```msgpack
{
    "type": "table",              // Always "table"
    "catalog_name": string,       // Target catalog
    "schema_name": string,        // Schema containing the table
    "name": string,               // Table name to drop
    "ignore_not_found": bool      // If true, don't error on missing table
}
```

### Response

**Success**: Empty response (no `flight.Result`)

**Errors**:
- `NOT_FOUND` - Table doesn't exist (when ignore_not_found=false)
- `UNIMPLEMENTED` - Schema does not support dynamic operations

---

## add_column

Adds a column to an existing table.

### Request

**Action Type**: `add_column`

```msgpack
{
    "catalog": string,              // Target catalog
    "schema": string,               // Schema containing the table
    "name": string,                 // Table name
    "column_schema": bytes,         // Arrow IPC schema with single field
    "ignore_not_found": bool,       // If true, don't error on missing table
    "if_column_not_exists": bool    // If true, don't error if column exists
}
```

### Response

**Success**: Single `flight.Result` with protobuf-encoded `FlightInfo`

The FlightInfo contains the updated table schema.

**Errors**:
- `ALREADY_EXISTS` - Column already exists (when if_column_not_exists=false)
- `NOT_FOUND` - Table doesn't exist (when ignore_not_found=false)
- `INVALID_ARGUMENT` - Column schema doesn't contain exactly one field
- `UNIMPLEMENTED` - Table does not support dynamic operations

---

## remove_column

Removes a column from an existing table.

### Request

**Action Type**: `remove_column`

```msgpack
{
    "catalog": string,           // Target catalog
    "schema": string,            // Schema containing the table
    "name": string,              // Table name
    "removed_column": string,    // Column name to remove
    "ignore_not_found": bool,    // If true, don't error on missing table
    "if_column_exists": bool,    // If true, don't error if column missing
    "cascade": bool              // If true, remove dependent objects
}
```

### Response

**Success**: Single `flight.Result` with protobuf-encoded `FlightInfo`

The FlightInfo contains the updated table schema.

**Errors**:
- `NOT_FOUND` - Table or column doesn't exist (when if_*=false)
- `UNIMPLEMENTED` - Table does not support dynamic operations

---

## Error Response Format

All errors are returned as gRPC status codes with descriptive messages:

```go
// Example error handling
if schema == nil {
    return status.Errorf(codes.NotFound, "schema %q not found", schemaName)
}

if _, ok := s.catalog.(DynamicCatalog); !ok {
    return status.Error(codes.Unimplemented, "catalog does not support DDL operations")
}
```

| Condition | gRPC Code | HTTP Equivalent |
|-----------|-----------|-----------------|
| Missing required field | `INVALID_ARGUMENT` | 400 |
| Object already exists | `ALREADY_EXISTS` | 409 |
| Object not found | `NOT_FOUND` | 404 |
| Operation not supported | `UNIMPLEMENTED` | 501 |
| Schema has tables | `FAILED_PRECONDITION` | 412 |
| Internal error | `INTERNAL` | 500 |
