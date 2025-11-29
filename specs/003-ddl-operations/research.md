# Research: DDL Operations

**Feature**: 003-ddl-operations
**Date**: 2025-11-29

## 1. Airport Extension Protocol - Action Names

### Decision
Use lowercase action names with underscores as defined in the Airport Extension specification.

### Rationale
The official Airport Extension documentation at https://airport.query.farm/server_actions.html uses lowercase with underscores:
- `create_schema` (not CreateSchema)
- `drop_schema` (not DropSchema)
- `create_table` (not CreateTable)
- `drop_table` (not DropTable)
- `add_column` (not AddColumn, AlterTableAddColumn)
- `remove_column` (not RemoveColumn, AlterTableDropColumn)

The existing code in `flight/doaction.go` uses PascalCase action names like "CreateSchema", "DropSchema" which appears to be placeholder code that should be updated to match the protocol.

### Alternatives Considered
1. **PascalCase (current code)**: Inconsistent with Airport protocol specification
2. **snake_case (protocol spec)**: Matches DuckDB Airport extension expectations - **Selected**

### Implementation Note
Update the switch statement in `flight/doaction.go` from:
```go
case "CreateSchema", "DropSchema", "CreateTable", "DropTable", "AlterTableAddColumn", "AlterTableDropColumn":
```
to:
```go
case "create_schema":
    return s.handleCreateSchema(ctx, action, stream)
case "drop_schema":
    return s.handleDropSchema(ctx, action, stream)
// etc.
```

---

## 2. Msgpack Request/Response Structures

### Decision
Follow the exact msgpack structures from the Airport protocol documentation.

### Rationale
The protocol defines specific structures for each DDL operation:

**create_schema request** (AirportCreateSchemaParameters):
```go
type CreateSchemaParams struct {
    CatalogName string            `msgpack:"catalog_name"`
    Schema      string            `msgpack:"schema"`
    Comment     *string           `msgpack:"comment,omitempty"`
    Tags        map[string]string `msgpack:"tags,omitempty"`
}
```

**drop_schema request** (DropItemActionParameters):
```go
type DropSchemaParams struct {
    Type           string `msgpack:"type"`           // Always "schema"
    CatalogName    string `msgpack:"catalog_name"`
    SchemaName     string `msgpack:"schema_name"`
    Name           string `msgpack:"name"`
    IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}
```

**create_table request** (AirportCreateTableParameters):
```go
type CreateTableParams struct {
    CatalogName        string   `msgpack:"catalog_name"`
    SchemaName         string   `msgpack:"schema_name"`
    TableName          string   `msgpack:"table_name"`
    ArrowSchema        string   `msgpack:"arrow_schema"`  // IPC serialized
    OnConflict         string   `msgpack:"on_conflict"`   // "error", "ignore", "replace"
    NotNullConstraints []uint64 `msgpack:"not_null_constraints"`
    UniqueConstraints  []uint64 `msgpack:"unique_constraints"`
    CheckConstraints   []string `msgpack:"check_constraints"`
}
```

**drop_table request** (DropItemActionParameters):
```go
type DropTableParams struct {
    Type           string `msgpack:"type"`           // Always "table"
    CatalogName    string `msgpack:"catalog_name"`
    SchemaName     string `msgpack:"schema_name"`
    Name           string `msgpack:"name"`
    IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}
```

**add_column request** (AirportAlterTableAddColumnParameters):
```go
type AddColumnParams struct {
    Catalog            string `msgpack:"catalog"`
    Schema             string `msgpack:"schema"`
    Name               string `msgpack:"name"`               // Table name
    ColumnSchema       string `msgpack:"column_schema"`      // IPC serialized Arrow schema
    IgnoreNotFound     bool   `msgpack:"ignore_not_found"`
    IfColumnNotExists  bool   `msgpack:"if_column_not_exists"`
}
```

**remove_column request** (AirportRemoveTableColumnParameters):
```go
type RemoveColumnParams struct {
    Catalog        string `msgpack:"catalog"`
    Schema         string `msgpack:"schema"`
    Name           string `msgpack:"name"`           // Table name
    RemovedColumn  string `msgpack:"removed_column"` // Column name to remove
    IgnoreNotFound bool   `msgpack:"ignore_not_found"`
    IfColumnExists bool   `msgpack:"if_column_exists"`
    Cascade        bool   `msgpack:"cascade"`
}
```

### Alternatives Considered
1. **Custom Go-style naming**: Would break protocol compatibility
2. **Exact protocol names**: Ensures interoperability - **Selected**

---

## 3. Response Formats

### Decision
- `create_schema`: Return AirportSerializedContentsWithSHA256Hash (same as list_schemas)
- `drop_schema`, `drop_table`: Response is ignored; raise exception on error
- `create_table`, `add_column`, `remove_column`: Return FlightInfo

### Rationale
From the protocol documentation:
- Create operations return structured data for client to use
- Drop operations return nothing (errors are exceptions)
- Table/column modifications return FlightInfo with updated schema

### Implementation
```go
// create_schema response
type CreateSchemaResponse struct {
    SHA256     string `msgpack:"sha256"`
    URL        *string `msgpack:"url"`
    Serialized []byte `msgpack:"serialized"`
}

// create_table response: FlightInfo protobuf
// add_column response: FlightInfo protobuf
// remove_column response: FlightInfo protobuf
// drop_schema response: empty (errors raised as gRPC status)
// drop_table response: empty (errors raised as gRPC status)
```

---

## 4. Arrow Schema Serialization

### Decision
Use `flight.SerializeSchema()` for Arrow IPC wire format.

### Rationale
The existing codebase already uses this pattern in `serializeSchemaContents()` for table discovery. For DDL operations:
- `create_table`: Client sends `arrow_schema` as IPC bytes, server deserializes with `flight.DeserializeSchema()`
- `add_column`: Client sends `column_schema` as IPC bytes containing single-field schema
- Response FlightInfo uses `flight.SerializeSchema()` for consistency

### Implementation
```go
// Deserialize incoming Arrow schema from create_table
schema, err := flight.DeserializeSchema([]byte(params.ArrowSchema), memory.DefaultAllocator)
if err != nil {
    return status.Errorf(codes.InvalidArgument, "invalid arrow_schema: %v", err)
}
```

---

## 5. DuckDB Integration Test Pattern

### Decision
Follow the established DML test pattern using DuckDB as a Flight client.

### Rationale
From `tests/integration/dml_test.go`, the pattern is:
1. Create mock catalog implementing required interfaces
2. Start test server with `newTestServer(t, cat, nil)`
3. Open DuckDB connection with `openDuckDB(t)`
4. Attach to Flight server with `connectToFlightServer(t, db, server.address, "")`
5. Execute SQL DDL statements via DuckDB
6. Verify changes via mock catalog methods

### Example Test Structure
```go
func TestDDLCreateSchema(t *testing.T) {
    mockCat := newMockDynamicCatalog()
    server := newTestServer(t, mockCat, nil)
    defer server.stop()

    db := openDuckDB(t)
    defer db.Close()

    attachName := connectToFlightServer(t, db, server.address, "")

    // Execute DDL
    _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.new_schema", attachName))
    if err != nil {
        t.Fatalf("CREATE SCHEMA failed: %v", err)
    }

    // Verify via mock
    if !mockCat.HasSchema("new_schema") {
        t.Error("schema was not created")
    }
}
```

---

## 6. Error Handling Strategy

### Decision
Map DDL errors to gRPC status codes consistently.

### Rationale
| Error Condition | gRPC Code | Example |
|-----------------|-----------|---------|
| Missing required parameter | `InvalidArgument` | Empty schema name |
| Invalid Arrow schema format | `InvalidArgument` | Malformed IPC bytes |
| Schema/table already exists | `AlreadyExists` | on_conflict="error" |
| Schema/table not found | `NotFound` | ignore_not_found=false |
| Operation not supported | `Unimplemented` | Non-dynamic catalog |
| Schema has tables on drop | `FailedPrecondition` | FR-016 |
| Internal error | `Internal` | Unexpected panic |

### Implementation
```go
func (s *Server) handleCreateSchema(...) error {
    // Check if catalog supports dynamic operations
    dynCat, ok := s.catalog.(catalog.DynamicCatalog)
    if !ok {
        return status.Error(codes.Unimplemented, "catalog does not support schema creation")
    }

    // Validate parameters
    if params.Schema == "" {
        return status.Error(codes.InvalidArgument, "schema name is required")
    }

    // Call dynamic catalog
    schema, err := dynCat.CreateSchema(ctx, params.Schema, opts)
    if errors.Is(err, catalog.ErrAlreadyExists) {
        return status.Error(codes.AlreadyExists, err.Error())
    }
    if err != nil {
        return status.Errorf(codes.Internal, "failed to create schema: %v", err)
    }
    // ...
}
```

---

## 7. Thread Safety

### Decision
All dynamic interfaces must be goroutine-safe; mutex usage follows existing patterns.

### Rationale
The constitution requires thread safety (FR-014). The mock implementation pattern from `transaction_test.go` shows:
```go
type mockDynamicCatalog struct {
    mu      sync.RWMutex
    schemas map[string]*mockDynamicSchema
}

func (c *mockDynamicCatalog) CreateSchema(ctx context.Context, name string, opts CreateSchemaOptions) (Schema, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    // ... create schema
}

func (c *mockDynamicCatalog) Schemas(ctx context.Context) ([]Schema, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    // ... return schemas
}
```

---

## Summary

All NEEDS CLARIFICATION items resolved:

| Item | Resolution |
|------|------------|
| Action name format | snake_case per protocol spec |
| Msgpack structures | Match AirportXxxParameters from docs |
| Response formats | create_schema: hash struct; create_table: FlightInfo; drop_*: empty |
| Arrow serialization | Use flight.SerializeSchema/DeserializeSchema |
| Test pattern | DuckDB client via openDuckDB + connectToFlightServer |
| Error handling | Map to gRPC status codes |
| Thread safety | RWMutex pattern from existing code |
