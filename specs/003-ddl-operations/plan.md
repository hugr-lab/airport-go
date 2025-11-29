# Implementation Plan: DDL Operations

**Branch**: `003-ddl-operations` | **Date**: 2025-11-29 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/003-ddl-operations/spec.md`

## Summary

Implement DDL (Data Definition Language) operations for the Airport Flight server, enabling dynamic schema and table management. This includes:
- Schema operations: `create_schema`, `drop_schema`
- Table operations: `create_table`, `drop_table`
- Column operations: `add_column`, `remove_column`

The implementation extends the existing catalog interfaces with dynamic variants (`DynamicCatalog`, `DynamicSchema`, `DynamicTable`) that allow runtime modifications while maintaining backward compatibility with static catalogs built via `NewCatalogBuilder()`.

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**:
- `github.com/apache/arrow-go/v18` (Arrow schema serialization)
- `github.com/vmihailenco/msgpack/v5` (protocol encoding via internal/msgpack)
- `google.golang.org/grpc` (Flight RPC transport)
**Storage**: N/A (storage-agnostic; delegated to user implementations)
**Testing**: `go test` with race detector (`-race`)
**Target Platform**: Linux/macOS server, any gRPC client
**Project Type**: Single library package
**Performance Goals**: DDL operations should complete in <100ms for typical operations
**Constraints**: Thread-safe; no silent failures; idiomatic Go
**Scale/Scope**: Supports catalogs with 100s of schemas and 1000s of tables

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Evidence |
|-----------|--------|----------|
| I. Code Quality | PASS | Follows existing patterns in `catalog/` and `flight/doaction.go`; all interfaces documented |
| II. Testing Standards | PASS | Integration tests using DuckDB as client; unit tests for interface validation |
| III. User Experience Consistency | PASS | New interfaces extend existing Catalog/Schema/Table; no breaking changes |
| IV. Performance Requirements | PASS | DDL operations are pass-through to user implementations; no unnecessary allocations |

**Gate Status**: All gates pass. Proceeding to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/003-ddl-operations/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
# Existing structure - additions marked with (+)
catalog/
├── catalog.go           # Catalog, Schema interfaces
├── table.go             # Table interface
├── dynamic.go           # (+) DynamicCatalog, DynamicSchema, DynamicTable interfaces
├── types.go             # ScanOptions, DMLResult, etc.
├── transaction.go       # TransactionManager interface
└── static.go            # Static catalog implementation

flight/
├── doaction.go          # DoAction handler (modify DDL case)
├── doaction_ddl.go      # (+) DDL action handlers (create_schema, drop_schema, etc.)
├── doaction_metadata.go # endpoints, list_schemas handlers
└── ...

tests/
├── integration/
│   ├── ddl_test.go      # (modify) DDL integration tests via DuckDB
│   ├── dynamic_test.go  # (+) DynamicCatalog mock implementation tests
│   └── ...
└── unit/
    └── catalog/
        └── dynamic_test.go  # (+) Unit tests for dynamic interfaces
```

**Structure Decision**: Single library package with modular subpackages. DDL action handlers go in new `flight/doaction_ddl.go` to maintain separation of concerns. Dynamic interfaces defined in new `catalog/dynamic.go`.

## Complexity Tracking

> No constitution violations requiring justification.

N/A - Design follows existing patterns without additional complexity.

---

## Phase 0: Research

### Research Tasks

1. **Airport Extension Protocol**: Verify action name conventions and msgpack encoding formats
2. **Arrow Schema Serialization**: Determine IPC wire format for `create_table` and `add_column`
3. **Existing Test Patterns**: Document DuckDB client integration test approach
4. **Error Handling**: Determine gRPC status codes for DDL errors

### Findings

#### 1. Airport Extension Protocol (from spec.md and reference docs)

**Action Names** (case-sensitive, per https://airport.query.farm/server_actions.html):
- `create_schema` - Creates a new schema
- `drop_schema` - Deletes an existing schema
- `create_table` - Creates a new table
- `drop_table` - Deletes an existing table
- `add_column` - Adds a column to a table
- `remove_column` - Removes a column from a table

**Request/Response Encoding**: All use msgpack for request parameters and responses.

#### 2. Arrow Schema Serialization

For `create_table` and `add_column`, the Arrow schema is serialized using Arrow IPC wire format via `flight.SerializeSchema()`. This is consistent with existing table discovery in `serializeSchemaContents()`.

#### 3. DuckDB Client Integration Test Pattern

From `tests/integration/dml_test.go` and `integration_test.go`:

```go
// Pattern for DuckDB-based DDL tests:
// 1. Create catalog with mock DynamicCatalog implementation
// 2. Start test server: server := newTestServer(t, cat, nil)
// 3. Open DuckDB: db := openDuckDB(t)
// 4. Attach Flight server: attachName := connectToFlightServer(t, db, server.address, "")
// 5. Execute SQL DDL: db.Exec(fmt.Sprintf("CREATE SCHEMA %s.new_schema", attachName))
// 6. Verify via SQL: db.QueryRow(fmt.Sprintf("SELECT * FROM %s.information_schema.schemata", attachName))
```

Key helper functions:
- `newTestServer(t, cat, auth)` - Creates gRPC server with Flight service
- `openDuckDB(t)` - Opens DuckDB with Airport extension loaded
- `connectToFlightServer(t, db, address, token)` - Attaches DuckDB to Flight server

#### 4. Error Handling

gRPC status codes for DDL operations:
- `codes.InvalidArgument` - Invalid parameters (missing required fields, invalid schema)
- `codes.AlreadyExists` - Schema/table already exists (when on_conflict="error")
- `codes.NotFound` - Schema/table not found (when ignore_not_found=false)
- `codes.Unimplemented` - Operation not supported (non-dynamic catalog/schema/table)
- `codes.Internal` - Internal server error

---

## Phase 1: Design

### Data Model

See [data-model.md](./data-model.md) for entity definitions.

### API Contracts

See [contracts/](./contracts/) directory for msgpack request/response schemas.

### Integration Test Strategy

Integration tests will use DuckDB as a Flight client to execute SQL DDL statements against the Airport server. This matches the established pattern in `dml_test.go`:

```go
// Example: TestDDLCreateSchemaViaDuckDB
func TestDDLCreateSchemaViaDuckDB(t *testing.T) {
    // 1. Create mock dynamic catalog
    mockCat := newMockDynamicCatalog()

    // 2. Start test server
    server := newTestServer(t, mockCat, nil)
    defer server.stop()

    // 3. Connect DuckDB
    db := openDuckDB(t)
    defer db.Close()
    attachName := connectToFlightServer(t, db, server.address, "")

    // 4. Execute DDL via SQL
    _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.new_schema", attachName))
    if err != nil {
        t.Fatalf("CREATE SCHEMA failed: %v", err)
    }

    // 5. Verify schema was created
    schemas, _ := mockCat.Schemas(context.Background())
    found := false
    for _, s := range schemas {
        if s.Name() == "new_schema" {
            found = true
            break
        }
    }
    if !found {
        t.Error("schema was not created")
    }
}
```

### Mock Dynamic Catalog

A mock implementation of `DynamicCatalog`, `DynamicSchema`, and `DynamicTable` will be created for testing:

```go
// mockDynamicCatalog implements catalog.DynamicCatalog for testing
type mockDynamicCatalog struct {
    mu      sync.RWMutex
    schemas map[string]*mockDynamicSchema
}

func (c *mockDynamicCatalog) CreateSchema(ctx context.Context, name string, opts CreateSchemaOptions) (Schema, error)
func (c *mockDynamicCatalog) DropSchema(ctx context.Context, name string, opts DropSchemaOptions) error
```

This follows the same pattern as `mockTransactionManager` in `transaction_test.go`.

---

## Implementation Phases

### Phase 1: Dynamic Interfaces (P1)

1. Define `DynamicCatalog` interface in `catalog/dynamic.go`
2. Define `DynamicSchema` interface in `catalog/dynamic.go`
3. Define `DynamicTable` interface in `catalog/dynamic.go`
4. Define option structs for each operation (CreateSchemaOptions, DropSchemaOptions, etc.)

### Phase 2: DDL Action Handlers (P1)

1. Create `flight/doaction_ddl.go` with stub implementations
2. Implement `handleCreateSchema` - decode msgpack, call DynamicCatalog.CreateSchema
3. Implement `handleDropSchema` - decode msgpack, call DynamicCatalog.DropSchema
4. Update `flight/doaction.go` to route to new handlers with correct action names

### Phase 3: Table Operations (P1)

1. Implement `handleCreateTable` - decode msgpack, deserialize Arrow schema, call DynamicSchema.CreateTable
2. Implement `handleDropTable` - decode msgpack, call DynamicSchema.DropTable

### Phase 4: Column Operations (P3)

1. Implement `handleAddColumn` - decode msgpack, deserialize column schema, call DynamicTable.AddColumn
2. Implement `handleRemoveColumn` - decode msgpack, call DynamicTable.RemoveColumn

### Phase 5: Integration Tests

1. Create mock `DynamicCatalog` implementation for testing
2. Update `tests/integration/ddl_test.go` with DuckDB-based tests
3. Add unit tests for interface validation
4. Run tests with race detector

### Phase 6: Documentation & Examples

1. Add godoc comments to all new interfaces and types
2. Create example code demonstrating dynamic catalog usage
3. Update CLAUDE.md with new feature summary

---

## Dependencies

- **D-001**: Existing `catalog.Catalog`, `catalog.Schema`, `catalog.Table` interfaces
- **D-002**: Existing `flight.DoAction` infrastructure
- **D-003**: `internal/msgpack` for encoding/decoding
- **D-004**: `flight.SerializeSchema` for Arrow IPC format

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| DuckDB Airport extension doesn't support DDL SQL | High | Use direct Flight client (gRPC) for tests if needed; existing ddl_test.go shows Flight client approach |
| Action names don't match DuckDB expectations | High | Reference https://airport.query.farm/ and verify with integration tests early |
| Arrow schema serialization format mismatch | Medium | Use existing `flight.SerializeSchema()` pattern from `serializeSchemaContents()` |
