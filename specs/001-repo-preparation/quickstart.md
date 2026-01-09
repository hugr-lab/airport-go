# Developer Quickstart: Repository Preparation Feature

**Feature**: 001-repo-preparation | **Date**: 2025-11-26
**Branch**: `001-repo-preparation`

## Overview

This feature prepares the airport-go repository for production use by:
1. Reorganizing repository structure (P1)
2. Adding CI/CD automation (P1)
3. Enhancing examples with DuckDB client code (P2)
4. Implementing DDL operations (CREATE/DROP SCHEMA/TABLE, ALTER TABLE) (P2)
5. Implementing DML operations (INSERT, UPDATE, DELETE) (P2)
6. Adding point-in-time query support (P3)
7. Implementing endpoint discovery (P3)

## Prerequisites

- Go 1.21+
- Git
- Basic understanding of Apache Arrow and Flight RPC
- (Optional) DuckDB 1.4+ for testing examples

## Getting Started

### 1. Checkout Feature Branch

```bash
git fetch origin
git checkout 001-repo-preparation
```

### 2. Review Documentation

Read these documents in order:
1. **spec.md**: Feature requirements and user stories
2. **plan.md** (this directory): Implementation approach
3. **research.md** (this directory): Technical decisions
4. **data-model.md** (this directory): Data structures
5. **contracts/** (this directory): API contracts

### 3. Understand Project Structure

```text
airport-go/
├── flight/           # Flight server implementation (FOCUS HERE)
│   ├── server.go     # Existing server
│   ├── ddl.go        # NEW: Add DDL handlers
│   ├── dml.go        # NEW: Add DML handlers
│   └── endpoints.go  # NEW: Add endpoint discovery
├── catalog/          # Catalog interfaces (READ ONLY - don't modify)
├── auth/             # Authentication (READ ONLY)
├── internal/serialize/ # Arrow IPC serialization (MAY EXTEND)
├── examples/         # Examples (ADD CLIENT CODE)
└── tests/integration/ # Integration tests (MOVE HERE)
```

### 4. Development Workflow

#### Phase 1: Repository Reorganization (P1)

**Task**: Move integration tests

```bash
# Create integration tests directory
mkdir -p tests/integration

# Move integration test files
git mv integration_*.go tests/integration/

# Update import paths in moved files
# (Change package from 'airport' to 'integration_test')

# Verify tests still pass
go test ./tests/integration/...
```

**Task**: Add GitHub Actions workflow

```bash
# Create GitHub Actions directory
mkdir -p .github/workflows

# Create ci.yml (see contracts/ddl-actions.md for template)
cat > .github/workflows/ci.yml <<EOF
name: CI
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
          cache: true
      - uses: golangci/golangci-lint-action@v4
        with:
          version: latest

  test:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
          cache: true
      - run: go test -v -race -timeout=10m ./...
EOF

# Test locally (requires act or push to GitHub)
git add .github/workflows/ci.yml
git commit -m "Add GitHub Actions CI workflow"
```

#### Phase 2: Example Enhancement (P2)

**Task**: Add DuckDB client examples

```bash
cd examples/basic

# Create client.sql
cat > client.sql <<'SQL'
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Create connection secret
CREATE SECRET airport_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:8815'
);

-- Query catalog
SELECT * FROM airport_catalog.default.example_table LIMIT 10;
SQL

# Update README.md with DuckDB instructions
cat >> README.md <<'MD'

## DuckDB Client Example

### Prerequisites
- DuckDB 1.4+
- Airport extension

### Installation
```bash
# Install DuckDB
brew install duckdb  # macOS
# or download from https://duckdb.org

# Install Airport extension (done automatically in client.sql)
```

### Running
1. Start server: `go run main.go`
2. In another terminal: `duckdb < client.sql`

### Expected Output
Query results will show table data in tabular format.
MD

# Repeat for examples/auth and examples/dynamic
```

#### Phase 3: DDL Operations (P2)

**Task**: Implement CREATE SCHEMA handler

```bash
cd flight

# Create ddl.go
cat > ddl.go <<'GO'
package flight

import (
    "context"
    "encoding/json"
    "io"

    "github.com/apache/arrow-go/v18/arrow/flight"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// CreateSchemaAction represents CREATE SCHEMA payload
type CreateSchemaAction struct {
    SchemaName  string `json:"schema_name"`
    IfNotExists bool   `json:"if_not_exists"`
    Comment     string `json:"comment,omitempty"`
}

// DoAction handles DDL operations
func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
    switch action.Type {
    case "CreateSchema":
        return s.handleCreateSchema(action.Body, stream)
    default:
        return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
    }
}

func (s *Server) handleCreateSchema(body []byte, stream flight.FlightService_DoActionServer) error {
    var action CreateSchemaAction
    if err := json.Unmarshal(body, &action); err != nil {
        return status.Errorf(codes.InvalidArgument, "invalid action body: %v", err)
    }

    ctx := stream.Context()

    // Check if schema exists
    existing, err := s.catalog.Schema(ctx, action.SchemaName)
    if err != nil {
        return status.Errorf(codes.Internal, "catalog error: %v", err)
    }

    if existing != nil {
        if action.IfNotExists {
            // Idempotent: return success
            return s.sendActionResult(stream, map[string]any{
                "status":      "success",
                "schema_name": action.SchemaName,
            })
        }
        return status.Errorf(codes.AlreadyExists, "schema '%s' already exists", action.SchemaName)
    }

    // Create schema (requires extending catalog interface)
    // This is a placeholder - actual implementation depends on catalog
    // TODO: Add CreateSchema method to catalog.Catalog interface

    return s.sendActionResult(stream, map[string]any{
        "status":      "success",
        "schema_name": action.SchemaName,
    })
}

func (s *Server) sendActionResult(stream flight.FlightService_DoActionServer, data any) error {
    jsonData, err := json.Marshal(data)
    if err != nil {
        return status.Errorf(codes.Internal, "failed to marshal result: %v", err)
    }
    return stream.Send(&flight.Result{Body: jsonData})
}
GO

# Write corresponding test
cat > ddl_test.go <<'GO'
package flight

import (
    "context"
    "testing"

    "github.com/apache/arrow-go/v18/arrow/flight"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

func TestCreateSchemaAction(t *testing.T) {
    server := startTestServer(t)
    defer server.Stop()

    client := connectClient(t, server.Address())
    ctx := context.Background()

    // Test: Create schema
    action := &flight.Action{
        Type: "CreateSchema",
        Body: []byte(`{"schema_name": "test_schema", "if_not_exists": false}`),
    }

    stream, err := client.DoAction(ctx, action)
    require.NoError(t, err)

    result, err := stream.Recv()
    require.NoError(t, err)
    assert.Contains(t, string(result.Body), "success")

    // Test: Duplicate without IF NOT EXISTS
    stream, err = client.DoAction(ctx, action)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))

    // Test: Duplicate with IF NOT EXISTS
    action.Body = []byte(`{"schema_name": "test_schema", "if_not_exists": true}`)
    stream, err = client.DoAction(ctx, action)
    require.NoError(t, err)

    result, err = stream.Recv()
    require.NoError(t, err)
    assert.Contains(t, string(result.Body), "success")
}
GO

# Run tests
go test -v ./flight/...
```

**Follow similar pattern for**:
- DROP SCHEMA
- CREATE TABLE
- DROP TABLE
- ALTER TABLE ADD COLUMN
- ALTER TABLE DROP COLUMN

#### Phase 4: DML Operations (P2)

**Task**: Implement INSERT via DoPut

```bash
# Add to flight/dml.go
# See contracts/dml-actions.md for full implementation
# Key points:
# 1. Parse FlightDescriptor.Cmd to extract schema/table
# 2. Read Arrow RecordBatch from stream
# 3. Validate schema compatibility
# 4. Insert into catalog
# 5. Return PutResult with row count
```

#### Phase 5: Testing

**Run all tests**:
```bash
# Unit tests
go test ./...

# Integration tests
go test ./tests/integration/...

# With race detector
go test -race ./...

# Coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

**Memory leak detection**:
```go
// In test files
import "github.com/apache/arrow-go/v18/arrow/memory"

func TestNoMemoryLeaks(t *testing.T) {
    allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
    defer allocator.AssertSize(t, 0)

    // Test code using allocator
}
```

## Common Tasks

### Adding a New DoAction Handler

1. Define action struct in `flight/ddl.go` or `flight/dml.go`
2. Add case to `DoAction` switch statement
3. Implement handler function
4. Write unit tests
5. Write integration tests
6. Update `ListActions` to include new action
7. Document in `contracts/` directory

### Debugging Flight RPC

```go
// Enable gRPC logging
import "google.golang.org/grpc/grpclog"

func init() {
    grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stdout, os.Stderr, os.Stderr))
}
```

### Running Examples

```bash
# Terminal 1: Start server
cd examples/basic
go run main.go

# Terminal 2: Run DuckDB client
duckdb < client.sql
```

## Troubleshooting

### Issue: Tests fail with "integration_*_test.go not found"

**Solution**: Integration tests moved to `tests/integration/`. Update test commands:
```bash
go test ./tests/integration/...
```

### Issue: golangci-lint fails in CI

**Solution**: Run linting locally and fix issues:
```bash
golangci-lint run ./...
```

### Issue: Race detector reports data race

**Solution**: Fix concurrent access. Common patterns:
- Use `sync.Mutex` for shared state
- Use channels for communication
- Avoid shared mutable state in tests

### Issue: Arrow memory leaks detected

**Solution**: Always call `Release()` on Arrow objects:
```go
builder := array.NewRecordBuilder(allocator, schema)
defer builder.Release()  // IMPORTANT!

record := builder.NewRecord()
defer record.Release()   // IMPORTANT!
```

## Next Steps

After implementing this feature:
1. Run `/speckit.tasks` to generate task breakdown
2. Implement tasks in priority order (P1 → P2 → P3)
3. Run tests frequently
4. Commit changes incrementally
5. Create PR when ready

## Resources

- [Apache Arrow Flight Go SDK](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow/flight)
- [DuckDB Airport Extension](https://airport.query.farm/)
- [Go testing](https://go.dev/doc/tutorial/add-a-test)
- [GitHub Actions for Go](https://github.com/actions/setup-go)

## Questions?

Refer to:
- `spec.md` for requirements
- `data-model.md` for data structures
- `contracts/*.md` for API contracts
- `research.md` for technical decisions

