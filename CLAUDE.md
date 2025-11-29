# airport-go Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-11-29

## Active Technologies
- Go 1.25+ (recommended for latest stdlib features and performance)
- Storage-agnostic via catalog.Catalog interface
- Go 1.25+ + Arrow-Go v18, gRPC, msgpack-go, ZStandard (004-column-statistics)
- N/A (storage-agnostic; delegated to user implementations) (004-column-statistics)

## Project Structure

```text
# Repository root contains airport package (github.com/hugr-lab/airport-go)
*.go                     # Root package files (server.go, config.go, etc.)
catalog/                 # Subpackage: catalog interfaces and builder
flight/                  # Subpackage: Flight RPC handler implementations
auth/                    # Subpackage: authentication implementations
internal/                # Internal utilities (not public API)
examples/                # Example usage code
testutil/                # Test utilities for integration tests

tests/
├── unit/                # Unit tests (co-located with source: *_test.go)
├── integration/         # Integration tests with DuckDB
└── benchmarks/          # Performance benchmarks
```

## Commands

```bash
# Run all tests
go test ./...

# Run with race detector
go test -race ./...

# Run linter
golangci-lint run ./...

# Run integration tests only
go test ./tests/integration/...
```

## Code Style

- Follow idiomatic Go style (gofmt, golangci-lint)
- All public APIs must have godoc comments
- No silent failures - errors must be handled explicitly

## Recent Changes
- 004-column-statistics: Added Go 1.25+ + Arrow-Go v18, gRPC, msgpack-go, ZStandard
- 003-ddl-operations: DDL operations (CREATE/DROP SCHEMA, CREATE/DROP TABLE, ADD/REMOVE COLUMN), DynamicCatalog/Schema/Table interfaces, CTAS support
- 002-dml-transactions: DML operations (INSERT/UPDATE/DELETE), transaction management, column projection

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
