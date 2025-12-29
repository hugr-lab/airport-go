# airport-go Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-11-29

## Active Technologies
- Go 1.25+ (recommended for latest stdlib features and performance)
- Storage-agnostic via catalog.Catalog interface
- Go 1.25+ + Arrow-Go v18, gRPC, msgpack-go, ZStandard (004-column-statistics)
- N/A (storage-agnostic; delegated to user implementations) (004-column-statistics)
- N/A (library, storage-agnostic) (005-module-reorganization)
- Go 1.25+ + Apache Arrow Go v18, gRPC, msgpack-go (001-batch-table-interfaces)
- N/A (storage-agnostic library) (001-batch-table-interfaces)

## Project Structure

The repository uses Go workspaces with three modules:

```text
# Main library module (github.com/hugr-lab/airport-go)
go.mod                   # Main library (no DuckDB dependency)
go.work                  # Workspace configuration
*.go                     # Root package files (server.go, config.go, etc.)
catalog/                 # Subpackage: catalog interfaces and builder
flight/                  # Subpackage: Flight RPC handler implementations
auth/                    # Subpackage: authentication implementations
internal/                # Internal utilities (not public API)
docs/                    # Protocol and API documentation

# Examples module (github.com/hugr-lab/airport-go/examples)
examples/
├── go.mod               # Examples module
├── basic/               # Basic server example
├── auth/                # Authenticated server example
├── ddl/                 # DDL operations example
├── dml/                 # DML operations example
└── dynamic/             # Dynamic catalog example

# Tests module (github.com/hugr-lab/airport-go/tests)
tests/
├── go.mod               # Tests module (with DuckDB)
├── integration/         # Integration tests with DuckDB
└── benchmarks/          # Performance benchmarks
```

## Commands

```bash
# Run unit tests (main module only)
go test ./...

# Run with race detector
go test -race ./...

# Run linter
golangci-lint run ./...

# Run integration tests (requires DuckDB with Airport extension)
cd tests && go test ./integration/...

# Run benchmarks
cd tests && go test -bench=. ./benchmarks/...

# Build examples
cd examples && go build ./...

# Sync workspace
go work sync
```

## Code Style

- Follow idiomatic Go style (gofmt, golangci-lint)
- All public APIs must have godoc comments
- No silent failures - errors must be handled explicitly

## Recent Changes
- 001-batch-table-interfaces: Added Go 1.25+ + Apache Arrow Go v18, gRPC, msgpack-go
- 006-returning-optimization: Added Go 1.25+ + Arrow-Go v18, gRPC, msgpack-go
- 005-module-reorganization: Added Go 1.25+

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
