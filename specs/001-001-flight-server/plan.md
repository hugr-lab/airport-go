# Implementation Plan: Airport Go Flight Server Package

**Branch**: `001-001-flight-server` | **Date**: 2025-11-25 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-001-flight-server/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

A Go package (`airport`) that provides a high-level API for building Apache Arrow Flight servers compatible with the DuckDB Airport Extension. The package registers Flight service handlers on a user-provided `grpc.Server`, implements the Flight protocol (DoGet, DoPut, ListFlights, GetFlightInfo, DoAction), and provides a fluent catalog builder API for defining schemas, tables, and custom scalar functions. Users implement scan functions that return `arrow.RecordReader` for data streaming. The package supports bearer token authentication, dynamic catalog implementations via interfaces, and uses `slog.Default()` for logging.

## Technical Context

**Language/Version**: Go 1.25+ (recommended for latest stdlib features and performance)
**Primary Dependencies**:
- `github.com/apache/arrow/go/v18/arrow` - Arrow data structures and IPC
- `github.com/apache/arrow/go/v18/arrow/flight` - Flight RPC protocol
- `google.golang.org/grpc` - gRPC server framework
- `github.com/klauspost/compress/zstd` - ZStandard compression for catalog
- `github.com/vmihailenco/msgpack/v5` - MessagePack for parameter serialization
- `log/slog` - Structured logging (Go 1.21+ stdlib)
- `github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth` - Auth interceptor

**Storage**: N/A (package does not manage data storage - delegated to user's scan functions)

**Testing**:
- Unit tests: `go test` with race detector (`-race`)
- Integration tests: `github.com/duckdb/duckdb-go` for Airport extension client testing
- Mocking: `github.com/stretchr/testify/mock` for interface mocks

**Target Platform**: Linux/macOS/Windows server environments, containerized deployments (Kubernetes with ingress TLS termination)

**Project Type**: Single Go library package (not web/mobile)

**Performance Goals**:
- Support 100+ concurrent client connections without memory leaks
- Stream Arrow record batches without rebatching overhead
- Catalog serialization/deserialization under 10ms for typical schemas (10 tables, 100 columns)

**Constraints**:
- No panics in library code (all errors returned explicitly)
- Context cancellation support for all long-running operations
- Thread-safe concurrent request handling via gRPC goroutines
- TLS handled by user's gRPC server configuration

**Scale/Scope**:
- Single Go package with ~3-5 subpackages
- Target: 5,000-10,000 LOC including tests
- Support catalogs with 100s of tables, 1000s of columns

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Code Quality (Idiomatic Go)

✅ **Pass**: Design follows idiomatic Go patterns:
- Interface-based abstraction (Catalog, Schema interfaces)
- Functional options pattern for configuration (`ServerConfig`)
- Builder pattern for fluent catalog construction
- Context propagation for cancellation
- Explicit error returns (no panics)
- Standard library integration (`slog`, `context`)

**Action**: Ensure godoc comments on all public APIs, follow Go naming conventions (no `GetX` prefixes)

### II. Testing Standards

✅ **Pass**: Comprehensive testing plan:
- Unit tests for catalog builder, serialization, authentication
- Integration tests with DuckDB Airport extension as client
- Contract tests for Flight protocol compliance
- Race detector enabled in CI
- 80% coverage target (SC-002)

**Action**: Ensure deterministic tests (no live network dependencies except mocked gRPC)

### III. User Experience Consistency

✅ **Pass**: Stable, predictable API design:
- Single entry point: `NewServer(grpcServer, config)`
- Builder API for common case: `NewCatalogBuilder().Schema().Table().Build()`
- Interface escape hatch for advanced cases: custom `Catalog` implementation
- Clear separation of concerns: package handles protocol, user handles data

**Action**: Follow semantic versioning, document breaking changes

### IV. Performance Requirements

✅ **Pass**: Performance-conscious design:
- No rebatching of `arrow.RecordReader` streams (FR-018)
- Streaming catalog serialization with ZStandard compression
- Context deadlines on catalog method calls to prevent blocking
- Goroutine-per-request model (managed by gRPC, no custom pooling)

**Action**: Profile with `pprof` during development, benchmark catalog serialization

### Code Review Gates Readiness

- ✅ Linting: Will use `golangci-lint` with standard Go rules
- ✅ Tests: Unit + integration with `-race` flag
- ✅ Documentation: godoc comments on all public APIs (SC-010)
- ✅ API Review: Semantic versioning for breaking changes

**Verdict**: ✅ **All gates PASS** - Proceed to Phase 0 research

## Project Structure

### Documentation (this feature)

```text
specs/001-001-flight-server/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── catalog-interface.go.md   # Catalog/Schema interface definitions
│   ├── flight-handlers.go.md     # Flight RPC handler signatures
│   └── builder-api.go.md         # CatalogBuilder API
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
# Repository root is the airport package (github.com/hugr-lab/airport-go)
server.go                # NewServer function, ServerConfig
config.go                # Configuration types, standard errors
doc.go                   # Package documentation

catalog/                 # Subpackage: catalog interfaces and builder
├── catalog.go           # Catalog, Schema interfaces
├── builder.go           # CatalogBuilder fluent API
├── static.go            # Static catalog implementation from builder
├── table.go             # Table, DynamicSchemaTable interfaces
└── function.go          # ScalarFunction, TableFunction interfaces

flight/                  # Subpackage: Flight RPC handler implementations
├── server.go            # Flight service registration
├── doget.go             # DoGet RPC handler
├── doput.go             # DoPut RPC handler
├── listflights.go       # ListFlights RPC handler
├── getflightinfo.go     # GetFlightInfo RPC handler
└── doaction.go          # DoAction RPC handler

auth/                    # Subpackage: authentication implementations
├── bearer.go            # BearerAuth function type and helpers
└── context.go           # Context-based auth propagation

internal/                # Internal utilities (not public API)
├── serialize/           # ZStandard catalog serialization
├── msgpack/             # MessagePack parameter handling
└── recovery/            # Panic recovery middleware

examples/                # Example usage code
├── basic/               # Basic server with static catalog
├── dynamic/             # Dynamic catalog implementation
└── auth/                # Authentication example

testutil/                # Test utilities for integration tests
├── duckdb.go            # DuckDB Airport client helpers
└── mock_catalog.go      # Mock Catalog implementation

tests/
├── unit/                # Unit tests (co-located with source: *_test.go)
├── integration/         # Integration tests with DuckDB
│   ├── catalog_test.go  # Catalog discovery tests
│   ├── query_test.go    # Query execution tests
│   └── auth_test.go     # Authentication tests
└── benchmarks/          # Performance benchmarks
    ├── catalog_bench.go # Catalog serialization benchmarks
    └── stream_bench.go  # Arrow streaming benchmarks
```

**Structure Decision**: Repository root is the `airport` package (module `github.com/hugr-lab/airport-go`). Root-level files contain core API (`NewServer`, `ServerConfig`). Subpackages organize implementation: `catalog/` for catalog abstractions, `flight/` for RPC handlers, `auth/` for authentication, `internal/` for private utilities. This follows Go best practices: flat hierarchy, clear imports, co-located tests, public API at package root.

## Complexity Tracking

No constitutional violations requiring justification.

The design intentionally uses:
- **Interface abstraction** (Catalog, Schema): Justified by need for dynamic implementations (US-7)
- **Builder pattern**: Justified by fluent API requirement for developer experience (SC-001: under 30 LOC)
- **Panic recovery**: Justified by library safety requirement (FR-024: recover from user code panics)

All complexity serves explicit functional or quality requirements. No simpler alternative meets the stated goals.
