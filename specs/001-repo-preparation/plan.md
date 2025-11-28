# Implementation Plan: Repository Preparation for Production Use

**Branch**: `001-repo-preparation` | **Date**: 2025-11-26 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-repo-preparation/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

This feature prepares the airport-go repository for production use through three major initiatives:

1. **Repository Reorganization (P1)**: Move integration tests to `tests/integration/`, remove unused directories, update documentation
2. **CI/CD Automation (P1)**: Implement GitHub Actions workflow with linting (golangci-lint) and testing (unit + integration with race detector)
3. **Example Enhancement (P2)**: Add DuckDB client connection code to all examples (basic, auth, dynamic) demonstrating SELECT queries
4. **DDL Operations (P2)**: Implement CREATE/DROP SCHEMA, CREATE/DROP/ALTER TABLE via DoAction RPC with IF EXISTS/IF NOT EXISTS support
5. **DML Operations (P2)**: Implement INSERT/UPDATE/DELETE via DoPut/DoAction RPC with Arrow record batch handling
6. **Point-in-Time Queries (P3)**: Support ts/ts_ns parameters for time-travel queries
7. **Endpoint Discovery (P3)**: Implement flight_info, endpoints, table_function_flight_info RPC actions

**Technical Approach**: Leverage existing catalog, auth, and serialize packages. Use Apache Arrow Flight RPC for DDL/DML operations. Align with DuckDB Airport extension capabilities (IF EXISTS/IF NOT EXISTS, rowid support). Follow idiomatic Go patterns with explicit error handling and structured logging (log/slog).

## Technical Context

**Language/Version**: Go 1.21+
**Primary Dependencies**:
- Apache Arrow Go v18 (Flight RPC, IPC serialization)
- DuckDB 1.4+ with latest Airport extension (for examples only)
- github.com/paulmach/orb v0.11+ (geometry abstractions for geospatial types)
- golangci-lint (for CI linting)
- GitHub Actions (for CI/CD)

**Storage**: Implementation-defined (spec remains storage-agnostic); Airport server abstracts storage via catalog.Catalog interface
**Testing**:
- Unit tests: Go standard testing framework
- Integration tests: Go testing with real Flight server instances
- Race detector: All tests run with `-race` flag in CI
- Test data: Up to 100M rows for DML scale validation

**Target Platform**: Linux/macOS server environments (gRPC/Flight RPC services)
**Project Type**: Single Go package with multiple subpackages (library + examples)
**Performance Goals**:
- DDL operations: <1 second for empty operations
- DML operations: 1000 rows in <2 seconds for INSERT
- Point-in-time queries: <10% performance degradation vs current queries
- Endpoint discovery: <500ms

**Constraints**:
- Zero breaking changes to existing public APIs (catalog, auth, builder packages)
- Single-writer model for DDL operations (no distributed consensus)
- No distributed transactions or two-phase commit
- Structured logging only (log/slog with JSON format)

**Scale/Scope**:
- DML operations: Unlimited rows (implementation-defined limits); test suite validates up to 100M rows
- CI workflow: <10 minutes completion time
- 7 user stories across 3 priority levels (P1, P2, P3)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Code Quality ✅

- **Idiomatic Go**: All code will follow gofmt, golangci-lint, Effective Go conventions
- **Simplicity**: Leveraging existing catalog/auth/serialize packages; minimal new abstractions
- **Documentation**: All public DDL/DML APIs will have godoc comments
- **Error Handling**: All Flight RPC handlers return explicit errors; no panics

**Status**: PASS - Plan aligns with idiomatic Go practices

### II. Testing Standards ✅

- **Unit Tests**: Required for all DDL/DML command parsing, Arrow schema validation, IF EXISTS/IF NOT EXISTS logic
- **Integration Tests**: Required for all Flight RPC handlers (DoAction for DDL, DoPut for INSERT, DoAction for DELETE)
- **Deterministic**: Tests will use in-memory catalogs and mock storage; no external services
- **CI with Race Detector**: GitHub Actions workflow will run all tests with `-race` flag

**Status**: PASS - Comprehensive testing strategy defined

### III. User Experience Consistency ✅

- **API Stability**: Zero breaking changes to existing public APIs (FR-004, SC-010)
- **Go Naming Idioms**: Following existing patterns in catalog package
- **Documentation**: Examples enhanced with clear README files specifying DuckDB 1.4+ requirements
- **Semantic Versioning**: No version bump required (internal refactoring + new features, backward compatible)

**Status**: PASS - Maintains API stability and consistency

### IV. Performance Requirements ✅

- **Efficient Memory**: Arrow IPC streaming already implemented in internal/serialize
- **Context Cancellation**: All Flight RPC handlers accept context.Context for cancellation
- **Performance Targets**: Specific metrics defined (SC-005 through SC-008)
- **Profiling**: Test suite will validate 100M row operations for scale

**Status**: PASS - Performance requirements clearly specified

### Constitution Re-Check Post-Design

**Performed**: 2025-11-26 after Phase 1 design completion

#### I. Code Quality ✅

- **Idiomatic Go**: Confirmed in contracts - JSON marshaling, explicit error handling, no panics
- **Simplicity**: Research confirms minimal abstractions; leveraging existing packages
- **Documentation**: Quickstart provides clear developer guide; contracts document all APIs
- **Error Handling**: Data model specifies gRPC status codes for all error cases

**Status**: PASS - Design follows idiomatic Go patterns

#### II. Testing Standards ✅

- **Unit Tests**: Data model defines validation rules for unit testing
- **Integration Tests**: Quickstart includes test examples; contracts specify test scenarios
- **Deterministic**: Research confirms in-memory catalogs for tests
- **CI with Race Detector**: GitHub Actions workflow includes `-race` flag

**Status**: PASS - Comprehensive test strategy confirmed in design

#### III. User Experience Consistency ✅

- **API Stability**: Data model confirms no public API changes (Flight handlers in flight/ subpackage)
- **Go Naming Idioms**: Contracts use idiomatic names (CreateSchemaAction, not GetCreateSchema)
- **Documentation**: Quickstart + contracts provide complete developer documentation
- **Semantic Versioning**: No breaking changes; backward compatible additions

**Status**: PASS - API stability maintained in design

#### IV. Performance Requirements ✅

- **Efficient Memory**: Data model specifies batch sizing (1000-10000 rows); quickstart includes memory leak testing
- **Context Cancellation**: Contracts show context.Context in all RPC signatures
- **Performance Targets**: Confirmed from spec (SC-005 through SC-008)
- **Profiling**: Research documents streaming approach for large datasets

**Status**: PASS - Performance considerations embedded in design

**Overall**: ✅ All constitution principles satisfied. Design ready for implementation.

## Project Structure

### Documentation (this feature)

```text
specs/001-repo-preparation/
├── spec.md              # Feature specification
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── ddl-actions.md   # DoAction RPC payloads for DDL
│   ├── dml-actions.md   # DoPut/DoAction RPC payloads for DML
│   └── endpoints.md     # flight_info, endpoints, table_function_flight_info
├── checklists/
│   └── requirements.md  # Quality checklist (already created)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

**Current Structure**:
```text
airport-go/
├── auth/                # Authentication (bearer token)
├── catalog/             # Catalog interfaces and types
├── flight/              # Flight server implementation
├── internal/
│   └── serialize/       # Arrow IPC serialization
├── examples/
│   ├── basic/           # Basic server example
│   ├── auth/            # Authenticated server example
│   └── dynamic/         # Dynamic catalog example
├── tests/               # Currently empty (new in this feature)
│   └── integration/     # Integration tests (moved from root)
├── testutil/            # Test utilities
├── *.go                 # Root-level package files
└── *_test.go            # Root-level test files
```

**Structure After This Feature**:
```text
airport-go/
├── auth/                # No changes
├── catalog/             # No changes
├── flight/              # Enhanced with DDL/DML/endpoint handlers
│   ├── server.go        # Existing Flight server
│   ├── ddl.go           # NEW: DoAction handlers for DDL
│   ├── dml.go           # NEW: DoPut/DoAction handlers for DML
│   └── endpoints.go     # NEW: flight_info/endpoints/table_function_flight_info
├── internal/
│   └── serialize/       # Potentially enhanced for DDL/DML payloads
├── examples/
│   ├── basic/
│   │   ├── main.go      # Server code (unchanged)
│   │   ├── client.sql   # NEW: DuckDB client example
│   │   └── README.md    # UPDATED: Installation instructions for DuckDB 1.4+
│   ├── auth/
│   │   ├── main.go      # Server code (unchanged)
│   │   ├── client.sql   # NEW: DuckDB client with auth
│   │   └── README.md    # UPDATED: Auth + DuckDB setup
│   └── dynamic/
│       ├── main.go      # Server code (unchanged)
│       ├── client.sql   # NEW: DuckDB client with dynamic catalog
│       └── README.md    # UPDATED: Dynamic catalog + DuckDB
├── tests/
│   └── integration/     # Integration tests moved from root
│       ├── auth_test.go
│       ├── catalog_test.go
│       ├── dynamic_test.go
│       ├── functions_test.go
│       ├── query_test.go
│       └── integration_test.go (base)
├── testutil/            # No changes
├── .github/
│   └── workflows/
│       └── ci.yml       # NEW: GitHub Actions CI workflow
├── *.go                 # Root-level package files (unchanged)
└── *_test.go            # Root-level unit tests (unchanged)
```

**Structure Decision**:

This is a **single Go package** with multiple subpackages (Option 1 style). The repository follows standard Go project layout:

- **Package root** (`airport-go/`): Core types (CatalogBuilder, SimpleTableDef) and root-level tests
- **Subpackages** (`auth/`, `catalog/`, `flight/`, `internal/`): Modular functionality
- **Examples**: Separate runnable programs demonstrating server usage
- **Tests**: Conventional `tests/integration/` for integration tests, root-level `*_test.go` for unit tests

**Changes**:
1. Move `integration_*_test.go` files from root to `tests/integration/`
2. Add `flight/ddl.go`, `flight/dml.go`, `flight/endpoints.go` for new RPC handlers
3. Add `.github/workflows/ci.yml` for CI automation
4. Enhance example README files with DuckDB client instructions

**No unused directories to remove** - all current top-level directories are actively used.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

*No violations identified. All requirements align with constitution principles:*
- Idiomatic Go with explicit error handling
- Comprehensive testing with race detector
- API stability maintained
- Performance targets specified

