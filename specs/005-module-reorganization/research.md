# Research: Module Reorganization

**Feature**: 005-module-reorganization
**Date**: 2025-11-29
**Status**: Complete

## Research Questions

1. Go workspace best practices for multi-module repositories
2. Module naming conventions for nested directories
3. Replace directive handling for local development
4. Common pitfalls when reorganizing modules
5. CI/CD considerations for go.work

---

## Key Findings

### Go Workspace Best Practices

**Finding**: Go officially recommends "1 repo = 1 module" for libraries. Multi-module monorepos add significant complexity (semver coordination, CI/CD overhead, testing orchestration).

**However**, the airport-go project has a specific requirement: isolate DuckDB (heavy dependency ~100MB) from the main module to prevent dependency pollution for library consumers.

### Decision: Multi-Module Structure (User Requirement Override)

Despite Go's single-module recommendation, we proceed with multi-module structure because:

1. **DuckDB isolation is critical** - Library consumers should not download DuckDB binaries
2. **Current go.mod already includes DuckDB** - This is the problem we're solving
3. **User explicitly requested separate modules** - Confirmed in spec clarifications

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Main module deps | Arrow-Go, gRPC, msgpack, compress only | Core library functionality |
| DuckDB location | tests/ and examples/ modules only | Heavy binary, not needed by consumers |
| go.work | Create but DO NOT commit | Local development only |
| .gitignore | Add go.work | Prevents accidental commits |

---

## Module Naming

**Decision**: Use nested module paths matching directory structure.

```
github.com/hugr-lab/airport-go           # Main library
github.com/hugr-lab/airport-go/examples  # Examples module
github.com/hugr-lab/airport-go/tests     # Tests module
```

**Rationale**: Standard Go convention for nested modules in monorepos.

---

## Replace Directives

**Decision**: Never commit replace directives. Use go.work for local development.

**go.work configuration** (local only, not committed):
```
go 1.25

use (
    .
    ./examples
    ./tests
)
```

**Before commits**: Always run:
```bash
go work sync
go mod tidy
cd examples && go mod tidy
cd tests && go mod tidy
```

---

## CI/CD Considerations

**Decision**: go.work NOT committed; CI builds each module independently.

| CI Step | Command | Notes |
|---------|---------|-------|
| Build main | `go build ./...` | No DuckDB required |
| Test main | `go test ./...` | Unit tests only |
| Build examples | `cd examples && go build ./...` | Requires DuckDB |
| Integration tests | `cd tests && go test ./integration/...` | Requires DuckDB |
| Benchmarks | `cd tests && go test -bench=. ./benchmarks/...` | Requires DuckDB |

**Rationale**: Each module tested independently. Main module can be built/tested without DuckDB installation.

---

## Common Pitfalls to Avoid

| Pitfall | Mitigation |
|---------|------------|
| go.work accidentally committed | Add to .gitignore |
| Replace directives in go.mod | Never commit; use go.work locally |
| Version drift between modules | Pin to same Arrow-Go, gRPC versions |
| Missing test coverage | CI runs tests for all 3 modules |
| go.mod inconsistencies | Run `go mod tidy` in each module before commit |

---

## Dependency Analysis

### Current Main Module Dependencies (to remove)

```
github.com/duckdb/duckdb-go/v2 v2.5.3          # REMOVE - move to tests/
github.com/duckdb/duckdb-go-bindings/*         # REMOVE - indirect, goes with duckdb-go
github.com/duckdb/duckdb-go/arrowmapping       # REMOVE - indirect
github.com/duckdb/duckdb-go/mapping            # REMOVE - indirect
```

### Main Module Dependencies (to keep)

```
github.com/apache/arrow-go/v18                  # KEEP - core functionality
github.com/klauspost/compress                   # KEEP - compression
github.com/paulmach/orb                         # KEEP - geometry types
github.com/vmihailenco/msgpack/v5               # KEEP - protocol encoding
golang.org/x/sync                               # KEEP - concurrency primitives
google.golang.org/grpc                          # KEEP - Flight RPC
google.golang.org/protobuf                      # KEEP - Flight protocol
github.com/google/uuid                          # KEEP - ID generation
```

### Potentially Unused (verify before removal)

```
github.com/paulmach/orb                         # CHECK - verify usage in catalog/geometry.go
```

---

## Documentation Structure

**Decision**: Create `docs/` folder with structured documentation.

```
docs/
├── README.md             # Index and navigation
├── protocol.md           # Airport protocol overview (actions, messages)
├── api-guide.md          # Public API reference (Catalog, Schema, Table interfaces)
└── implementation.md     # Guide for implementing custom catalogs
```

**Content Sources**:
- README.md current content → split into docs/
- GoDoc comments → reference in api-guide.md
- airport.query.farm documentation → summarize in protocol.md
- Examples → reference in implementation.md

---

## Alternatives Considered

### Alternative 1: Single Module (Go Recommended)

Keep everything in one module, accept DuckDB as a dependency.

**Rejected Because**: Users would download ~100MB of DuckDB binaries when they only need the Flight server library (~5MB).

### Alternative 2: Separate go.mod Per Example

Each example gets its own go.mod (8 modules total).

**Rejected Because**: Excessive complexity. Single examples/ module is sufficient and simpler to maintain.

### Alternative 3: Use Build Tags

Use `//go:build integration` to exclude DuckDB from main builds.

**Rejected Because**: Doesn't solve dependency pollution. `go mod download` still fetches all deps regardless of build tags.

---

## Summary

| Topic | Decision |
|-------|----------|
| Module count | 3 (main, examples, tests) |
| go.work | Use locally, do NOT commit |
| DuckDB isolation | examples/ and tests/ only |
| Documentation | Create docs/ folder |
| CI/CD | Test each module independently |
