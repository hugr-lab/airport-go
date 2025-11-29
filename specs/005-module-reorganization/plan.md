# Implementation Plan: Module Reorganization

**Branch**: `005-module-reorganization` | **Date**: 2025-11-29 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/005-module-reorganization/spec.md`

## Summary

Reorganize the airport-go repository into a Go workspace with three separate modules:
1. **Main module** (`github.com/hugr-lab/airport-go`) - Core library with minimal dependencies
2. **Examples module** (`github.com/hugr-lab/airport-go/examples`) - All example code
3. **Tests module** (`github.com/hugr-lab/airport-go/tests`) - Integration tests and benchmarks

This removes DuckDB from the main module's dependency tree, cleans up unused code, and creates comprehensive protocol documentation.

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**:
- Main: Arrow-Go v18, gRPC, msgpack-go, klauspost/compress
- Tests/Examples: DuckDB-Go v2.5.3 (heavy dependency, isolated to sub-modules)
**Storage**: N/A (library, storage-agnostic)
**Testing**: `go test` with race detector, integration tests use DuckDB as Flight client
**Target Platform**: Cross-platform (darwin, linux, windows)
**Project Type**: Library with multi-module monorepo
**Performance Goals**: Minimal dependency footprint (<50MB excluding DuckDB)
**Constraints**: Go 1.18+ for workspace support, backward compatible API
**Scale/Scope**: 3 modules, ~70 source files, ~15 integration tests, 8 examples

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Requirement | Status | Notes |
|-----------|-------------|--------|-------|
| I. Code Quality | gofmt, golangci-lint, godoc | ✅ PASS | No new code; reorganization only |
| II. Testing Standards | Unit + integration tests, race detector | ✅ PASS | Tests moved but preserved |
| III. User Experience | Stable API, documentation | ✅ PASS | No API changes; improved docs |
| IV. Performance | Efficient, non-blocking | ✅ PASS | Reduces dependency bloat |

**Gate Result**: PASS - All principles maintained. This is a structural reorganization with no behavioral changes.

## Project Structure

### Documentation (this feature)

```text
specs/005-module-reorganization/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output (module structure)
├── quickstart.md        # Phase 1 output (developer guide)
├── contracts/           # Phase 1 output (N/A for this feature)
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

**Current Structure** (before reorganization):
```text
airport-go/
├── go.mod                    # Single module with all deps (including DuckDB)
├── *.go                      # Root package files
├── auth/                     # Auth subpackage
├── catalog/                  # Catalog interfaces
├── flight/                   # Flight RPC handlers
├── internal/                 # Internal utilities
├── examples/                 # Examples (no go.mod)
│   ├── auth/
│   ├── basic/
│   ├── ddl/
│   ├── dml/
│   ├── dynamic/
│   ├── functions/
│   ├── timetravel/
│   └── tls/
├── tests/                    # Tests (no go.mod)
│   ├── integration/
│   ├── benchmarks/           # Empty, benchmark_test.go at root
│   └── unit/                 # Empty
└── benchmark_test.go         # Benchmarks at root
```

**Target Structure** (after reorganization):
```text
airport-go/
├── go.mod                    # Main library (NO DuckDB)
├── go.work                   # Workspace configuration
├── go.work.sum               # Workspace checksums
├── *.go                      # Root package files
├── auth/
├── catalog/
├── flight/
├── internal/
├── docs/                     # NEW: Protocol documentation
│   ├── README.md             # Docs index
│   ├── protocol.md           # Airport protocol overview
│   ├── api-guide.md          # API reference
│   └── implementation.md     # Implementation guide
├── examples/                 # NEW: Separate module
│   ├── go.mod                # github.com/hugr-lab/airport-go/examples
│   ├── auth/
│   ├── basic/
│   ├── ddl/
│   ├── dml/
│   ├── dynamic/
│   ├── functions/
│   ├── timetravel/
│   └── tls/
└── tests/                    # NEW: Separate module
    ├── go.mod                # github.com/hugr-lab/airport-go/tests
    ├── integration/          # Integration tests
    └── benchmarks/           # Benchmarks (moved from root)
```

**Structure Decision**: Multi-module Go workspace with 3 modules. The main module contains only core library code. Examples and tests are isolated with their own go.mod files to prevent dependency pollution.

## Complexity Tracking

No violations. This reorganization simplifies the project structure and reduces complexity by:
- Removing unused dependencies from main module
- Isolating heavy dependencies (DuckDB) to test modules
- Clarifying module boundaries

## Constitution Check (Post-Design)

*Re-evaluation after Phase 1 design artifacts completed.*

| Principle | Requirement | Status | Post-Design Notes |
|-----------|-------------|--------|-------------------|
| I. Code Quality | gofmt, golangci-lint, godoc | ✅ PASS | No new code patterns; docs added |
| II. Testing Standards | Unit + integration tests, race detector | ✅ PASS | Tests reorganized; all preserved |
| III. User Experience | Stable API, documentation | ✅ PASS | API unchanged; docs/ folder added |
| IV. Performance | Efficient, non-blocking | ✅ PASS | Dependency footprint reduced |

**Post-Design Gate Result**: PASS - Design artifacts align with constitution. Ready for task generation.

## Generated Artifacts

| Artifact | Path | Status |
|----------|------|--------|
| Research | [research.md](research.md) | ✅ Complete |
| Data Model | [data-model.md](data-model.md) | ✅ Complete |
| Contracts | [contracts/](contracts/) | ✅ N/A (no API changes) |
| Quickstart | [quickstart.md](quickstart.md) | ✅ Complete |

## Next Steps

Run `/speckit.tasks` to generate implementation tasks from this plan.
