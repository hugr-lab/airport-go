# Implementation Plan: DML RETURNING Clause Column Selection

**Branch**: `006-returning-optimization` | **Date**: 2025-11-30 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/006-returning-optimization/spec.md`

## Summary

Enable DML operations (INSERT, UPDATE, DELETE) to return only the columns specified in the RETURNING clause, rather than all columns. This requires:
1. Investigating how DuckDB Airport extension communicates RETURNING column names
2. Adding a `DMLOptions` struct parameter to DML interface methods
3. Passing RETURNING column information from protocol layer to table implementations
4. Updating all existing implementations and adding integration tests

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**: Arrow-Go v18, gRPC, msgpack-go
**Storage**: N/A (library, storage-agnostic)
**Testing**: go test with DuckDB Airport extension for integration tests
**Target Platform**: Linux/macOS/Windows (Go library)
**Project Type**: Single Go module with workspace (library + examples + tests)
**Performance Goals**: No degradation from current DML performance; reduced network payload for filtered RETURNING
**Constraints**: One-time breaking change to DML interfaces; must update all implementations
**Scale/Scope**: ~10 files to modify (catalog interfaces, flight handlers, examples, tests)

**Research Finding** (see [research.md](research.md)):
- DuckDB Airport extension does NOT communicate RETURNING column names to the server
- Only `return-chunks: 1/0` header indicates if RETURNING is requested
- DuckDB filters columns client-side after receiving server response
- `DMLOptions.ReturningColumns` is for future protocol extensions and server-side optimization

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | ✅ PASS | Follows idiomatic Go; explicit error handling; godoc comments required |
| II. Testing Standards | ✅ PASS | Unit tests for DMLOptions; integration tests for RETURNING column filtering |
| III. User Experience Consistency | ⚠️ BREAKING | One-time breaking change to DML interfaces; justified as pre-1.0 library; enables future extensibility |
| IV. Performance Requirements | ✅ PASS | Reduces network payload; streaming preserved |

**Breaking Change Justification**: The library is pre-1.0 (v0.1.0). Adding `DMLOptions` parameter is a one-time breaking change that enables future extensibility without further interface changes. This aligns with constitution principle III which states breaking changes "MUST follow semantic versioning" - appropriate for 0.x versions.

## Project Structure

### Documentation (this feature)

```text
specs/006-returning-optimization/
├── plan.md              # This file
├── research.md          # Phase 0: Protocol investigation
├── data-model.md        # Phase 1: DMLOptions struct design
├── quickstart.md        # Phase 1: Migration guide
├── contracts/           # Phase 1: Interface contracts
│   └── dml-interfaces.md
└── tasks.md             # Phase 2: Implementation tasks
```

### Source Code (repository root)

```text
# Main library module
catalog/
├── types.go             # DMLOptions struct (NEW)
├── table.go             # InsertableTable, UpdatableTable, DeletableTable (MODIFY)
└── ...

flight/
├── doexchange_dml.go    # DML handlers - extract & pass RETURNING columns (MODIFY)
├── dml_types.go         # Protocol types (MODIFY if needed)
└── ...

# Examples module
examples/
├── dml/main.go          # Update to new signatures (MODIFY)
└── ...

# Tests module
tests/
├── integration/
│   ├── dml_test.go      # Update existing tests (MODIFY)
│   └── returning_columns_test.go  # New tests (NEW)
└── ...
```

**Structure Decision**: Existing Go module structure with workspace. Changes span catalog interfaces, flight handlers, examples, and integration tests.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Breaking interface change | Enable RETURNING column filtering and future extensibility | Adding optional interface would fragment API; nil-parameter approach still requires signature change |
