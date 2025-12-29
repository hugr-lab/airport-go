# Implementation Plan: Batch Table Interfaces for Update and Delete

**Branch**: `001-batch-table-interfaces` | **Date**: 2025-12-29 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-batch-table-interfaces/spec.md`

## Summary

Add modern `UpdatableBatchTable` and `DeletableBatchTable` interfaces that simplify DML implementations by embedding the rowid column in the RecordReader parameter, eliminating the need for separate `rowIDs []int64` arrays. Refactor `handleDoExchangeUpdate` and `handleDoExchangeDelete` handlers to support both new batch interfaces and legacy interfaces with internal wrapper functions for backward compatibility.

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**: Apache Arrow Go v18, gRPC, msgpack-go
**Storage**: N/A (storage-agnostic library)
**Testing**: Go standard testing (`go test`), integration tests with DuckDB Airport extension
**Target Platform**: Cross-platform (Linux, macOS, Windows)
**Project Type**: Single Go module library
**Performance Goals**: Maintain current streaming efficiency; no additional allocations for batch interface path
**Constraints**: Must maintain backward compatibility with existing UpdatableTable/DeletableTable interfaces
**Scale/Scope**: Library package - affects catalog/table.go, flight/doexchange_dml.go, examples, docs

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Code Quality
- [x] Code follows idiomatic Go style (gofmt, golangci-lint compliant)
- [x] New interfaces documented with godoc-compatible comments
- [x] Explicit error handling (no panics, errors returned)
- [x] Simplicity: New interfaces are simpler than legacy pattern (fewer parameters)

### II. Testing Standards
- [x] Unit tests required for wrapper functions
- [x] Integration tests required for new interface dispatch
- [x] Tests must be deterministic and isolated
- [x] Race detector will be run (`go test -race`)

### III. User Experience Consistency
- [x] Public API extends existing patterns (consistent naming)
- [x] No breaking changes (legacy interfaces continue to work)
- [x] Documentation and examples updated
- [x] Follows Go naming idioms (BatchTable suffix pattern)

### IV. Performance Requirements
- [x] No additional memory allocations in batch interface path
- [x] Streaming RecordReader preserved (no buffering)
- [x] Context cancellation supported
- [x] Wrapper functions only invoked for legacy interface fallback

**Gate Status**: ✅ PASS - All constitution principles satisfied

### Post-Design Re-evaluation (Phase 1 Complete)

After completing Phase 1 design artifacts (research.md, data-model.md, contracts/, quickstart.md):

- [x] **Code Quality**: Interface designs follow Go conventions; godoc comments specified
- [x] **Testing Standards**: Contract tests defined in contracts/batch-interfaces.md
- [x] **User Experience**: Migration guide in quickstart.md; backward compatibility preserved
- [x] **Performance**: Batch interface eliminates extraction overhead; no new allocations

**Post-Design Gate Status**: ✅ PASS - Design aligns with constitution principles

## Project Structure

### Documentation (this feature)

```text
specs/001-batch-table-interfaces/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (created by /speckit.tasks)
```

### Source Code (repository root)

```text
# Go module library structure
catalog/
├── table.go            # Add UpdatableBatchTable, DeletableBatchTable interfaces
├── helpers.go          # Add wrapper/adapter functions (internal)
└── table_test.go       # Unit tests for wrapper functions

flight/
├── doexchange_dml.go   # Refactor handlers for dual interface support
└── doexchange_dml_test.go # Updated tests

examples/
└── dml/
    └── main.go         # Updated example with batch interface demo

docs/
└── api-guide.md        # Updated documentation

tests/
└── integration/
    └── dml_batch_test.go # New integration tests for batch interfaces
```

**Structure Decision**: Existing Go module library structure. Changes are additive to catalog/ and flight/ packages with updated examples and documentation.

## Complexity Tracking

> No constitution violations requiring justification. The new interfaces are simpler than the legacy pattern.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| N/A | N/A | N/A |
