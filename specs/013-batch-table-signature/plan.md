# Implementation Plan: Simplify Batch Table Interface Signatures

**Branch**: `013-batch-table-signature` | **Date**: 2025-12-30 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/013-batch-table-signature/spec.md`

## Summary

Change `UpdatableBatchTable.Update` and `DeletableBatchTable.Delete` method signatures from accepting `array.RecordReader` to `arrow.Record`. This simplifies both the interface (users access data directly without iterator semantics) and the handler implementation (no need to wrap single batches in RecordReader).

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**: Apache Arrow Go v18 (`github.com/apache/arrow-go/v18`), gRPC
**Storage**: N/A (storage-agnostic library)
**Testing**: Go test with race detector, integration tests with DuckDB Airport extension
**Target Platform**: Cross-platform (Linux, macOS, Windows)
**Project Type**: Go library (single module with workspace for tests/examples)
**Performance Goals**: Zero-copy where possible, streaming for large datasets
**Constraints**: Backward compatibility with legacy interfaces (`UpdatableTable`, `DeletableTable`)
**Scale/Scope**: Library change affecting 2 interfaces, 2 handlers, integration tests, and examples

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Evidence |
|-----------|--------|----------|
| I. Code Quality | ✅ PASS | Change follows idiomatic Go patterns; simplifies API by removing unnecessary abstraction |
| II. Testing Standards | ✅ PASS | Integration tests exist and will be updated; no new untested code paths |
| III. User Experience Consistency | ✅ PASS | Improves API clarity; backward compatible via legacy interfaces |
| IV. Performance Requirements | ✅ PASS | Removes RecordReader construction overhead; zero-copy improvement |

**Gate Result**: PASS - No violations. Proceed to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/013-batch-table-signature/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output (interface contracts)
├── quickstart.md        # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
# Go library structure
catalog/
├── table.go             # Interface definitions (UpdatableBatchTable, DeletableBatchTable)
├── helpers.go           # Helper functions (FindRowIDColumn)
└── types.go             # DMLOptions, DMLResult types

flight/
├── doexchange_dml.go    # Handler implementations (handleDoExchangeUpdate, handleDoExchangeDelete)
└── dml_types.go         # DML-related types

examples/
└── dml/main.go          # DML example (demonstrates both interface styles)

tests/
└── integration/
    └── dml_batch_test.go # Batch interface integration tests

docs/
└── api-guide.md         # API documentation
```

**Structure Decision**: Existing Go library structure. Changes affect `catalog/table.go` (interfaces), `flight/doexchange_dml.go` (handlers), `tests/integration/dml_batch_test.go`, `examples/dml/main.go`, and `docs/api-guide.md`.

## Complexity Tracking

> No violations - simplification reduces complexity.
