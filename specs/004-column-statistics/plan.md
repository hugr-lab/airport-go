# Implementation Plan: Column Statistics

**Branch**: `004-column-statistics` | **Date**: 2025-11-29 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/004-column-statistics/spec.md`

## Summary

Implement the `column_statistics` DoAction handler to enable DuckDB to query field statistics (min, max, distinct_count, null presence, string metrics) for query optimization. Add a new `StatisticsTable` interface to the catalog package that tables can optionally implement. Response is an Arrow RecordBatch with a dynamically-typed schema for min/max fields.

**User Requirement**: Check that the statistic action is called if the DuckDB filtered by columns.

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**: Arrow-Go v18, gRPC, msgpack-go, ZStandard
**Storage**: N/A (storage-agnostic; delegated to user implementations)
**Testing**: go test with race detector, DuckDB as integration test client
**Target Platform**: Linux/macOS server
**Project Type**: Single Go module with subpackages
**Performance Goals**: Metadata action - no specific latency requirements
**Constraints**: Response schema must match Airport protocol exactly
**Scale/Scope**: Single action handler, one interface, integration tests

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | ✅ Pass | Will follow idiomatic Go, gofmt, golangci-lint |
| II. Testing Standards | ✅ Pass | Unit tests for handler, integration tests with DuckDB |
| III. User Experience Consistency | ✅ Pass | Optional interface pattern matches existing (InsertableTable, etc.) |
| IV. Performance Requirements | ✅ Pass | Metadata action, no streaming data - efficiency not critical |

**Gate Result**: PASS - All principles satisfied, no violations requiring justification.

## Project Structure

### Documentation (this feature)

```text
specs/004-column-statistics/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
catalog/
├── catalog.go           # Catalog interface (existing)
├── table.go             # Table interfaces including new StatisticsTable (modify)
├── dynamic.go           # DynamicCatalog/Schema/Table interfaces (existing)
├── static.go            # Static catalog builder (existing)
└── types.go             # Shared types including new ColumnStats (modify)

flight/
├── doaction.go          # DoAction router (modify - add column_statistics case)
├── doaction_ddl.go      # DDL handlers (existing)
├── doaction_statistics.go  # NEW: column_statistics handler
└── server.go            # Flight server (existing)

tests/integration/
├── statistics_test.go   # NEW: Integration tests for column_statistics
└── ...                  # Existing test files
```

**Structure Decision**: Single Go module structure. New handler file `doaction_statistics.go` follows pattern of `doaction_ddl.go`. Interface added to existing `catalog/table.go` to co-locate with other table interfaces.

## Complexity Tracking

No violations requiring justification. Implementation follows established patterns.
