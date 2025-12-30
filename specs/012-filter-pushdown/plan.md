# Implementation Plan: Filter Pushdown Encoder-Decoder Package

**Branch**: `012-filter-pushdown` | **Date**: 2025-12-29 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/012-filter-pushdown/spec.md`

## Summary

Implement a standalone Go package (`filter/`) that parses DuckDB Airport extension filter pushdown JSON into strongly-typed Go structures and encodes them to SQL strings. The package supports:
- Full JSON parsing of DuckDB filter expressions
- DuckDB SQL dialect encoding with column name/expression mapping
- Extensible encoder interface for custom SQL dialects
- Graceful handling of unsupported expressions (skip instead of error)

## Technical Context

**Language/Version**: Go 1.25+ (matching existing project)
**Primary Dependencies**: Standard library only (encoding/json for parsing, strings/fmt for encoding). No new external dependencies required.
**Storage**: N/A (pure transformation library)
**Testing**: Go standard testing (`go test`), integration tests with DuckDB Airport extension in `tests/` module
**Target Platform**: Any platform supported by Go (library package)
**Project Type**: Single Go module with new subpackage
**Performance Goals**: <1ms parsing for typical filters (up to 10 conditions), zero allocations in hot paths where practical
**Constraints**: Pure Go (no CGO/DuckDB C library), separate package from core, no changes to existing public APIs
**Scale/Scope**: Expression trees with 10-100 nodes typical, deeply nested (10+ levels) supported

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | ✅ PASS | Package follows idiomatic Go: explicit error handling, no panics, godoc comments on all public APIs |
| II. Testing Standards | ✅ PASS | Unit tests for all expression types, integration tests with DuckDB client (20+ WHERE conditions) |
| III. User Experience Consistency | ✅ PASS | New package adds capability without changing existing APIs (ScanOpts.Filter remains []byte) |
| IV. Performance Requirements | ✅ PASS | Streaming/incremental not needed (single JSON parse), context.Context for cancellation, profile if needed |

**Code Review Gates**:
- Linting: `gofmt` and `golangci-lint` pass
- Tests: Unit + integration with race detector
- Documentation: All public types/functions have godoc
- API Review: No breaking changes (additive only)

## Project Structure

### Documentation (this feature)

```text
specs/012-filter-pushdown/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (Go interfaces)
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
# New package structure
filter/                      # New package: github.com/hugr-lab/airport-go/filter
├── doc.go                   # Package documentation
├── parse.go                 # JSON parsing: Parse([]byte) (*FilterPushdown, error)
├── types.go                 # Expression types: Expression, ComparisonExpr, etc.
├── logical_types.go         # LogicalType, Value, type conversions
├── encode.go                # Encoder interface and helpers
├── duckdb.go                # DuckDB SQL dialect encoder
├── parse_test.go            # Unit tests for parsing
├── encode_test.go           # Unit tests for encoding
└── duckdb_test.go           # Unit tests for DuckDB encoder

# Integration tests (existing tests module)
tests/integration/
└── filter_pushdown_test.go  # Integration tests with DuckDB client (20+ WHERE conditions)

# Examples (existing examples module)
examples/filter/
└── main.go                  # Example: parse filter, encode to SQL with column mapping
```

**Structure Decision**: Single new subpackage `filter/` in the main module. Follows existing pattern of `auth/`, `catalog/`, `flight/` packages. Integration tests in `tests/` module to avoid DuckDB dependency in main module.

## Complexity Tracking

No constitution violations. Design is straightforward:
- Standard Go interfaces for extensibility (no complex abstractions)
- Single-pass JSON parsing with standard library
- Visitor pattern for encoding (common for expression trees)
