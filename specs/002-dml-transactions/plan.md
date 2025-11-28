# Implementation Plan: DML Operations and Transaction Management

**Branch**: `002-dml-transactions` | **Date**: 2025-11-28 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-dml-transactions/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Implement INSERT, UPDATE, and DELETE operations for Airport-managed tables through Arrow Flight RPC, with optional transaction management coordination. The implementation extends the existing Flight RPC handlers (DoPut for INSERT/UPDATE, DoAction for DELETE) with actual catalog integration, adds table capability checking (writable, rowid presence), and introduces an optional TransactionManager interface for coordinating multi-operation transactions with automatic commit/rollback handling.

## Technical Context

**Language/Version**: Go 1.25 (as specified in go.mod)
**Primary Dependencies**:
- `github.com/apache/arrow-go/v18` - Arrow data format & Flight RPC protocol
- `github.com/vmihailenco/msgpack/v5` - MessagePack serialization for parameters
- `google.golang.org/grpc` - gRPC framework for Flight service

**Storage**: N/A (storage-agnostic via catalog.Catalog interface)
**Testing**: `go test` with integration tests using DuckDB for realistic scenarios
**Target Platform**: Cross-platform (Linux, macOS, Windows)
**Project Type**: Single Go module with subpackages (catalog/, flight/, auth/, internal/)
**Performance Goals**: Streaming record batches for large DML operations; efficient memory usage with Arrow allocator
**Constraints**: Non-blocking operations with context.Context cancellation support; explicit error handling (no panics)
**Scale/Scope**: Library package for embedding in applications; handles arbitrary table sizes via streaming

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Design Gate (Phase 0 Entry)

| Principle | Requirement | Status | Notes |
|-----------|-------------|--------|-------|
| **I. Code Quality** | Idiomatic Go, gofmt/golangci-lint compliant | ✅ PASS | Will follow existing codebase patterns |
| **I. Code Quality** | Explicit error handling, no panics | ✅ PASS | Will use gRPC status codes as per existing handlers |
| **I. Code Quality** | Public APIs documented with godoc | ✅ PASS | All new interfaces will have godoc comments |
| **II. Testing** | Unit tests for all new code | ✅ PASS | Will add unit tests for DML handlers and transaction logic |
| **II. Testing** | Integration tests for protocol handling | ✅ PASS | Existing DML test scaffolding in tests/integration/dml_test.go |
| **II. Testing** | Tests deterministic, no external dependencies | ✅ PASS | Will use StaticCatalog with mock implementations |
| **III. UX Consistency** | Predictable, stable API | ✅ PASS | Extends existing catalog.Table interface pattern |
| **III. UX Consistency** | Go naming idioms | ✅ PASS | TransactionManager, InsertableTable, etc. |
| **IV. Performance** | Efficient memory, streaming | ✅ PASS | Arrow RecordReader/RecordBatch streaming pattern |
| **IV. Performance** | Context cancellation support | ✅ PASS | All operations accept context.Context |

**Gate Status**: ✅ PASSED - Proceed to Phase 0 Research

### Post-Design Gate (Phase 1 Complete)

| Principle | Requirement | Status | Design Verification |
|-----------|-------------|--------|---------------------|
| **I. Code Quality** | Idiomatic Go | ✅ PASS | Interface composition pattern (InsertableTable, etc.), unexported context keys |
| **I. Code Quality** | No panics | ✅ PASS | All errors returned via gRPC status codes; idempotent commit/rollback |
| **I. Code Quality** | Godoc documentation | ✅ PASS | All interfaces documented in data-model.md |
| **II. Testing** | Unit tests | ✅ PASS | Mock TransactionManager design supports isolated testing |
| **II. Testing** | Integration tests | ✅ PASS | Test patterns documented in quickstart.md |
| **II. Testing** | Deterministic | ✅ PASS | In-memory mocks, no external services required |
| **III. UX Consistency** | Stable API | ✅ PASS | Backward compatible - existing Table interface unchanged |
| **III. UX Consistency** | Go naming | ✅ PASS | TransactionManager, InsertableTable, DMLResult follow conventions |
| **IV. Performance** | Streaming | ✅ PASS | Uses array.RecordReader for all data flow |
| **IV. Performance** | Context cancellation | ✅ PASS | All interface methods accept context.Context |

**Gate Status**: ✅ PASSED - Design complete, ready for /speckit.tasks

## Project Structure

### Documentation (this feature)

```text
specs/002-dml-transactions/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
# Go module root package (github.com/hugr-lab/airport-go)
*.go                     # Root package: server.go, config.go, builder.go

catalog/
├── table.go             # MODIFY: Add InsertableTable, UpdatableTable, DeletableTable interfaces
├── transaction.go       # NEW: TransactionManager interface
├── types.go             # MODIFY: Add DMLResult, TransactionState types
└── static.go            # MODIFY: Add static implementations for testing

flight/
├── dml.go               # MODIFY: Implement actual DML handlers with catalog integration
├── dml_types.go         # MODIFY: Add transaction header parsing, result types
├── doput.go             # MODIFY: Add transaction context propagation
├── doaction.go          # MODIFY: Add create_transaction action
└── transaction.go       # NEW: Transaction context helpers

internal/
└── txcontext/           # NEW: Transaction context utilities
    └── context.go       # Transaction ID extraction/injection

tests/integration/
├── dml_test.go          # MODIFY: Expand with real catalog integration tests
└── transaction_test.go  # NEW: Transaction management tests
```

**Structure Decision**: Single Go module following existing package structure. New interfaces added to `catalog/` package following established patterns. Transaction context utilities in `internal/` to avoid public API pollution.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No violations identified. The design follows existing patterns:
- Interface-based extension (InsertableTable, etc.) matches existing Table interface pattern
- Transaction context propagation uses standard Go context.Context patterns
- All new code adheres to constitution principles
