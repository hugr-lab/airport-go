# Implementation Plan: Multi-Catalog Server Support

**Branch**: `001-multicatalog-server` | **Date**: 2026-01-08 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-multicatalog-server/spec.md`

## Summary

Implement a `MultiCatalogServer` that aggregates multiple catalogs and routes Flight RPC requests to the appropriate internal server based on the `airport-catalog` metadata header. The implementation includes:
- High-level `NewMultiCatalogServer(grpcServer, config)` API similar to existing `NewServer`
- Dynamic catalog management (AddCatalog/RemoveCatalog)
- Catalog-aware authentication via `CatalogAwareAuthenticator` interface
- Catalog-aware transaction management via `CatalogAwareTransactionManager` interface
- Context propagation for trace/session IDs

## Technical Context

**Language/Version**: Go 1.25+
**Primary Dependencies**: Apache Arrow Go v18, gRPC, google.golang.org/grpc/metadata
**Storage**: N/A (storage-agnostic library)
**Testing**: go test with race detector, integration tests in tests/ module
**Target Platform**: Linux/macOS server (any platform supporting Go)
**Project Type**: Single Go library with workspace modules
**Performance Goals**: Minimal routing overhead (<1ms per request lookup)
**Constraints**: Thread-safe concurrent access, no blocking operations, context cancellation support
**Scale/Scope**: Support 10+ concurrent catalogs, 1000+ concurrent requests

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | PASS | Idiomatic Go, explicit error handling, godoc comments required |
| II. Testing Standards | PASS | Unit tests for all logic, integration tests for routing/auth |
| III. User Experience Consistency | PASS | API follows existing flight.Server patterns, stable interface |
| IV. Performance Requirements | PASS | Thread-safe map, streaming support, context cancellation |

**Gate Status**: PASSED - No violations requiring justification.

## Project Structure

### Documentation (this feature)

```text
specs/001-multicatalog-server/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
# Root package (high-level API)
multicatalog.go          # NEW: NewMultiCatalogServer function + MultiCatalogServerConfig
multicatalog_test.go     # NEW: Unit tests for high-level API

flight/
├── server.go            # MODIFY: Add Catalog() method to expose catalog
├── multicatalog.go      # NEW: MultiCatalogServer implementation (internal dispatcher)
├── multicatalog_test.go # NEW: Unit tests for dispatcher
├── doaction.go          # Existing (no changes)
├── doexchange.go        # Existing (no changes)
├── doget.go             # Existing (no changes)
├── getflightinfo.go     # Existing (no changes)
├── listflights.go       # Existing (no changes)
└── ...

auth/
├── auth.go              # Existing Authenticator interface
├── catalog_aware.go     # NEW: CatalogAwareAuthenticator interface
├── context.go           # MODIFY: Add trace/session/catalog ID context helpers
├── interceptor.go       # MODIFY: Add catalog-aware interceptor variant
└── ...

catalog/
├── transaction.go       # MODIFY: Add CatalogAwareTransactionManager interface
└── ...

tests/
└── integration/
    └── multicatalog_test.go  # NEW: Integration tests
```

**Structure Decision**:
- High-level API (`NewMultiCatalogServer`) in root package (mirrors existing `NewServer`)
- Low-level dispatcher in `flight/` package
- Extended interfaces in `auth/` and `catalog/` packages
- Follows existing project organization patterns

## Complexity Tracking

No violations to justify - design follows existing patterns and adds minimal complexity.
