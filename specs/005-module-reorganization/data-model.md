# Data Model: Module Structure

**Feature**: 005-module-reorganization
**Date**: 2025-11-29

This document describes the target module structure for the airport-go repository.

## Module Entities

### 1. Main Module

**Path**: `github.com/hugr-lab/airport-go`
**Location**: Repository root (`/`)

**Purpose**: Core Flight server library with minimal dependencies.

**Direct Dependencies** (go.mod require):
```
github.com/apache/arrow-go/v18
github.com/klauspost/compress
github.com/paulmach/orb
github.com/vmihailenco/msgpack/v5
github.com/google/uuid
golang.org/x/sync
google.golang.org/grpc
google.golang.org/protobuf
```

**Excluded Dependencies** (moved to tests/examples):
```
github.com/duckdb/duckdb-go/v2    # Moved to tests module
```

**Packages**:
| Package | Path | Description |
|---------|------|-------------|
| airport | `/` | Main package, server factory, builder |
| auth | `/auth` | Authentication interfaces and bearer token |
| catalog | `/catalog` | Catalog, Schema, Table interfaces |
| flight | `/flight` | Flight RPC handlers |
| internal/serialize | `/internal/serialize` | IPC serialization |
| internal/msgpack | `/internal/msgpack` | Msgpack decoding |
| internal/recovery | `/internal/recovery` | Panic recovery |
| internal/txcontext | `/internal/txcontext` | Transaction context |

**Test Files** (remain in main module, unit tests only):
- `*_test.go` files at root and in subpackages
- Must NOT import DuckDB

---

### 2. Examples Module

**Path**: `github.com/hugr-lab/airport-go/examples`
**Location**: `/examples/`

**Purpose**: Demonstration code for library usage.

**Dependencies** (go.mod):
```
github.com/hugr-lab/airport-go        # Main library (replace in development)
github.com/apache/arrow-go/v18        # Arrow types for examples
google.golang.org/grpc                # gRPC server setup
```

**Subdirectories** (packages within examples module):
| Package | Path | Description |
|---------|------|-------------|
| auth | `/examples/auth` | Bearer token authentication example |
| basic | `/examples/basic` | Minimal server example |
| ddl | `/examples/ddl` | DDL operations (CREATE/DROP) |
| dml | `/examples/dml` | DML operations (INSERT/UPDATE/DELETE) |
| dynamic | `/examples/dynamic` | Dynamic catalog example |
| functions | `/examples/functions` | Scalar/table functions example |
| timetravel | `/examples/timetravel` | Time travel queries example |
| tls | `/examples/tls` | TLS configuration example |

**Build Constraint**: Each subdirectory has `main` package for `go run .`

---

### 3. Tests Module

**Path**: `github.com/hugr-lab/airport-go/tests`
**Location**: `/tests/`

**Purpose**: Integration tests and benchmarks using DuckDB as Flight client.

**Dependencies** (go.mod):
```
github.com/hugr-lab/airport-go        # Main library (replace in development)
github.com/duckdb/duckdb-go/v2        # DuckDB client for testing
github.com/apache/arrow-go/v18        # Arrow types
google.golang.org/grpc                # gRPC for server setup
```

**Subdirectories**:
| Directory | Path | Description |
|-----------|------|-------------|
| integration | `/tests/integration` | Integration tests using DuckDB |
| benchmarks | `/tests/benchmarks` | Performance benchmarks |

**Files to Move**:
- `/benchmark_test.go` → `/tests/benchmarks/benchmark_test.go`
- Requires refactoring to use DuckDB as client

---

### 4. Go Workspace

**File**: `go.work` (NOT committed)
**Location**: Repository root

**Purpose**: Local development coordination.

**Content**:
```go
go 1.25

use (
    .
    ./examples
    ./tests
)
```

**Lifecycle**:
- Created by developers: `go work init . ./examples ./tests`
- Added to `.gitignore`
- Synced before commits: `go work sync`

---

## Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                      go.work (local only)                    │
│   coordinates development across all modules                 │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Main Module   │  │ Examples Module │  │  Tests Module   │
│  airport-go     │  │  examples/      │  │   tests/        │
│                 │  │                 │  │                 │
│ • Core library  │  │ • 8 examples    │  │ • Integration   │
│ • Minimal deps  │  │ • Imports main  │  │ • Benchmarks    │
│ • NO DuckDB     │  │ • NO DuckDB     │  │ • HAS DuckDB    │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         ▲                    │                    │
         │                    │                    │
         └────────────────────┴────────────────────┘
                      imports
```

---

## File Manifest

### Files to Create

| File | Location | Description |
|------|----------|-------------|
| go.mod | /examples/go.mod | Examples module definition |
| go.mod | /tests/go.mod | Tests module definition |
| go.work | / (not committed) | Workspace file |
| .gitignore entry | / | Add go.work to ignore |
| README.md | /docs/README.md | Documentation index |
| protocol.md | /docs/protocol.md | Protocol overview |
| api-guide.md | /docs/api-guide.md | API reference |
| implementation.md | /docs/implementation.md | Implementation guide |

### Files to Move

| From | To | Notes |
|------|-----|-------|
| /benchmark_test.go | /tests/benchmarks/benchmark_test.go | Refactor for DuckDB |
| /tests/integration/*.go | Keep in place | Now under tests module |

### Files to Modify

| File | Changes |
|------|---------|
| /go.mod | Remove DuckDB dependency |
| /README.md | Update structure, link to docs/ |
| /CLAUDE.md | Update with new structure |

### Files to Delete

| File | Reason |
|------|--------|
| /tests/unit/ | Empty directory |
| /tests/benchmarks/ (empty) | Will be recreated with content |

---

## Validation Rules

1. **Main module**: `go mod tidy` must NOT add DuckDB
2. **Examples module**: Must import and compile against main module
3. **Tests module**: Must have DuckDB for integration tests
4. **All modules**: Same Go version (1.25+)
5. **All modules**: Same Arrow-Go version (v18.4.1)
