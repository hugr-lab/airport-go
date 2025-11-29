# Tasks: Module Reorganization

**Input**: Design documents from `/specs/005-module-reorganization/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md

**Tests**: Validation tests included per spec.md requirements (verify module isolation, dependency cleanup)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Based on plan.md project structure:
- **Main module**: Repository root (`/`)
- **Examples module**: `/examples/`
- **Tests module**: `/tests/`
- **Documentation**: `/docs/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create workspace configuration and prepare for module separation

- [x] T001 Add go.work to .gitignore in /.gitignore
- [x] T002 [P] Create go.work file at repository root with `go work init . ./examples ./tests`
- [x] T003 [P] Remove empty /tests/unit/ directory
- [x] T004 [P] Remove empty /tests/benchmarks/ directory (will be recreated)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Create module definitions - MUST complete before user story implementation

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T005 Create examples module: /examples/go.mod with module github.com/hugr-lab/airport-go/examples
- [x] T006 Create tests module: /tests/go.mod with module github.com/hugr-lab/airport-go/tests
- [x] T007 Add DuckDB dependency to /tests/go.mod (github.com/duckdb/duckdb-go/v2)
- [x] T008 Add Arrow-Go and gRPC dependencies to /examples/go.mod
- [x] T009 Add Arrow-Go, gRPC dependencies to /tests/go.mod
- [x] T010 Run `go work sync` to synchronize all modules

**Checkpoint**: Foundation ready - all three modules exist with proper dependencies

---

## Phase 3: User Story 1 - Isolated Example Development (Priority: P1) 🎯 MVP

**Goal**: Examples compile and run with their own go.mod, isolated from main module

**Independent Test**: `cd examples && go build ./...` succeeds; examples run with `go run .`

### Implementation for User Story 1

- [x] T011 [US1] Update /examples/auth/main.go imports to use examples module path (no change needed - imports main module via replace)
- [x] T012 [P] [US1] Update /examples/basic/main.go imports to use examples module path (no change needed)
- [x] T013 [P] [US1] Update /examples/basic/functions.go imports to use examples module path (no change needed)
- [x] T014 [P] [US1] Update /examples/ddl/main.go imports to use examples module path (no change needed)
- [x] T015 [P] [US1] Update /examples/dml/main.go imports to use examples module path (no change needed)
- [x] T016 [P] [US1] Update /examples/dynamic/main.go imports to use examples module path (no change needed)
- [x] T017 [P] [US1] Update /examples/functions/main.go imports to use examples module path (no change needed)
- [x] T018 [P] [US1] Update /examples/timetravel/main.go imports to use examples module path (no change needed)
- [x] T019 [P] [US1] Update /examples/tls/main.go imports to use examples module path (no change needed)
- [x] T020 [US1] Run `cd examples && go mod tidy` to resolve dependencies
- [x] T021 [US1] Verify `cd examples && go build ./...` succeeds
- [x] T022 [US1] Verify `cd examples/basic && go run .` starts server

**Checkpoint**: User Story 1 complete - examples work independently with their own go.mod

---

## Phase 4: User Story 2 - Independent Integration Testing (Priority: P1)

**Goal**: Integration tests run from tests/ module with DuckDB, isolated from main module

**Independent Test**: `cd tests && go test ./integration/...` runs all integration tests

### Implementation for User Story 2

- [x] T023 [US2] Update /tests/integration/integration_test.go imports to use tests module path (no change needed - imports main module via replace)
- [x] T024 [P] [US2] Update /tests/integration/auth_test.go imports (no change needed)
- [x] T025 [P] [US2] Update /tests/integration/catalog_test.go imports (no change needed)
- [x] T026 [P] [US2] Update /tests/integration/datatypes_test.go imports (no change needed)
- [x] T027 [P] [US2] Update /tests/integration/discovery_test.go imports (no change needed)
- [x] T028 [P] [US2] Update /tests/integration/dml_test.go imports (no change needed)
- [x] T029 [P] [US2] Update /tests/integration/dynamic_test.go imports (no change needed)
- [x] T030 [P] [US2] Update /tests/integration/dynamic_catalog_test.go imports (no change needed)
- [x] T031 [P] [US2] Update /tests/integration/ddl_test.go imports (no change needed)
- [x] T032 [P] [US2] Update /tests/integration/projection_test.go imports (no change needed)
- [x] T033 [P] [US2] Update /tests/integration/query_test.go imports (no change needed)
- [x] T034 [P] [US2] Update /tests/integration/scalar_functions_test.go imports (no change needed)
- [x] T035 [P] [US2] Update /tests/integration/statistics_test.go imports (no change needed)
- [x] T036 [P] [US2] Update /tests/integration/table_functions_test.go imports (no change needed)
- [x] T037 [P] [US2] Update /tests/integration/transaction_test.go imports (no change needed)
- [x] T038 [US2] Run `cd tests && go mod tidy` to resolve dependencies
- [x] T039 [US2] Verify `cd tests && go test ./integration/...` runs all tests

**Checkpoint**: User Story 2 complete - integration tests run independently

---

## Phase 5: User Story 3 - Clean Dependency Tree (Priority: P1)

**Goal**: Main module has no DuckDB dependency, unused packages removed

**Independent Test**: `go mod tidy && go build ./...` succeeds without DuckDB

### Implementation for User Story 3

- [x] T040 [US3] Remove github.com/duckdb/duckdb-go/v2 from /go.mod require section (already absent)
- [x] T041 [US3] Run `go mod tidy` on main module to clean up indirect deps
- [x] T042 [US3] Verify DuckDB-related indirect deps are removed from /go.sum
- [x] T043 [US3] Run `go build ./...` to verify main module compiles without DuckDB
- [x] T044 [US3] Run `go test ./...` to verify unit tests pass (should NOT require DuckDB)
- [x] T045 [US3] Verify /go.mod only contains: arrow-go, grpc, msgpack, compress, orb, uuid, sync, protobuf

**Checkpoint**: User Story 3 complete - main module has minimal, clean dependencies

---

## Phase 6: User Story 4 - Benchmark Suite Development (Priority: P2)

**Goal**: Benchmarks moved to tests/benchmarks/ and refactored to use DuckDB client

**Independent Test**: `cd tests && go test -bench=. ./benchmarks/...` runs benchmarks

### Implementation for User Story 4

- [x] T046 [US4] Create /tests/benchmarks/ directory
- [x] T047 [US4] Move /benchmark_test.go to /tests/benchmarks/benchmark_test.go
- [x] T048 [US4] Update package declaration in /tests/benchmarks/benchmark_test.go to `package benchmarks`
- [x] T049 [US4] Update imports in /tests/benchmarks/benchmark_test.go to use tests module path
- [x] T050 [US4] Refactor BenchmarkCatalogSerialization to use DuckDB as Flight client
- [x] T051 [US4] Refactor BenchmarkTableScan to use DuckDB as Flight client
- [x] T052 [US4] Refactor BenchmarkCatalogBuilder to use DuckDB as Flight client (kept internal, no Flight needed)
- [x] T053 [US4] Refactor BenchmarkRecordBuilding to use DuckDB as Flight client (kept internal, no Flight needed)
- [x] T054 [US4] Refactor BenchmarkConcurrentScans to use DuckDB as Flight client
- [x] T055 [US4] Remove helper types (benchCatalog, benchSchema, benchTable) if no longer needed (kept for serialization benchmark)
- [x] T056 [US4] Run `cd tests && go mod tidy` to include benchmark dependencies
- [x] T057 [US4] Verify `cd tests && go test -bench=. ./benchmarks/...` runs successfully

**Checkpoint**: User Story 4 complete - benchmarks run from tests module with DuckDB

---

## Phase 7: User Story 5 - Comprehensive Protocol Documentation (Priority: P2)

**Goal**: Create docs/ folder with protocol, API, and implementation guides

**Independent Test**: docs/ folder contains README.md, protocol.md, api-guide.md, implementation.md

### Implementation for User Story 5

- [x] T058 [US5] Create /docs/ directory
- [x] T059 [P] [US5] Create /docs/README.md with documentation index and navigation
- [x] T060 [P] [US5] Create /docs/protocol.md with Airport protocol overview (actions, messages, Flight RPC)
- [x] T061 [P] [US5] Create /docs/api-guide.md with public API reference (Catalog, Schema, Table, Dynamic interfaces)
- [x] T062 [P] [US5] Create /docs/implementation.md with guide for implementing custom catalogs
- [x] T063 [US5] Update /README.md to be concise with links to /docs/ for detailed information
- [x] T064 [US5] Reference examples/ in /docs/implementation.md for practical usage patterns

**Checkpoint**: User Story 5 complete - comprehensive documentation available

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation updates, and cleanup

- [x] T065 [P] Update /CLAUDE.md with new module structure and commands
- [x] T066 [P] Update /README.md project structure section to show 3-module layout
- [x] T067 Run `go work sync` to ensure all modules are synchronized
- [x] T068 Run `go mod tidy` in all three modules (main, examples, tests)
- [x] T069 Run `go build ./...` on main module to verify no DuckDB dependency
- [x] T070 Run `cd examples && go build ./...` to verify examples compile
- [x] T071 Run `cd tests && go test ./integration/...` to verify integration tests pass
- [x] T072 Run `cd tests && go test -bench=. ./benchmarks/...` to verify benchmarks run
- [x] T073 Run `golangci-lint run ./...` on all modules to check code quality
- [x] T074 Update roadmap.md to mark 005-module-reorganization as complete
- [x] T075 Update spec.md status from Draft to Complete

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
  - US1, US2, US3 (all P1) can proceed in parallel after Foundational
  - US4, US5 (P2) can proceed after P1 stories or in parallel if capacity allows
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (examples)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (tests)**: Can start after Foundational - No dependencies on other stories
- **User Story 3 (cleanup)**: Can start after US1 and US2 complete (to avoid conflicts)
- **User Story 4 (benchmarks)**: Depends on US2 (tests module must exist first)
- **User Story 5 (docs)**: Can start after Foundational - No dependencies on other stories

### Parallel Opportunities

- Setup tasks T002-T004 can run in parallel
- Foundational T005-T009 can run in parallel (module creation is independent)
- US1 example updates T011-T019 can run in parallel (different files)
- US2 test updates T023-T037 can run in parallel (different files)
- US5 docs T059-T062 can run in parallel (different files)
- Polish tasks T065-T066 can run in parallel

---

## Parallel Example: User Story 1 (Examples)

```bash
# Launch all example updates together:
Task: "Update /examples/auth/main.go imports"
Task: "Update /examples/basic/main.go imports"
Task: "Update /examples/ddl/main.go imports"
Task: "Update /examples/dml/main.go imports"
Task: "Update /examples/dynamic/main.go imports"
Task: "Update /examples/functions/main.go imports"
Task: "Update /examples/timetravel/main.go imports"
Task: "Update /examples/tls/main.go imports"
```

## Parallel Example: User Story 2 (Tests)

```bash
# Launch all test file updates together:
Task: "Update /tests/integration/auth_test.go imports"
Task: "Update /tests/integration/catalog_test.go imports"
Task: "Update /tests/integration/datatypes_test.go imports"
# ... (all 15 test files can be updated in parallel)
```

---

## Implementation Strategy

### MVP First (User Stories 1-3 Only)

1. Complete Phase 1: Setup (T001-T004)
2. Complete Phase 2: Foundational (T005-T010)
3. Complete Phase 3: User Story 1 - Examples module (T011-T022)
4. Complete Phase 4: User Story 2 - Tests module (T023-T039)
5. Complete Phase 5: User Story 3 - Clean dependencies (T040-T045)
6. **STOP and VALIDATE**: All 3 modules work independently, DuckDB isolated from main
7. Deploy/demo if ready - this enables the core reorganization

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 (examples) → Test → Examples work independently
3. Add US2 (tests) → Test → Tests work independently
4. Add US3 (cleanup) → Test → Main module clean (MVP!)
5. Add US4 (benchmarks) → Test → Benchmarks in tests module
6. Add US5 (docs) → Test → Documentation complete
7. Polish phase → Final release

### Single Developer Strategy

Execute in priority order:
1. Setup → Foundational → US1 → US2 → US3 (P1 stories complete) → US4 → US5 → Polish

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Run `go work sync` and `go mod tidy` after each story completion
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
