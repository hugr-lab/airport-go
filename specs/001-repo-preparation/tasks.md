# Tasks: Repository Preparation for Production Use

**Input**: Design documents from `/specs/001-repo-preparation/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `- [ ] [ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

This is a Go project with the following structure:
- **Root package**: `airport-go/` (core types, root-level tests)
- **Subpackages**: `auth/`, `catalog/`, `flight/`, `internal/`
- **Examples**: `examples/basic/`, `examples/auth/`, `examples/dynamic/`
- **Tests**: `tests/integration/` (integration tests), `*_test.go` (unit tests)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create necessary infrastructure and prepare repository structure

- [X] T001 Create tests/integration/ directory for integration tests
- [X] T002 [P] Create .github/workflows/ directory for CI configuration
- [X] T003 [P] Verify all existing packages compile with current Go version

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 Implement GeometryExtensionType in catalog/geometry.go for WKB geospatial support
- [X] T005 [P] Add geometry validation functions in catalog/geometry.go
- [X] T006 [P] Create geometry encoding/decoding utilities using github.com/paulmach/orb in catalog/geometry.go
- [X] T007 [P] Add unit tests for geometry types in catalog/geometry_test.go

**Checkpoint**: Foundation ready - geometry types available, user story implementation can now begin

---

## Phase 3: User Story 1 - Clean Repository Structure (Priority: P1) 🎯 MVP

**Goal**: Well-organized repository with tests in conventional locations and no unused directories

**Independent Test**: Clone repository, verify integration tests exist in tests/integration/, confirm documentation reflects current structure

### Implementation for User Story 1

- [X] T008 [P] [US1] Move integration_auth_test.go to tests/integration/auth_test.go
- [X] T009 [P] [US1] Move integration_catalog_test.go to tests/integration/catalog_test.go
- [X] T010 [P] [US1] Move integration_dynamic_test.go to tests/integration/dynamic_test.go
- [X] T011 [P] [US1] Move integration_functions_test.go to tests/integration/functions_test.go
- [X] T012 [P] [US1] Move integration_query_test.go to tests/integration/query_test.go
- [X] T013 [P] [US1] Move integration_test.go to tests/integration/integration_test.go
- [X] T014 [US1] Update import paths in moved test files
- [X] T015 [US1] Verify all tests pass in new location with GOWORK=off go test ./tests/integration/...
- [X] T016 [US1] Update README.md to document tests/integration/ location
- [X] T017 [US1] Update documentation to reflect current repository structure

**Checkpoint**: Integration tests successfully relocated, documentation accurate

---

## Phase 4: User Story 2 - Automated Quality Assurance (Priority: P1)

**Goal**: Automated linting and testing through GitHub Actions for every PR

**Independent Test**: Create test PR, verify GitHub Actions runs automatically, check linting and tests execute

### Implementation for User Story 2

- [X] T018 [US2] Create .github/workflows/ci.yml with workflow configuration
- [X] T019 [US2] Add lint job using golangci/golangci-lint-action@v4 in ci.yml
- [X] T020 [US2] Add test job with go test -v -race ./... in ci.yml
- [X] T021 [US2] Configure job timeout-minutes: 10 for both jobs in ci.yml
- [X] T022 [US2] Add actions/checkout@v4 and actions/setup-go@v5 steps
- [X] T023 [US2] Configure Go version '1.21' and cache: true in setup-go
- [X] T024 [US2] Test workflow locally or on branch to verify execution
- [X] T025 [US2] Create .golangci.yml if needed for linter configuration

**Checkpoint**: CI workflow runs automatically on PRs, validates code quality

---

## Phase 5: User Story 3 - Working Examples with DuckDB Client (Priority: P2)

**Goal**: Working examples showing DuckDB client connections and queries

**Independent Test**: Run each example, verify DuckDB client connects successfully, execute sample queries

### Implementation for User Story 3

- [X] T026 [P] [US3] Create examples/basic/client.sql with DuckDB connection and SELECT query
- [X] T027 [P] [US3] Create examples/auth/client.sql with DuckDB auth and protected query
- [X] T028 [P] [US3] Create examples/dynamic/client.sql with dynamic catalog query
- [X] T029 [P] [US3] Update examples/basic/README.md with DuckDB 1.4+ installation instructions
- [X] T030 [P] [US3] Update examples/auth/README.md with Airport extension setup
- [X] T031 [P] [US3] Update examples/dynamic/README.md with running instructions
- [X] T032 [US3] Test basic example: start server, run duckdb < client.sql
- [X] T033 [US3] Test auth example: verify bearer token authentication works
- [X] T034 [US3] Test dynamic example: verify dynamic catalog connection

**Checkpoint**: All three examples run successfully with DuckDB client

---

## Phase 6: User Story 4 - DDL Operations Support (Priority: P2)

**Goal**: Create and manage schemas and tables through Airport server

**Independent Test**: Execute CREATE SCHEMA, CREATE TABLE, ALTER TABLE, DROP TABLE, DROP SCHEMA; verify catalog state changes

### Implementation for User Story 4

- [X] T035 [P] [US4] Define DDL action payloads in flight/ddl_types.go (CreateSchemaAction, DropSchemaAction, CreateTableAction, etc.)
- [X] T036 [P] [US4] Create flight/ddl.go with DoAction handler skeleton
- [X] T037 [US4] Implement handleCreateSchema in flight/ddl.go with IF NOT EXISTS support
- [X] T038 [US4] Implement handleDropSchema in flight/ddl.go with IF EXISTS and CASCADE support
- [X] T039 [US4] Implement handleCreateTable in flight/ddl.go with Arrow schema parsing
- [X] T040 [US4] Implement handleDropTable in flight/ddl.go with IF EXISTS support
- [X] T041 [US4] Implement handleAlterTableAddColumn in flight/ddl.go
- [X] T042 [US4] Implement handleAlterTableDropColumn in flight/ddl.go
- [X] T043 [US4] Add DDL action type routing in DoAction handler in flight/server.go
- [X] T044 [P] [US4] Create flight/ddl_test.go with unit tests for action parsing
- [X] T045 [P] [US4] Add integration test for CREATE SCHEMA in tests/integration/ddl_test.go
- [X] T046 [P] [US4] Add integration test for CREATE TABLE with geometry column in tests/integration/ddl_test.go
- [X] T047 [P] [US4] Add integration test for IF NOT EXISTS idempotency in tests/integration/ddl_test.go
- [X] T048 [US4] Add integration test for ALTER TABLE operations in tests/integration/ddl_test.go
- [X] T049 [US4] Add error handling tests for invalid schemas in tests/integration/ddl_test.go

**Checkpoint**: DDL operations fully functional, catalog management works

---

## Phase 7: User Story 5 - DML Operations Support (Priority: P2)

**Goal**: Insert, update, and delete data through Airport server

**Independent Test**: Execute INSERT with Arrow batches, UPDATE using rowid, DELETE using rowid; verify data changes

### Implementation for User Story 5

- [X] T050 [P] [US5] Define DML descriptor types in flight/dml_types.go (InsertDescriptor, UpdateDescriptor, DeleteAction)
- [X] T051 [P] [US5] Create flight/dml.go with DoPut handler skeleton
- [X] T052 [US5] Implement handleInsert in flight/dml.go with Arrow RecordBatch streaming
- [X] T053 [US5] Implement handleUpdate in flight/dml.go with rowid validation
- [X] T054 [US5] Implement handleDelete in flight/dml.go (via DoAction)
- [X] T055 [US5] Add schema compatibility validation in flight/dml.go
- [X] T056 [US5] Add affected row count reporting in DML operations
- [X] T057 [US5] Wire DoPut handler in flight/server.go
- [X] T058 [P] [US5] Create flight/dml_test.go with unit tests for descriptors
- [X] T059 [P] [US5] Add integration test for INSERT in tests/integration/dml_test.go
- [X] T060 [P] [US5] Add integration test for UPDATE with rowid in tests/integration/dml_test.go
- [X] T061 [P] [US5] Add integration test for DELETE with rowid in tests/integration/dml_test.go
- [X] T062 [P] [US5] Add integration test for INSERT with geometry data in tests/integration/dml_test.go
- [X] T063 [US5] Add error handling tests for schema mismatch in tests/integration/dml_test.go

**Checkpoint**: DML operations fully functional, data lifecycle management complete

---

## Phase 8: User Story 6 - Point-in-Time Query Support (Priority: P3)

**Goal**: Query historical states of data using point-in-time parameters

**Independent Test**: Execute queries with ts and ts_ns parameters, verify historical data returned

### Implementation for User Story 6

- [X] T064 [US6] Extend catalog.ScanOptions with Timestamp and TimestampNs fields in catalog/types.go
- [X] T065 [US6] Update ticket parsing in flight/ticket.go to extract ts and ts_ns parameters
- [X] T066 [US6] Pass timestamp parameters to catalog.Scan in flight/doget.go
- [X] T067 [P] [US6] Add unit tests for timestamp parameter extraction in flight/ticket_test.go
- [X] T068 [P] [US6] Add integration test for ts parameter queries in tests/integration/timetravel_test.go
- [X] T069 [P] [US6] Add integration test for ts_ns parameter queries in tests/integration/timetravel_test.go
- [X] T070 [US6] Add integration test for queries without time parameters in tests/integration/timetravel_test.go
- [ ] T071 [US6] Document time-travel limitations in examples/README.md

**Checkpoint**: Point-in-time queries functional, temporal analysis enabled

---

## Phase 9: User Story 7 - Endpoint Discovery and Management (Priority: P3)

**Goal**: Discover and manage Flight endpoints for distributed query routing

**Independent Test**: Call flight_info, endpoints, table_function_flight_info actions; verify metadata returned

### Implementation for User Story 7

- [ ] T072 [P] [US7] Create flight/endpoints.go with endpoint discovery handlers
- [ ] T073 [US7] Implement handleFlightInfo action returning FlightInfo with schema and endpoints
- [ ] T074 [US7] Implement handleListEndpoints action returning endpoint list
- [ ] T075 [US7] Implement handleTableFunctionFlightInfo action for parameterized queries
- [ ] T076 [US7] Add endpoint action routing in DoAction handler in flight/server.go
- [ ] T077 [P] [US7] Add unit tests for FlightInfo serialization in flight/endpoints_test.go
- [ ] T078 [P] [US7] Add integration test for flight_info action in tests/integration/endpoints_test.go
- [ ] T079 [P] [US7] Add integration test for endpoints action in tests/integration/endpoints_test.go
- [ ] T080 [US7] Add integration test for table_function_flight_info in tests/integration/endpoints_test.go

**Checkpoint**: Endpoint discovery fully functional, distributed routing enabled

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T081 [P] Update root README.md with comprehensive feature documentation
- [ ] T082 [P] Add CONTRIBUTING.md with development guidelines
- [ ] T083 [P] Add examples/README.md with overview of all examples
- [ ] T084 [P] Review and update godoc comments for all public APIs
- [ ] T085 [P] Add performance benchmarks for DDL operations in benchmark_test.go
- [ ] T086 [P] Add performance benchmarks for DML operations in benchmark_test.go
- [ ] T087 Verify all tests pass with race detector: GOWORK=off go test -race ./...
- [ ] T088 Run quickstart.md validation scenarios
- [ ] T089 Final code cleanup and formatting with gofmt and golangci-lint

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational (Phase 2) - Can proceed independently
- **User Story 2 (Phase 4)**: Depends on Foundational (Phase 2) - Can proceed independently
- **User Story 3 (Phase 5)**: Depends on Foundational (Phase 2) - Can proceed independently
- **User Story 4 (Phase 6)**: Depends on Foundational (Phase 2) - Can proceed independently
- **User Story 5 (Phase 7)**: Depends on Foundational (Phase 2) - Can proceed independently
- **User Story 6 (Phase 8)**: Depends on Foundational (Phase 2) - Can proceed independently
- **User Story 7 (Phase 9)**: Depends on Foundational (Phase 2) - Can proceed independently
- **Polish (Phase 10)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 3 (P2)**: Can start after Foundational - May integrate with examples but independently testable
- **User Story 4 (P2)**: Can start after Foundational - Requires geometry types from Foundational phase
- **User Story 5 (P2)**: Can start after Foundational - Can work with US4 schemas, independently testable
- **User Story 6 (P3)**: Can start after Foundational - Builds on query infrastructure but independently testable
- **User Story 7 (P3)**: Can start after Foundational - Extends Flight protocol but independently testable

### Within Each User Story

- Tests run in parallel when marked [P]
- Models/types before services
- Services before endpoints
- Core implementation before integration tests
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks (Phase 1) marked [P] can run in parallel
- All Foundational tasks (Phase 2) marked [P] can run in parallel
- Once Foundational phase completes, P1 user stories (US1, US2) can start in parallel
- After P1 completes, P2 user stories (US3, US4, US5) can start in parallel
- After P2 completes, P3 user stories (US6, US7) can start in parallel
- Within each user story, tasks marked [P] can run in parallel

---

## Parallel Example: User Story 4 (DDL Operations)

```bash
# Launch all type definitions and test files in parallel:
Task T035: "Define DDL action payloads in flight/ddl_types.go"
Task T036: "Create flight/ddl.go with DoAction handler skeleton"

# After skeleton is ready, launch all integration tests in parallel:
Task T045: "Add integration test for CREATE SCHEMA"
Task T046: "Add integration test for CREATE TABLE with geometry column"
Task T047: "Add integration test for IF NOT EXISTS idempotency"
```

---

## Parallel Example: User Story 5 (DML Operations)

```bash
# Launch type definitions and test files in parallel:
Task T050: "Define DML descriptor types in flight/dml_types.go"
Task T051: "Create flight/dml.go with DoPut handler skeleton"

# Launch all integration tests in parallel:
Task T059: "Add integration test for INSERT"
Task T060: "Add integration test for UPDATE with rowid"
Task T061: "Add integration test for DELETE with rowid"
Task T062: "Add integration test for INSERT with geometry data"
```

---

## Implementation Strategy

### MVP First (User Stories 1 & 2 Only - Both P1)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - geometry types needed)
3. Complete Phase 3: User Story 1 (Clean Repository Structure)
4. Complete Phase 4: User Story 2 (Automated Quality Assurance)
5. **STOP and VALIDATE**: Test US1 and US2 independently
6. Deploy/demo if ready

**Delivers**: Clean repository with automated CI - ready for feature development

### Incremental Delivery

1. Complete Setup + Foundational → Geometry types ready
2. Add User Story 1 → Test independently → Clean structure validated
3. Add User Story 2 → Test independently → CI automated
4. Add User Story 3 → Test independently → DuckDB examples working
5. Add User Story 4 → Test independently → DDL operations functional
6. Add User Story 5 → Test independently → DML operations functional
7. Add User Story 6 → Test independently → Time-travel enabled
8. Add User Story 7 → Test independently → Endpoint discovery complete
9. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Clean Repository Structure)
   - Developer B: User Story 2 (Automated Quality Assurance)
3. After P1 stories complete:
   - Developer A: User Story 3 (DuckDB Examples)
   - Developer B: User Story 4 (DDL Operations)
   - Developer C: User Story 5 (DML Operations)
4. After P2 stories complete:
   - Developer A: User Story 6 (Point-in-Time Queries)
   - Developer B: User Story 7 (Endpoint Discovery)
5. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies - can run in parallel
- [Story] label (US1-US7) maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Geometry types (Phase 2) are foundational and block DDL/DML work
- DuckDB examples (US3) provide validation for DDL/DML features
- All tests run with race detector via GOWORK=off go test -race
- IF EXISTS / IF NOT EXISTS clauses enable idempotent DDL operations
- Arrow IPC format used for all data exchange (via existing internal/serialize package)
