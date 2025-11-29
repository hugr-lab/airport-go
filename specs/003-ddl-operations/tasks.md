# Tasks: DDL Operations

**Input**: Design documents from `/specs/003-ddl-operations/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Integration tests included per spec.md requirements (SC-006, SC-007)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Based on plan.md project structure:
- **catalog/**: Interface definitions and types
- **flight/**: DoAction handlers
- **tests/integration/**: DuckDB-based integration tests
- **examples/**: Example code

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create new files and define foundational types shared across all user stories

- [ ] T001 Create catalog/dynamic.go with sentinel errors (ErrAlreadyExists, ErrNotFound, ErrSchemaNotEmpty)
- [ ] T002 [P] Define OnConflict type and constants in catalog/dynamic.go
- [ ] T003 [P] Define CreateSchemaOptions struct in catalog/dynamic.go
- [ ] T004 [P] Define DropSchemaOptions struct in catalog/dynamic.go
- [ ] T005 [P] Define CreateTableOptions struct in catalog/dynamic.go
- [ ] T006 [P] Define DropTableOptions struct in catalog/dynamic.go
- [ ] T007 [P] Define AddColumnOptions struct in catalog/dynamic.go
- [ ] T008 [P] Define RemoveColumnOptions struct in catalog/dynamic.go

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Define interfaces and create handler file - MUST complete before user story implementation

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T009 Define DynamicCatalog interface in catalog/dynamic.go (extends Catalog with CreateSchema, DropSchema)
- [ ] T010 Define DynamicSchema interface in catalog/dynamic.go (extends Schema with CreateTable, DropTable)
- [ ] T011 Define DynamicTable interface in catalog/dynamic.go (extends Table with AddColumn, RemoveColumn)
- [ ] T012 Create flight/doaction_ddl.go with package declaration and imports
- [ ] T013 [P] Define msgpack request parameter structs in flight/doaction_ddl.go (CreateSchemaParams, DropSchemaParams, CreateTableParams, DropTableParams, AddColumnParams, RemoveColumnParams)
- [ ] T014 Update flight/doaction.go switch statement to route create_schema, drop_schema to new handlers
- [ ] T015 Update flight/doaction.go switch statement to route create_table, drop_table to new handlers
- [ ] T016 Update flight/doaction.go switch statement to route add_column, remove_column to new handlers
- [ ] T017 Create tests/integration/dynamic_catalog_test.go with mockDynamicCatalog, mockDynamicSchema, mockDynamicTable implementations

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Create Schema Dynamically (Priority: P1) 🎯 MVP

**Goal**: Enable dynamic schema creation via create_schema action

**Independent Test**: Call create_schema action and verify schema appears in list_schemas

### Implementation for User Story 1

- [ ] T018 [US1] Implement handleCreateSchema in flight/doaction_ddl.go - decode msgpack params
- [ ] T019 [US1] Implement handleCreateSchema DynamicCatalog type assertion and error for non-dynamic
- [ ] T020 [US1] Implement handleCreateSchema validation (empty schema name returns InvalidArgument)
- [ ] T021 [US1] Implement handleCreateSchema call to DynamicCatalog.CreateSchema with options
- [ ] T022 [US1] Implement handleCreateSchema response serialization (AirportSerializedContentsWithSHA256Hash)
- [ ] T023 [US1] Implement handleCreateSchema error mapping (ErrAlreadyExists to codes.AlreadyExists)
- [ ] T024 [US1] Add mockDynamicCatalog.CreateSchema implementation in tests/integration/dynamic_catalog_test.go
- [ ] T025 [US1] Add TestDDLCreateSchema integration test using DuckDB client in tests/integration/ddl_test.go
- [ ] T026 [US1] Add TestDDLCreateSchemaAlreadyExists test case in tests/integration/ddl_test.go
- [ ] T027 [US1] Add TestDDLCreateSchemaOnStaticCatalog test case in tests/integration/ddl_test.go

**Checkpoint**: User Story 1 complete - create_schema action functional and tested

---

## Phase 4: User Story 2 - Create Table Dynamically (Priority: P1)

**Goal**: Enable dynamic table creation via create_table action

**Independent Test**: Call create_table with Arrow schema and verify table appears in schema queries

### Implementation for User Story 2

- [ ] T028 [US2] Implement handleCreateTable in flight/doaction_ddl.go - decode msgpack params
- [ ] T029 [US2] Implement handleCreateTable schema lookup and DynamicSchema type assertion
- [ ] T030 [US2] Implement handleCreateTable Arrow schema deserialization using flight.DeserializeSchema
- [ ] T031 [US2] Implement handleCreateTable on_conflict parameter handling (error/ignore/replace)
- [ ] T032 [US2] Implement handleCreateTable call to DynamicSchema.CreateTable with options
- [ ] T033 [US2] Implement handleCreateTable FlightInfo response generation
- [ ] T034 [US2] Implement handleCreateTable error mapping (NotFound, AlreadyExists, InvalidArgument)
- [ ] T035 [US2] Add mockDynamicSchema.CreateTable implementation in tests/integration/dynamic_catalog_test.go
- [ ] T036 [US2] Add TestDDLCreateTable integration test using DuckDB client in tests/integration/ddl_test.go
- [ ] T037 [US2] Add TestDDLCreateTableOnConflictIgnore test case in tests/integration/ddl_test.go
- [ ] T038 [US2] Add TestDDLCreateTableOnConflictReplace test case in tests/integration/ddl_test.go

**Checkpoint**: User Stories 1 AND 2 complete - can create schemas and tables

---

## Phase 5: User Story 3 - Drop Schema (Priority: P2)

**Goal**: Enable schema deletion via drop_schema action

**Independent Test**: Create schema, drop it, verify no longer in list_schemas

### Implementation for User Story 3

- [ ] T039 [US3] Implement handleDropSchema in flight/doaction_ddl.go - decode msgpack params
- [ ] T040 [US3] Implement handleDropSchema DynamicCatalog type assertion
- [ ] T041 [US3] Implement handleDropSchema ignore_not_found parameter handling
- [ ] T042 [US3] Implement handleDropSchema call to DynamicCatalog.DropSchema
- [ ] T043 [US3] Implement handleDropSchema error mapping (NotFound, SchemaNotEmpty to FailedPrecondition)
- [ ] T044 [US3] Add mockDynamicCatalog.DropSchema implementation with table count check in tests/integration/dynamic_catalog_test.go
- [ ] T045 [US3] Add TestDDLDropSchema integration test in tests/integration/ddl_test.go
- [ ] T046 [US3] Add TestDDLDropSchemaNotFound test case in tests/integration/ddl_test.go
- [ ] T047 [US3] Add TestDDLDropSchemaWithTables test case (expects error) in tests/integration/ddl_test.go

**Checkpoint**: User Story 3 complete - full schema lifecycle (create/drop)

---

## Phase 6: User Story 4 - Drop Table (Priority: P2)

**Goal**: Enable table deletion via drop_table action

**Independent Test**: Create table, drop it, verify no longer in table listings

### Implementation for User Story 4

- [ ] T048 [US4] Implement handleDropTable in flight/doaction_ddl.go - decode msgpack params
- [ ] T049 [US4] Implement handleDropTable schema lookup and DynamicSchema type assertion
- [ ] T050 [US4] Implement handleDropTable ignore_not_found parameter handling
- [ ] T051 [US4] Implement handleDropTable call to DynamicSchema.DropTable
- [ ] T052 [US4] Implement handleDropTable error mapping (NotFound, Unimplemented)
- [ ] T053 [US4] Add mockDynamicSchema.DropTable implementation in tests/integration/dynamic_catalog_test.go
- [ ] T054 [US4] Add TestDDLDropTable integration test in tests/integration/ddl_test.go
- [ ] T055 [US4] Add TestDDLDropTableNotFound test case in tests/integration/ddl_test.go

**Checkpoint**: User Stories 1-4 complete - full schema and table lifecycle

---

## Phase 7: User Story 5 - Add Column to Table (Priority: P3)

**Goal**: Enable adding columns to tables via add_column action

**Independent Test**: Create table, add column, verify column in schema

### Implementation for User Story 5

- [ ] T056 [US5] Implement handleAddColumn in flight/doaction_ddl.go - decode msgpack params
- [ ] T057 [US5] Implement handleAddColumn table lookup and DynamicTable type assertion
- [ ] T058 [US5] Implement handleAddColumn column schema deserialization and validation (single field)
- [ ] T059 [US5] Implement handleAddColumn if_column_not_exists parameter handling
- [ ] T060 [US5] Implement handleAddColumn call to DynamicTable.AddColumn
- [ ] T061 [US5] Implement handleAddColumn FlightInfo response with updated schema
- [ ] T062 [US5] Add mockDynamicTable.AddColumn implementation in tests/integration/dynamic_catalog_test.go
- [ ] T063 [US5] Add TestDDLAddColumn integration test in tests/integration/ddl_test.go
- [ ] T064 [US5] Add TestDDLAddColumnAlreadyExists test case in tests/integration/ddl_test.go

**Checkpoint**: User Story 5 complete - can modify table schema by adding columns

---

## Phase 8: User Story 6 - Remove Column from Table (Priority: P3)

**Goal**: Enable removing columns from tables via remove_column action

**Independent Test**: Create table with columns, remove one, verify column gone

### Implementation for User Story 6

- [ ] T065 [US6] Implement handleRemoveColumn in flight/doaction_ddl.go - decode msgpack params
- [ ] T066 [US6] Implement handleRemoveColumn table lookup and DynamicTable type assertion
- [ ] T067 [US6] Implement handleRemoveColumn if_column_exists and cascade parameter handling
- [ ] T068 [US6] Implement handleRemoveColumn call to DynamicTable.RemoveColumn
- [ ] T069 [US6] Implement handleRemoveColumn FlightInfo response with updated schema
- [ ] T070 [US6] Add mockDynamicTable.RemoveColumn implementation in tests/integration/dynamic_catalog_test.go
- [ ] T071 [US6] Add TestDDLRemoveColumn integration test in tests/integration/ddl_test.go
- [ ] T072 [US6] Add TestDDLRemoveColumnNotFound test case in tests/integration/ddl_test.go

**Checkpoint**: All user stories complete - full DDL lifecycle functional

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, validation, and cleanup

- [ ] T073 [P] Add godoc comments to all interfaces in catalog/dynamic.go
- [ ] T074 [P] Add godoc comments to all handler functions in flight/doaction_ddl.go
- [ ] T075 Run all tests with race detector: go test -race ./...
- [ ] T076 [P] Run golangci-lint and fix any issues
- [ ] T077 Add TestDDLFullLifecycle integration test (create schema, create table, add column, remove column, drop table, drop schema) in tests/integration/ddl_test.go
- [ ] T078 Update roadmap.md to mark 003-ddl-operations as complete
- [ ] T079 Update CLAUDE.md with DDL operations in recent changes section

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-8)**: All depend on Foundational phase completion
  - US1 and US2 (both P1) can proceed in parallel
  - US3 and US4 (both P2) can proceed in parallel after US1/US2
  - US5 and US6 (both P3) can proceed in parallel after US3/US4
- **Polish (Phase 9)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (create_schema)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (create_table)**: Can start after Foundational - Requires schema to exist but independent test setup
- **User Story 3 (drop_schema)**: Can start after Foundational - Requires US1 for full test but handler independent
- **User Story 4 (drop_table)**: Can start after Foundational - Requires US2 for full test but handler independent
- **User Story 5 (add_column)**: Can start after Foundational - Requires table to exist
- **User Story 6 (remove_column)**: Can start after Foundational - Requires table with columns

### Parallel Opportunities

- All Setup tasks T002-T008 can run in parallel
- All Foundational param struct definitions in T013 can be done together
- US1 and US2 implementation can run in parallel (different actions)
- US3 and US4 implementation can run in parallel (different actions)
- US5 and US6 implementation can run in parallel (different actions)
- Polish tasks T073, T074, T076 can run in parallel

---

## Parallel Example: Setup Phase

```bash
# Launch all option struct definitions together:
Task: "Define CreateSchemaOptions struct in catalog/dynamic.go"
Task: "Define DropSchemaOptions struct in catalog/dynamic.go"
Task: "Define CreateTableOptions struct in catalog/dynamic.go"
Task: "Define DropTableOptions struct in catalog/dynamic.go"
Task: "Define AddColumnOptions struct in catalog/dynamic.go"
Task: "Define RemoveColumnOptions struct in catalog/dynamic.go"
```

## Parallel Example: User Stories

```bash
# After Foundational complete, launch US1 and US2 together:
Task: "Implement handleCreateSchema in flight/doaction_ddl.go"
Task: "Implement handleCreateTable in flight/doaction_ddl.go"
```

---

## Implementation Strategy

### MVP First (User Stories 1 & 2 Only)

1. Complete Phase 1: Setup (T001-T008)
2. Complete Phase 2: Foundational (T009-T017)
3. Complete Phase 3: User Story 1 - create_schema (T018-T027)
4. Complete Phase 4: User Story 2 - create_table (T028-T038)
5. **STOP and VALIDATE**: Test schema and table creation independently
6. Deploy/demo if ready - this enables basic DDL functionality

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 + US2 (create operations) → Test → Deploy (MVP!)
3. Add US3 + US4 (drop operations) → Test → Deploy (Full lifecycle)
4. Add US5 + US6 (column operations) → Test → Deploy (Complete feature)
5. Polish phase → Final release

### Single Developer Strategy

Execute in priority order:
1. Setup → Foundational → US1 → US2 (P1 stories) → US3 → US4 (P2 stories) → US5 → US6 (P3 stories) → Polish

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Integration tests use DuckDB as Flight client per established pattern
- Run `go test -race ./...` after each story completion
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
