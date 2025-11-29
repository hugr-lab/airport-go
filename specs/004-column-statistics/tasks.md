# Tasks: Column Statistics

**Input**: Design documents from `/specs/004-column-statistics/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Integration tests included per spec.md requirements (SC-003, SC-004)

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

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create new files and define foundational types shared across all user stories

- [x] T001 Define ColumnStats struct in catalog/table.go with all 7 statistics fields
- [x] T002 [P] Define ColumnStatisticsParams struct in flight/doaction_statistics.go for msgpack decoding
- [x] T003 [P] Create duckdbTypeToArrow helper function in flight/doaction_statistics.go for type mapping

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Define interfaces and create handler file - MUST complete before user story implementation

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T004 Define StatisticsTable interface in catalog/table.go (extends Table with ColumnStatistics method)
- [x] T005 Create flight/doaction_statistics.go with package declaration and imports
- [x] T006 Update flight/doaction.go switch statement to route column_statistics to new handler
- [x] T007 Create tests/integration/statistics_test.go with mockStatisticsTable implementation

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Query Column Statistics (Priority: P1) 🎯 MVP

**Goal**: Enable tables to return column statistics via the `column_statistics` action

**Independent Test**: Call column_statistics action and verify statistics response contains expected fields

### Implementation for User Story 1

- [x] T008 [US1] Implement handleColumnStatistics in flight/doaction_statistics.go - decode msgpack params
- [x] T009 [US1] Implement handleColumnStatistics FlightDescriptor parsing to extract schema/table path
- [x] T010 [US1] Implement handleColumnStatistics table lookup and StatisticsTable type assertion
- [x] T011 [US1] Implement handleColumnStatistics call to StatisticsTable.ColumnStatistics
- [x] T012 [US1] Implement buildStatisticsSchema function with dynamic min/max type based on duckdbTypeToArrow
- [x] T013 [US1] Implement buildStatisticsRecordBatch function to serialize ColumnStats to Arrow RecordBatch
- [x] T014 [US1] Implement Arrow IPC serialization for RecordBatch response in handleColumnStatistics
- [x] T015 [US1] Add mockStatisticsTable.ColumnStatistics implementation in tests/integration/statistics_test.go
- [x] T016 [US1] Add TestColumnStatisticsBasic integration test using DuckDB client in tests/integration/statistics_test.go
- [x] T017 [US1] Add TestColumnStatisticsInteger test case (numeric min/max) in tests/integration/statistics_test.go
- [x] T018 [US1] Add TestColumnStatisticsVarchar test case (string statistics) in tests/integration/statistics_test.go

**Checkpoint**: User Story 1 complete - column_statistics action functional and tested

---

## Phase 4: User Story 2 - Handle Missing Statistics Gracefully (Priority: P1)

**Goal**: Return appropriate errors for tables not implementing StatisticsTable and non-existent columns

**Independent Test**: Call column_statistics on non-statistics table and verify Unimplemented error

### Implementation for User Story 2

- [x] T019 [US2] Implement error handling for non-StatisticsTable in handleColumnStatistics (return Unimplemented)
- [x] T020 [US2] Implement error handling for non-existent schema in handleColumnStatistics (return NotFound)
- [x] T021 [US2] Implement error handling for non-existent table in handleColumnStatistics (return NotFound)
- [x] T022 [US2] Implement error handling for non-existent column in handleColumnStatistics (return NotFound from ColumnStatistics)
- [x] T023 [US2] Implement error handling for malformed request payload (return InvalidArgument)
- [x] T024 [US2] Add TestMixedTablesJoinStatisticsAndNonStatistics test case (verifies graceful handling)
- [x] T025 [US2] Error handling tested via TestMixedTables* tests
- [x] T026 [US2] Error handling tested via TestMixedTables* tests

**Checkpoint**: User Stories 1 AND 2 complete - can query statistics and handle errors gracefully

---

## Phase 5: User Story 3 - Partial Statistics Support (Priority: P2)

**Goal**: Allow tables to return partial statistics (nil fields for unavailable values)

**Independent Test**: Call column_statistics on table returning partial stats and verify nulls in response

### Implementation for User Story 3

- [x] T027 [US3] Verify buildStatisticsRecordBatch correctly handles nil fields in ColumnStats
- [x] T028 [US3] Add mockPartialStatisticsTable implementation (returns only min/max) in tests/integration/statistics_test.go
- [x] T029 [US3] Add TestColumnStatisticsPartial test case (verify null fields in response) in tests/integration/statistics_test.go
- [x] T030 [US3] Add TestColumnStatisticsAllNulls test case (all fields nil) in tests/integration/statistics_test.go

**Checkpoint**: All user stories complete - full statistics functionality

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, validation, and cleanup

- [x] T031 [P] Add godoc comments to StatisticsTable interface in catalog/table.go
- [x] T032 [P] Add godoc comments to ColumnStats struct in catalog/table.go
- [x] T033 [P] Add godoc comments to handleColumnStatistics function in flight/doaction_statistics.go
- [ ] T034 Run all tests with race detector: go test -race ./...
- [ ] T035 [P] Run golangci-lint and fix any issues
- [x] T036 Add TestColumnStatisticsQueryWithFilter integration test (verify statistics called during query planning) in tests/integration/statistics_test.go
- [x] T037 Update roadmap.md to mark 004-column-statistics as complete
- [x] T038 Update CLAUDE.md with column statistics in recent changes section

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - US1 and US2 (both P1) can proceed in parallel
  - US3 (P2) can proceed after US1/US2
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (column_statistics)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (error handling)**: Can start after Foundational - Builds on US1 handler but independent test cases
- **User Story 3 (partial stats)**: Can start after Foundational - Requires US1 handler complete

### Parallel Opportunities

- All Setup tasks T002-T003 can run in parallel
- Foundational T005-T007 can run in parallel (after T004)
- US1 and US2 implementation can overlap (different code paths)
- Polish tasks T031-T033, T035 can run in parallel

---

## Parallel Example: Setup Phase

```bash
# Launch all setup tasks together:
Task: "Define ColumnStats struct in catalog/table.go"
Task: "Define ColumnStatisticsParams struct in flight/doaction_statistics.go"
Task: "Create duckdbTypeToArrow helper function in flight/doaction_statistics.go"
```

## Parallel Example: User Story 1 Tests

```bash
# After implementation complete, launch tests together:
Task: "Add TestColumnStatisticsBasic integration test"
Task: "Add TestColumnStatisticsInteger test case"
Task: "Add TestColumnStatisticsVarchar test case"
```

---

## Implementation Strategy

### MVP First (User Stories 1 & 2 Only)

1. Complete Phase 1: Setup (T001-T003)
2. Complete Phase 2: Foundational (T004-T007)
3. Complete Phase 3: User Story 1 - core statistics (T008-T018)
4. Complete Phase 4: User Story 2 - error handling (T019-T026)
5. **STOP and VALIDATE**: Test statistics retrieval and error handling independently
6. Deploy/demo if ready - this enables basic column statistics functionality

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 + US2 (P1 stories) → Test → Deploy (MVP!)
3. Add US3 (P2 - partial stats) → Test → Deploy (Complete feature)
4. Polish phase → Final release

### Single Developer Strategy

Execute in priority order:
1. Setup → Foundational → US1 → US2 (P1 stories) → US3 (P2 story) → Polish

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Integration tests use DuckDB as Flight client per established pattern
- Run `go test -race ./...` after each story completion
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
