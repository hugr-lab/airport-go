# Tasks: DML RETURNING Clause Column Selection

**Input**: Design documents from `/specs/006-returning-optimization/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Integration tests are required per FR-005 in spec.md.

**Organization**: Tasks grouped by user story for independent implementation.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (US1, US2, US3)
- Include exact file paths

## Path Conventions

```text
catalog/           # Interface definitions and types
flight/            # Flight server handlers
examples/dml/      # DML example implementation
tests/integration/ # Integration tests with DuckDB
```

---

## Phase 1: Setup

**Purpose**: No additional setup needed - existing Go workspace is configured.

- [x] T001 Verify branch `006-returning-optimization` is checked out and clean

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core types and interface changes that MUST be complete before user stories.

**⚠️ CRITICAL**: All DML handlers depend on these changes.

- [x] T002 Add `DMLOptions` struct to `catalog/types.go` with `Returning bool` and `ReturningColumns []string` fields per data-model.md
- [x] T003 Update `InsertableTable` interface in `catalog/table.go` to add `opts *DMLOptions` parameter to `Insert` method
- [x] T004 Update `UpdatableTable` interface in `catalog/table.go` to add `opts *DMLOptions` parameter to `Update` method
- [x] T005 Update `DeletableTable` interface in `catalog/table.go` to add `opts *DMLOptions` parameter to `Delete` method
- [x] T006 Run `go build ./...` to verify interface changes compile (will fail - implementations not updated yet)

**Checkpoint**: Interface contracts defined - implementations can now be updated.

---

## Phase 3: User Story 1 - INSERT RETURNING Column Selection (Priority: P1) 🎯 MVP

**Goal**: Enable INSERT operations to receive `DMLOptions` with RETURNING information.

**Independent Test**: Execute `INSERT ... RETURNING id` via DuckDB and verify the table receives `DMLOptions{Returning: true, ReturningColumns: [column names]}`.

### Implementation for User Story 1

- [x] T007 [US1] Update `handleDoExchangeInsert` in `flight/doexchange_dml.go` to create `DMLOptions` with `Returning` field set from `returnData` boolean and `ReturningColumns` populated from input schema column names
- [x] T008 [US1] Update `handleDoExchangeInsert` to pass `opts *DMLOptions` to `insertableTable.Insert()` call
- [x] T009 [US1] Update `duckDBDMLTable.Insert` method in `tests/integration/dml_test.go` to accept `opts *DMLOptions` parameter
- [x] T010 [US1] Update `DMLTable.Insert` method in `examples/dml/main.go` to accept `opts *DMLOptions` parameter
- [x] T011 [US1] Run `go build ./...` to verify INSERT path compiles
- [x] T012 [US1] Run existing INSERT integration tests: `go test ./tests/integration/... -run TestDMLInsert -v`

### Integration Tests for User Story 1

- [x] T013 [US1] Add test `TestDMLInsertReturningColumns` in `tests/integration/dml_test.go` to verify `DMLOptions.Returning=true` and `ReturningColumns` contains expected column names when `RETURNING id` is specified
- [x] T014 [US1] Add test case for `RETURNING *` to verify `ReturningColumns` contains all input schema columns
- [x] T015 [US1] Run new INSERT RETURNING tests: `go test ./tests/integration/... -run TestDMLInsertReturning -v`

**Checkpoint**: INSERT with RETURNING clause passes `DMLOptions` to implementation.

---

## Phase 4: User Story 2 - UPDATE RETURNING Column Selection (Priority: P2)

**Goal**: Enable UPDATE operations to receive `DMLOptions` with RETURNING information.

**Independent Test**: Execute `UPDATE ... RETURNING id, name` via DuckDB and verify the table receives `DMLOptions{Returning: true, ReturningColumns: [...]}`.

### Implementation for User Story 2

- [x] T016 [US2] Update `handleDoExchangeUpdate` in `flight/doexchange_dml.go` to create `DMLOptions` with `Returning` field and `ReturningColumns` from input schema
- [x] T017 [US2] Update `handleDoExchangeUpdate` to pass `opts *DMLOptions` to `updatableTable.Update()` call
- [x] T018 [US2] Update `duckDBDMLTable.Update` method in `tests/integration/dml_test.go` to accept `opts *DMLOptions` parameter
- [x] T019 [US2] Update `DMLTable.Update` method in `examples/dml/main.go` to accept `opts *DMLOptions` parameter
- [x] T020 [US2] Run `go build ./...` to verify UPDATE path compiles
- [x] T021 [US2] Run existing UPDATE integration tests: `go test ./tests/integration/... -run TestDMLUpdate -v`

### Integration Tests for User Story 2

- [x] T022 [US2] Add test `TestDMLUpdateReturningColumns` in `tests/integration/dml_test.go` to verify `DMLOptions.Returning=true` and `ReturningColumns` are passed correctly
- [x] T023 [US2] Run new UPDATE RETURNING tests: `go test ./tests/integration/... -run TestDMLUpdateReturning -v`

**Checkpoint**: UPDATE with RETURNING clause passes `DMLOptions` to implementation.

---

## Phase 5: User Story 3 - DELETE RETURNING Column Selection (Priority: P2)

**Goal**: Enable DELETE operations to receive `DMLOptions` with RETURNING information.

**Independent Test**: Execute `DELETE ... RETURNING id` via DuckDB and verify the table receives `DMLOptions{Returning: true, ReturningColumns: [...]}`.

### Implementation for User Story 3

- [x] T024 [US3] Update `handleDoExchangeDelete` in `flight/doexchange_dml.go` to create `DMLOptions` with `Returning` field and `ReturningColumns` from schema (DELETE doesn't have input data columns, use table schema or empty)
- [x] T025 [US3] Update `handleDoExchangeDelete` to pass `opts *DMLOptions` to `deletableTable.Delete()` call
- [x] T026 [US3] Update `duckDBDMLTable.Delete` method in `tests/integration/dml_test.go` to accept `opts *DMLOptions` parameter
- [x] T027 [US3] Update `DMLTable.Delete` method in `examples/dml/main.go` to accept `opts *DMLOptions` parameter
- [x] T028 [US3] Run `go build ./...` to verify DELETE path compiles
- [x] T029 [US3] Run existing DELETE integration tests: `go test ./tests/integration/... -run TestDMLDelete -v`

### Integration Tests for User Story 3

- [x] T030 [US3] Add test `TestDMLDeleteReturningColumns` in `tests/integration/dml_test.go` to verify `DMLOptions.Returning=true` is passed correctly
- [x] T031 [US3] Run new DELETE RETURNING tests: `go test ./tests/integration/... -run TestDMLDeleteReturning -v`

**Checkpoint**: DELETE with RETURNING clause passes `DMLOptions` to implementation.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, cleanup, and validation.

- [x] T032 [P] Update `catalog/table.go` godoc comments to document `DMLOptions` usage
- [x] T033 [P] Update `specs/006-returning-optimization/quickstart.md` with actual `Returning` flag usage
- [x] T034 Run full test suite: `go test ./...`
- [x] T035 Run integration tests with race detector: `go test -race ./tests/integration/...`
- [x] T036 Run linter: `golangci-lint run ./...`
- [x] T037 Verify all existing tests still pass (SC-004)
- [x] T038 Mark spec as complete and update status to "Implemented"

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies
- **Foundational (Phase 2)**: Depends on Setup - BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational completion
  - US1, US2, US3 can proceed in parallel OR sequentially by priority
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **User Story 1 (P1) - INSERT**: Can start after Foundational
- **User Story 2 (P2) - UPDATE**: Can start after Foundational (independent of US1)
- **User Story 3 (P2) - DELETE**: Can start after Foundational (independent of US1/US2)

### Within Each User Story

1. Update handler to create `DMLOptions`
2. Update handler to pass `opts` to interface method
3. Update test implementation signature
4. Update example implementation signature
5. Verify compilation
6. Run existing tests (regression)
7. Add new RETURNING-specific tests
8. Run new tests

### Parallel Opportunities

**Within Foundational Phase (Phase 2)**:
- T003, T004, T005 can run in parallel (different interface methods)

**Within User Story Implementation**:
- T009 and T010 can run in parallel [P] (different files)
- T018 and T019 can run in parallel [P] (different files)
- T026 and T027 can run in parallel [P] (different files)

**Across User Stories**:
- All three user stories can be worked on in parallel after Foundational phase

---

## Parallel Example: Foundational Phase

```bash
# After T002 (DMLOptions type) is complete, these can run in parallel:
Task: "Update InsertableTable interface in catalog/table.go"
Task: "Update UpdatableTable interface in catalog/table.go"
Task: "Update DeletableTable interface in catalog/table.go"
```

## Parallel Example: User Story 1

```bash
# After handler updates (T007, T008), these can run in parallel:
Task: "Update duckDBDMLTable.Insert in tests/integration/dml_test.go"
Task: "Update DMLTable.Insert in examples/dml/main.go"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001)
2. Complete Phase 2: Foundational (T002-T006)
3. Complete Phase 3: User Story 1 - INSERT (T007-T015)
4. **STOP and VALIDATE**: Test INSERT RETURNING independently
5. Demo/merge if ready

### Incremental Delivery

1. Foundation (Phase 2) → Interface contracts ready
2. User Story 1 → INSERT RETURNING works → **MVP complete**
3. User Story 2 → UPDATE RETURNING works
4. User Story 3 → DELETE RETURNING works
5. Polish → Full feature complete

### Key Implementation Details

**DMLOptions Population** (from user input):
```go
// In flight/doexchange_dml.go handlers:
opts := &catalog.DMLOptions{
    Returning: returnData, // from return-chunks header
    ReturningColumns: nil, // populated below if returning
}

if returnData {
    // Populate column names from input schema
    for i := 0; i < inputSchema.NumFields(); i++ {
        opts.ReturningColumns = append(opts.ReturningColumns, inputSchema.Field(i).Name)
    }
}
```

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story
- Each user story can be independently tested
- Commit after each task or logical group
- Run `go build ./...` frequently to catch compilation errors early
- The `Returning` bool field indicates if RETURNING clause is present
- `ReturningColumns` is populated from input schema when `Returning=true`
