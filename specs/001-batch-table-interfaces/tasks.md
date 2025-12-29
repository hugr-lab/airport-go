# Tasks: Batch Table Interfaces for Update and Delete

**Input**: Design documents from `/specs/001-batch-table-interfaces/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Tests are included as specified in the feature requirements (FR-010, FR-011).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Go module library structure** (from plan.md)
- Main code: `catalog/`, `flight/`
- Tests: `catalog/*_test.go`, `flight/*_test.go`, `tests/integration/`
- Examples: `examples/dml/`
- Documentation: `docs/`, root `*.md` files

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add helper function and interface definitions that all stories depend on

- [ ] T001 Add FindRowIDColumn helper function in catalog/helpers.go
- [ ] T002 [P] Add unit tests for FindRowIDColumn in catalog/helpers_test.go

---

## Phase 2: Foundational (Interface Definitions)

**Purpose**: Define the new interfaces that all DML operations will use

**⚠️ CRITICAL**: Handler refactoring depends on these interfaces being defined

- [ ] T003 Add UpdatableBatchTable interface definition in catalog/table.go
- [ ] T004 Add DeletableBatchTable interface definition in catalog/table.go

**Checkpoint**: Interfaces defined - handler refactoring can now begin

---

## Phase 3: User Story 1 - UpdatableBatchTable Interface (Priority: P1) 🎯 MVP

**Goal**: Enable simpler UPDATE implementations where rowid is embedded in RecordReader

**Independent Test**: Execute UPDATE SQL via DuckDB against a table implementing UpdatableBatchTable

### Tests for User Story 1

- [ ] T005 [P] [US1] Add unit test for batch update interface detection in flight/doexchange_dml_test.go
- [ ] T006 [P] [US1] Add integration test for batch UPDATE in tests/integration/dml_batch_test.go

### Implementation for User Story 1

- [ ] T007 [US1] Refactor handleDoExchangeUpdate to check for UpdatableBatchTable first in flight/doexchange_dml.go
- [ ] T008 [US1] Add batch path that calls Update(ctx, inputReader, opts) directly in flight/doexchange_dml.go
- [ ] T009 [US1] Add legacy fallback that extracts rowIDs and calls UpdatableTable.Update in flight/doexchange_dml.go
- [ ] T010 [US1] Add test for UPDATE with RETURNING using batch interface in tests/integration/dml_batch_test.go

**Checkpoint**: User Story 1 complete - UPDATE via UpdatableBatchTable works independently

---

## Phase 4: User Story 2 - DeletableBatchTable Interface (Priority: P1)

**Goal**: Enable simpler DELETE implementations where rowid is provided as RecordReader

**Independent Test**: Execute DELETE SQL via DuckDB against a table implementing DeletableBatchTable

### Tests for User Story 2

- [ ] T011 [P] [US2] Add unit test for batch delete interface detection in flight/doexchange_dml_test.go
- [ ] T012 [P] [US2] Add integration test for batch DELETE in tests/integration/dml_batch_test.go

### Implementation for User Story 2

- [ ] T013 [US2] Refactor handleDoExchangeDelete to check for DeletableBatchTable first in flight/doexchange_dml.go
- [ ] T014 [US2] Add batch path that calls Delete(ctx, inputReader, opts) directly in flight/doexchange_dml.go
- [ ] T015 [US2] Add legacy fallback that extracts rowIDs and calls DeletableTable.Delete in flight/doexchange_dml.go
- [ ] T016 [US2] Add test for DELETE with RETURNING using batch interface in tests/integration/dml_batch_test.go

**Checkpoint**: User Story 2 complete - DELETE via DeletableBatchTable works independently

---

## Phase 5: User Story 3 - Backward Compatibility (Priority: P2)

**Goal**: Ensure existing UpdatableTable and DeletableTable implementations continue to work

**Independent Test**: Run existing DML integration tests without modification - all must pass

### Tests for User Story 3

- [ ] T017 [P] [US3] Add test for legacy UpdatableTable fallback in flight/doexchange_dml_test.go
- [ ] T018 [P] [US3] Add test for legacy DeletableTable fallback in flight/doexchange_dml_test.go
- [ ] T019 [P] [US3] Add test verifying batch interface preferred when both implemented in flight/doexchange_dml_test.go

### Implementation for User Story 3

- [ ] T020 [US3] Run existing integration tests to verify backward compatibility in tests/integration/
- [ ] T021 [US3] Add test for interface priority (batch preferred over legacy) in tests/integration/dml_batch_test.go

**Checkpoint**: User Story 3 complete - Legacy interfaces work, batch is preferred when both exist

---

## Phase 6: User Story 4 - Documentation and Examples (Priority: P3)

**Goal**: Provide clear documentation and examples for batch interfaces

**Independent Test**: Documentation completeness review and running updated examples

### Implementation for User Story 4

- [ ] T022 [P] [US4] Update docs/api-guide.md with batch interface documentation
- [ ] T023 [P] [US4] Add migration guide section to docs/api-guide.md
- [ ] T024 [P] [US4] Update protocol_implementation_golang.qmd with batch interface examples
- [ ] T025 [US4] Update examples/dml/main.go to demonstrate UpdatableBatchTable implementation
- [ ] T026 [US4] Update examples/dml/main.go to demonstrate DeletableBatchTable implementation
- [ ] T027 [US4] Update examples/dml/README.md with batch interface usage instructions
- [ ] T028 [US4] Update README.md features section to mention batch interfaces

**Checkpoint**: User Story 4 complete - Documentation and examples updated

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and cleanup

- [ ] T029 Run go fmt on all modified files
- [ ] T030 Run golangci-lint and fix any issues
- [ ] T031 [P] Run all unit tests with race detector (go test -race ./...)
- [ ] T032 [P] Run all integration tests in tests/integration/
- [ ] T033 Validate examples build and run correctly (cd examples && go build ./...)
- [ ] T034 Review godoc comments on all new public APIs

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup (T001, T002) - Defines interfaces
- **User Stories (Phase 3-6)**: All depend on Foundational phase completion
  - User stories 1-2 (P1) can proceed in parallel (different handler functions)
  - User story 3 (P2) can run after 1-2 complete
  - User story 4 (P3) can proceed in parallel with others
- **Polish (Phase 7)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Depends on Foundational (T003-T004) - No dependencies on other stories
- **User Story 2 (P1)**: Depends on Foundational (T003-T004) - No dependencies on other stories
- **User Story 3 (P2)**: Depends on US1 and US2 (tests backward compatibility after new code)
- **User Story 4 (P3)**: Can proceed after T003-T004; documentation can be written in parallel

### Within Each User Story

- Tests can be written before implementation (TDD)
- Interface detection before batch path implementation
- Batch path before legacy fallback
- RETURNING test after main implementation

### Parallel Opportunities

- T001 and T002 can run in parallel (helper and its tests)
- T003 and T004 can run in parallel (different interfaces)
- T005 and T006 can run in parallel (different test files)
- T011 and T012 can run in parallel (different test files)
- T017, T018, T019 can run in parallel (different test cases)
- T022, T023, T024 can run in parallel (different documentation files)
- T031 and T032 can run in parallel (unit vs integration tests)

---

## Parallel Example: User Story 1 and 2 (P1 Stories)

```bash
# After Phase 2 (Foundational) completes, these can run in parallel:

# Developer A: User Story 1 (UPDATE)
Task: "Refactor handleDoExchangeUpdate in flight/doexchange_dml.go"

# Developer B: User Story 2 (DELETE)
Task: "Refactor handleDoExchangeDelete in flight/doexchange_dml.go"
```

---

## Parallel Example: Documentation Tasks

```bash
# All documentation tasks can run in parallel:
Task: "Update docs/api-guide.md with batch interface documentation"
Task: "Add migration guide section to docs/api-guide.md"
Task: "Update protocol_implementation_golang.qmd with batch interface examples"
Task: "Update README.md features section to mention batch interfaces"
```

---

## Implementation Strategy

### MVP First (User Stories 1-2)

1. Complete Phase 1: Setup (T001-T002)
2. Complete Phase 2: Foundational (T003-T004)
3. Complete Phase 3: User Story 1 - UpdatableBatchTable (T005-T010)
4. Complete Phase 4: User Story 2 - DeletableBatchTable (T011-T016)
5. **STOP and VALIDATE**: Both batch interfaces work
6. Deploy/merge if ready

### Incremental Delivery

1. Setup + Foundational → Interfaces defined
2. Add User Story 1 → Test batch UPDATE → Validate MVP
3. Add User Story 2 → Test batch DELETE → Full batch support
4. Add User Story 3 → Validate backward compatibility
5. Add User Story 4 → Documentation complete
6. Polish → Production ready

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (UPDATE)
   - Developer B: User Story 2 (DELETE)
   - Developer C: User Story 4 (Documentation)
3. After A and B complete: User Story 3 (Backward Compatibility)
4. Team: Polish phase

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- The spec requires tests (FR-010, FR-011), so test tasks are included
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
