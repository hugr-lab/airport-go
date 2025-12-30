# Tasks: Simplify Batch Table Interface Signatures

**Input**: Design documents from `/specs/013-batch-table-signature/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Go library**: `catalog/`, `flight/`, `examples/`, `tests/`, `docs/` at repository root

---

## Phase 1: Setup

**Purpose**: No setup required - existing project structure

**Note**: This feature modifies an existing Go library. No project initialization needed.

---

## Phase 2: Foundational - Interface Definitions (US1 + US2 Combined)

**Purpose**: Update interface definitions that MUST be complete before handler refactoring

**⚠️ CRITICAL**: Handler refactoring (US3) cannot begin until this phase is complete

**Goal**: Change `UpdatableBatchTable.Update` and `DeletableBatchTable.Delete` signatures from `array.RecordReader` to `arrow.RecordBatch`

### Implementation

- [x] T001 [US1] [US2] Update UpdatableBatchTable.Update signature in catalog/table.go - change parameter from `rows array.RecordReader` to `rows arrow.RecordBatch`
- [x] T002 [US1] [US2] Update DeletableBatchTable.Delete signature in catalog/table.go - change parameter from `rows array.RecordReader` to `rows arrow.RecordBatch`
- [x] T003 [US1] [US2] Update godoc comments for both interfaces in catalog/table.go - document null rowid error requirement and memory management
- [x] T004 [P] [US1] [US2] Add ErrNullRowID sentinel error in catalog/types.go

**Checkpoint**: Interface definitions updated - handler refactoring can now begin

---

## Phase 3: User Story 3 - Simplified Handler Implementation (Priority: P2)

**Goal**: Refactor handleDoExchangeUpdate and handleDoExchangeDelete to pass Record directly instead of wrapping in RecordReader

**Independent Test**: Integration tests pass after refactoring (tested in Phase 4)

### Implementation

- [x] T005 [US3] Refactor newUpdateProcessor in flight/doexchange_dml.go - remove RecordReader wrapper, pass batch directly to Update method
- [x] T006 [US3] Refactor newDeleteProcessor in flight/doexchange_dml.go - remove RecordReader wrapper, pass batch directly to Delete method
- [x] T007 [US3] Verify backward compatibility - ensure legacy interface (UpdatableTable, DeletableTable) path still works in flight/doexchange_dml.go

**Checkpoint**: Handler refactoring complete - ready for test updates

---

## Phase 4: Integration Tests

**Goal**: Update test table implementations to use new signatures

**Purpose**: Verify the signature change works end-to-end with DuckDB

### Implementation

- [x] T008 [US1] [US2] Update batchDMLTable.Update method in tests/integration/dml_batch_test.go - change signature from RecordReader to RecordBatch, remove Next() loop
- [x] T009 [US1] [US2] Update batchDMLTable.Delete method in tests/integration/dml_batch_test.go - change signature from RecordReader to RecordBatch, remove Next() loop
- [x] T010 [US1] [US2] Add null rowid handling to test table Update method in tests/integration/dml_batch_test.go - return error if rowid column has nulls
- [x] T011 [US1] [US2] Add null rowid handling to test table Delete method in tests/integration/dml_batch_test.go - return error if rowid column has nulls
- [x] T012 Run integration tests with race detector: `cd tests && go test -race ./integration/... -run TestDMLBatch`

**Checkpoint**: All integration tests pass - ready for examples and documentation

---

## Phase 5: User Story 4 - Updated Examples and Documentation (Priority: P3)

**Goal**: Update documentation and examples to reflect new method signatures

**Independent Test**: Documentation review and example compilation

### Implementation

- [x] T013 [P] [US4] Update api-guide.md batch interface documentation in docs/api-guide.md - update method signatures and add migration notes
- [x] T014 [P] [US4] Update examples/dml/main.go comments - clarify that legacy interface (UpdatableTable) is still supported and batch interface uses RecordBatch
- [x] T015 [US4] Review and verify examples compile: `cd examples && go build ./...`

**Checkpoint**: Documentation and examples updated

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and cleanup

- [x] T016 Run full test suite with race detector: `go test -race ./...`
- [x] T017 Run linter: `golangci-lint run ./...`
- [x] T018 Verify backward compatibility by running dml_test.go (legacy interface tests): `cd tests && go test -race ./integration/... -run TestDML`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: N/A - no setup needed
- **Phase 2 (Foundational)**: No dependencies - start immediately
- **Phase 3 (US3 Handlers)**: Depends on Phase 2 completion
- **Phase 4 (Tests)**: Depends on Phase 3 completion
- **Phase 5 (US4 Docs)**: Can start after Phase 2, but recommend after Phase 4 to document final API
- **Phase 6 (Polish)**: Depends on all previous phases

### User Story Dependencies

```
US1 + US2 (Interface Definitions) ──┬──► US3 (Handler Refactoring) ──► Tests ──► US4 (Docs)
                                    │
                                    └──► US4 (Docs can start early for interface changes)
```

- **US1 + US2**: Combined because they modify the same file (catalog/table.go) and are both P1 priority
- **US3**: Depends on US1+US2 - handlers call the interface methods
- **US4**: Can partially start after US1+US2, but should be finalized after tests pass

### Within Each Phase

- Tasks without [P] marker must complete in order
- Tasks with [P] marker within same phase can run in parallel

### Parallel Opportunities

- T003 and T004 can run in parallel (different files)
- T013 and T014 can run in parallel (different files)
- T008/T009 are in same file, must be sequential
- T010/T011 are in same file, must be sequential

---

## Parallel Example: Phase 2

```bash
# Sequential (same file):
Task T001: Update UpdatableBatchTable.Update signature
Task T002: Update DeletableBatchTable.Delete signature
Task T003: Update godoc comments

# Parallel with above (different file):
Task T004: Add ErrNullRowID sentinel error
```

## Parallel Example: Phase 5

```bash
# Can run in parallel (different files):
Task T013: Update api-guide.md
Task T014: Update examples/dml/main.go comments
```

---

## Implementation Strategy

### MVP First (US1 + US2 + US3)

1. Complete Phase 2: Interface Definitions (US1 + US2)
2. Complete Phase 3: Handler Refactoring (US3)
3. Complete Phase 4: Integration Tests
4. **STOP and VALIDATE**: All integration tests pass
5. Proceed to documentation

### Incremental Delivery

1. Phase 2 → Interface definitions updated
2. Phase 3 → Handlers simplified
3. Phase 4 → Tests verify everything works
4. Phase 5 → Documentation updated
5. Phase 6 → Final validation

### Single Developer Strategy

Execute phases sequentially in order. Total estimated tasks: 18

---

## Notes

- All tasks modify existing files (refactoring, not new feature)
- Legacy interfaces (UpdatableTable, DeletableTable) remain unchanged
- Memory management pattern unchanged (caller releases Record after method returns)
- Null rowid handling is a new requirement from clarification session
- Test tasks run existing tests - no new test files created
