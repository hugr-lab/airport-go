# Tasks: DML Operations and Transaction Management

**Input**: Design documents from `/specs/002-dml-transactions/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Integration tests are included per constitution requirements (Testing Standards: unit + integration tests for all new code).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

Based on plan.md structure (Go module at repository root):
- **catalog/**: Catalog interfaces and types
- **flight/**: Flight RPC handler implementations
- **internal/**: Internal utilities
- **tests/integration/**: Integration tests

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Core types and interfaces required by all user stories

- [x] T001 Add DMLResult type to catalog/types.go
- [x] T002 [P] Add InsertableTable interface to catalog/table.go
- [x] T003 [P] Add UpdatableTable interface to catalog/table.go
- [x] T004 [P] Add DeletableTable interface to catalog/table.go
- [x] T005 [P] Add Returning field to InsertDescriptor in flight/dml_types.go
- [x] T006 [P] Add Returning field to UpdateDescriptor in flight/dml_types.go
- [x] T007 [P] Add Returning field to DeleteAction in flight/dml_types.go
- [x] T008 Add DMLResponse type for MessagePack results in flight/dml_types.go

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Transaction infrastructure that MUST be complete before user stories can be fully implemented

**Note**: User Story 4 (Transactions) provides infrastructure used by US1-3 for optional transaction support

- [x] T009 Create catalog/transaction.go with TransactionManager interface and TransactionState type
- [x] T010 Create internal/txcontext/context.go with WithTransactionID and TransactionIDFromContext helpers
- [x] T011 Add extractTransactionID function for gRPC metadata extraction in flight/transaction.go
- [x] T012 Add withTransaction wrapper function in flight/transaction.go for auto commit/rollback
- [x] T013 Add TransactionManager field to flight.Server struct in flight/server.go
- [x] T014 Add TransactionManager field to ServerConfig in config.go

**Checkpoint**: Foundation ready - user story implementation can now begin

---

## Phase 3: User Story 1 - Insert Data into Tables (Priority: P1) MVP

**Goal**: Enable application developers to insert new data rows into Airport-managed tables

**Independent Test**: Create a table implementing InsertableTable, insert rows via DoPut, verify affected_rows count returned

### Tests for User Story 1

- [x] T015 [P] [US1] Add integration test for successful INSERT operation in tests/integration/dml_test.go
- [x] T016 [P] [US1] Add integration test for INSERT with RETURNING clause in tests/integration/dml_test.go
- [x] T017 [P] [US1] Add integration test for INSERT rejection on read-only table in tests/integration/dml_test.go

### Implementation for User Story 1

- [x] T018 [US1] Create test InsertableTable implementation (mock) in tests/integration/dml_test.go
- [x] T019 [US1] Implement handleDoPutInsert catalog integration in flight/dml.go - resolve table, check InsertableTable
- [x] T020 [US1] Add schema validation for INSERT (column count/type matching) in flight/dml.go
- [x] T021 [US1] Implement RETURNING clause serialization (Arrow IPC to MessagePack) in flight/dml.go
- [x] T022 [US1] Add gRPC error codes for INSERT failures (NotFound, FailedPrecondition, InvalidArgument) in flight/dml.go
- [x] T023 [US1] Add transaction context propagation to INSERT handler in flight/dml.go

**Checkpoint**: INSERT operations fully functional and independently testable

---

## Phase 4: User Story 2 - Update Existing Data (Priority: P2)

**Goal**: Enable application developers to modify existing rows in tables with rowid support

**Independent Test**: Insert test data, execute UPDATE via DoPut with row_ids, verify affected_rows and data changes

### Tests for User Story 2

- [x] T024 [P] [US2] Add integration test for successful UPDATE operation in tests/integration/dml_test.go
- [x] T025 [P] [US2] Add integration test for UPDATE with RETURNING clause in tests/integration/dml_test.go
- [x] T026 [P] [US2] Add integration test for UPDATE rejection on table without rowid in tests/integration/dml_test.go
- [x] T027 [P] [US2] Add integration test for UPDATE with empty row_ids validation in tests/integration/dml_test.go

### Implementation for User Story 2

- [x] T028 [US2] Create test UpdatableTable implementation (mock) in tests/integration/dml_test.go
- [x] T029 [US2] Implement handleDoPutUpdate catalog integration in flight/dml.go - resolve table, check UpdatableTable
- [x] T030 [US2] Add row_ids validation (non-empty, count matches RecordReader rows) in flight/dml.go
- [x] T031 [US2] Implement RETURNING clause serialization for UPDATE in flight/dml.go
- [x] T032 [US2] Add gRPC error codes for UPDATE failures in flight/dml.go
- [x] T033 [US2] Add transaction context propagation to UPDATE handler in flight/dml.go

**Checkpoint**: UPDATE operations fully functional and independently testable

---

## Phase 5: User Story 3 - Delete Unwanted Data (Priority: P3)

**Goal**: Enable application developers to remove rows from tables with rowid support

**Independent Test**: Insert test data, execute DELETE via DoAction with row_ids, verify affected_rows and rows removed

### Tests for User Story 3

- [x] T034 [P] [US3] Add integration test for successful DELETE operation in tests/integration/dml_test.go
- [x] T035 [P] [US3] Add integration test for DELETE with RETURNING clause in tests/integration/dml_test.go
- [x] T036 [P] [US3] Add integration test for DELETE rejection on table without rowid in tests/integration/dml_test.go
- [x] T037 [P] [US3] Add integration test for DELETE with empty row_ids validation in tests/integration/dml_test.go

### Implementation for User Story 3

- [x] T038 [US3] Create test DeletableTable implementation (mock) in tests/integration/dml_test.go
- [x] T039 [US3] Implement handleDeleteAction catalog integration in flight/dml.go - resolve table, check DeletableTable
- [x] T040 [US3] Add row_ids validation for DELETE (non-empty) in flight/dml.go
- [x] T041 [US3] Implement RETURNING clause serialization for DELETE in flight/dml.go
- [x] T042 [US3] Add gRPC error codes for DELETE failures in flight/dml.go
- [x] T043 [US3] Add transaction context propagation to DELETE handler in flight/dml.go

**Checkpoint**: DELETE operations fully functional and independently testable

---

## Phase 6: User Story 4 - Coordinate Related Operations (Priority: P2)

**Goal**: Enable transaction coordination across multiple DML operations with automatic commit/rollback

**Independent Test**: Configure transaction manager, create transaction, execute operations with transaction ID header, verify commit/rollback behavior

### Tests for User Story 4

- [x] T044 [P] [US4] Create tests/integration/transaction_test.go with test setup
- [x] T045 [P] [US4] Add integration test for create_transaction action in tests/integration/transaction_test.go
- [x] T046 [P] [US4] Add integration test for get_transaction_status action in tests/integration/transaction_test.go
- [x] T047 [US4] Add integration test for automatic commit on success in tests/integration/transaction_test.go
- [x] T048 [US4] Add integration test for automatic rollback on failure in tests/integration/transaction_test.go
- [x] T049 [US4] Add integration test for operations without transaction manager in tests/integration/transaction_test.go

### Implementation for User Story 4

- [x] T050 [US4] Create mock TransactionManager for testing in tests/integration/transaction_test.go
- [x] T051 [US4] Implement handleCreateTransaction action in flight/doaction_metadata.go
- [x] T052 [US4] Implement handleGetTransactionStatus action in flight/doaction_metadata.go
- [x] T053 [US4] Add create_transaction and get_transaction_status to action router in flight/doaction.go
- [x] T054 [US4] TransactionIDFromContext already exported in catalog/transaction.go
- [x] T055 [US4] Wire TransactionManager from config to flight.Server in server.go

**Checkpoint**: Transaction coordination fully functional and independently testable

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Code quality improvements and final validation

- [x] T056 Run golangci-lint on all modified files and fix any issues
- [x] T057 Run go test -race on all tests to verify concurrency safety
- [x] T058 Add godoc comments to all exported types and functions in catalog/table.go (already documented)
- [x] T059 [P] Add godoc comments to all exported types and functions in catalog/transaction.go (already documented)
- [x] T060 [P] Add godoc comments to all exported types and functions in flight/dml_types.go (already documented)
- [x] T061 [P] Add godoc comments to all exported types and functions in flight/transaction.go (unexported functions, N/A)
- [x] T062 Validate quickstart.md examples compile and work correctly (fixed deprecation warnings)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS transaction integration in user stories
- **User Stories (Phases 3-6)**:
  - US1 (INSERT) can start after Setup (Phase 1) - transaction integration needs Phase 2
  - US2 (UPDATE) can start after Setup - depends on US1 for test data setup
  - US3 (DELETE) can start after Setup - depends on US1 for test data setup
  - US4 (Transactions) can start after Phase 2 (Foundational)
- **Polish (Phase 7)**: Depends on all user stories being complete

### User Story Dependencies

| Story | Depends On | Can Parallel With |
|-------|------------|-------------------|
| US1 (INSERT) | Phase 1 Setup | - |
| US2 (UPDATE) | Phase 1 Setup, US1 for test data | US3, US4 |
| US3 (DELETE) | Phase 1 Setup, US1 for test data | US2, US4 |
| US4 (Transactions) | Phase 2 Foundational | US2, US3 |

### Within Each User Story

1. Tests MUST be written and FAIL before implementation
2. Mock implementations before handler implementation
3. Handler implementation before error handling
4. Core implementation before transaction integration
5. Story complete before moving to next priority

### Parallel Opportunities

**Phase 1 (Setup)**:
```
T002, T003, T004 - All DML interfaces (different types in same file)
T005, T006, T007 - All descriptor Returning fields (different types)
```

**Phase 2 (Foundational)**:
```
T009, T010 - TransactionManager interface + context helpers (different files)
T011, T012 - Flight transaction helpers (same file, sequential)
```

**User Story Tests** (within each story):
```
All [P] marked tests can run in parallel
```

---

## Parallel Example: Phase 1 Setup

```bash
# Launch all interface definitions together:
Task: "Add InsertableTable interface to catalog/table.go"
Task: "Add UpdatableTable interface to catalog/table.go"
Task: "Add DeletableTable interface to catalog/table.go"

# Launch all descriptor updates together:
Task: "Add Returning field to InsertDescriptor in flight/dml_types.go"
Task: "Add Returning field to UpdateDescriptor in flight/dml_types.go"
Task: "Add Returning field to DeleteAction in flight/dml_types.go"
```

## Parallel Example: User Story 1 Tests

```bash
# Launch all US1 tests together:
Task: "Add integration test for successful INSERT operation"
Task: "Add integration test for INSERT with RETURNING clause"
Task: "Add integration test for INSERT rejection on read-only table"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T008)
2. Complete Phase 3: User Story 1 (T015-T023)
3. **STOP and VALIDATE**: Test INSERT independently
4. Deploy/demo if ready - basic data population works

### Incremental Delivery

1. Setup → US1 complete → MVP: INSERT works
2. Add US2 → Test independently → UPDATE works
3. Add US3 → Test independently → DELETE works
4. Complete Phase 2 + US4 → Transaction coordination works
5. Each story adds value without breaking previous stories

### Suggested Execution Order

For solo developer:
1. Phase 1: Setup (T001-T008)
2. Phase 3: User Story 1 - INSERT (T015-T023) ← **MVP**
3. Phase 2: Foundational (T009-T014) - enables transaction support
4. Phase 4: User Story 2 - UPDATE (T024-T033)
5. Phase 5: User Story 3 - DELETE (T034-T043)
6. Phase 6: User Story 4 - Transactions (T044-T055)
7. Phase 7: Polish (T056-T062)

---

## Notes

- [P] tasks = different files or independent code sections
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- All DML handlers should work without TransactionManager (optional)
