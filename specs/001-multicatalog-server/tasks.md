# Tasks: Multi-Catalog Server Support

**Input**: Design documents from `/specs/001-multicatalog-server/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/

**Tests**: Not explicitly requested - omitting test tasks per template guidance.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Based on plan.md project structure:
- Root package: `*.go` (high-level API)
- Flight package: `flight/*.go` (dispatcher implementation)
- Auth package: `auth/*.go` (context helpers, catalog-aware auth)
- Catalog package: `catalog/*.go` (transaction manager interface)
- Tests: `tests/integration/*.go`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and foundational types

- [X] T001 [P] Create error types for multi-catalog operations in auth/errors.go
- [X] T002 [P] Create context key types and helper functions in auth/context.go (extend existing file with TraceID, SessionID, CatalogName helpers)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**CRITICAL**: No user story work can begin until this phase is complete

- [X] T003 Add Catalog() method to flight.Server to expose catalog reference in flight/server.go
- [X] T004 Create CatalogTransactionManager interface in catalog/transaction.go (extend existing file)
- [X] T005 [P] Create CatalogAuthorizer interface in auth/catalog_aware.go

**Checkpoint**: Foundation ready - user story implementation can now begin

---

## Phase 3: User Story 1 - Multi-Catalog Request Routing (Priority: P1) MVP

**Goal**: Route Flight RPC requests to the appropriate catalog server based on `airport-catalog` metadata header

**Independent Test**: Send requests with different `airport-catalog` header values and verify each request is handled by the correct underlying catalog server

### Implementation for User Story 1

- [X] T006 [US1] Create metadata extraction helper function in flight/multicatalog.go (extract airport-catalog from gRPC metadata)
- [X] T007 [US1] Create MultiCatalogServer struct with thread-safe catalog map in flight/multicatalog.go
- [X] T008 [US1] Implement catalog lookup method (getCatalogServer) in flight/multicatalog.go
- [X] T009 [US1] Implement DoGet delegation method in flight/multicatalog.go
- [X] T010 [US1] Implement DoAction delegation method in flight/multicatalog.go
- [X] T011 [US1] Implement DoExchange delegation method in flight/multicatalog.go
- [X] T012 [US1] Implement GetFlightInfo delegation method in flight/multicatalog.go
- [X] T013 [US1] Implement ListFlights delegation method in flight/multicatalog.go
- [X] T014 [US1] Implement GetSchema delegation method in flight/multicatalog.go
- [X] T015 [US1] Implement Handshake delegation method in flight/multicatalog.go
- [X] T016 [US1] Implement ListActions delegation method in flight/multicatalog.go
- [X] T017 [US1] Implement DoPut delegation method in flight/multicatalog.go

**Checkpoint**: At this point, User Story 1 (request routing) should be fully functional

---

## Phase 4: User Story 4 - Dynamic Catalog Management (Priority: P1)

**Goal**: Add and remove catalog servers at runtime without restarting the MultiCatalogServer

**Independent Test**: Add/remove catalogs at runtime and verify requests are routed correctly to newly added catalogs and fail gracefully for removed catalogs

### Implementation for User Story 4

- [X] T018 [US4] Implement AddCatalog method with thread-safe map update in flight/multicatalog.go
- [X] T019 [US4] Implement RemoveCatalog method with thread-safe map update in flight/multicatalog.go
- [X] T020 [US4] Implement Catalogs method to list all registered catalogs in flight/multicatalog.go

**Checkpoint**: At this point, User Story 4 (dynamic management) should be fully functional

---

## Phase 5: User Story 5 - Server Registration Validation (Priority: P1)

**Goal**: Validate catalog name uniqueness both at construction and when adding catalogs

**Independent Test**: Attempt to create a MultiCatalogServer with duplicate catalog names and verify an error is returned

### Implementation for User Story 5

- [X] T021 [US5] Add duplicate name validation to AddCatalog method in flight/multicatalog.go
- [X] T022 [US5] Implement NewMultiCatalogServerInternal constructor with validation in flight/multicatalog.go
- [X] T023 [US5] Add nil catalog validation to AddCatalog method in flight/multicatalog.go

**Checkpoint**: At this point, User Story 5 (validation) should be fully functional

---

## Phase 6: User Story 7 - High-Level Server Creation API (Priority: P1)

**Goal**: Provide simple `NewMultiCatalogServer` function similar to existing `NewServer`

**Independent Test**: Create a multi-catalog server with the high-level API and verify all catalogs are accessible

### Implementation for User Story 7

- [X] T024 [US7] Create MultiCatalogServerConfig struct in multicatalog.go (root package)
- [X] T025 [US7] Implement NewMultiCatalogServer function in multicatalog.go (root package)
- [X] T026 [US7] Add catalog name uniqueness validation in NewMultiCatalogServer
- [X] T027 [US7] Create internal flight.Server for each catalog in NewMultiCatalogServer
- [X] T028 [US7] Register MultiCatalogServer with gRPC server in NewMultiCatalogServer
- [X] T029 [US7] Add gRPC interceptor setup for auth (when auth configured) in multicatalog.go

**Checkpoint**: At this point, User Story 7 (high-level API) should be fully functional - Core MVP complete

---

## Phase 7: User Story 2 - Catalog-Aware Authorization (Priority: P2)

**Goal**: Implement per-catalog authorization after authentication

**Independent Test**: Implement CatalogAuthorizer with different rules per catalog and verify that access is correctly granted or denied based on the catalog being accessed

### Implementation for User Story 2

- [X] T030 [US2] Add CatalogAuthorizer type assertion check in auth interceptor in auth/interceptor.go
- [X] T031 [US2] Implement catalog-aware interceptor variant in auth/interceptor.go
- [X] T032 [US2] Integrate catalog-aware auth into MultiCatalogServer request flow in flight/multicatalog.go
- [X] T033 [US2] Wire catalog-aware interceptor in NewMultiCatalogServer when Auth implements CatalogAuthorizer in multicatalog.go

**Checkpoint**: At this point, User Story 2 (catalog-aware auth) should be fully functional

---

## Phase 8: User Story 6 - Catalog-Aware Transaction Management (Priority: P2)

**Goal**: Track which catalog a transaction belongs to for correct commit/rollback routing

**Independent Test**: Start transactions in different catalogs and verify commit/rollback operations affect the correct catalog

### Implementation for User Story 6

- [X] T034 [US6] Update BeginTransaction call to include catalog name in flight/transaction.go
- [X] T035 [US6] Update transaction handling in DoAction to use CatalogTransactionManager when available in flight/doaction.go
- [X] T036 [US6] Wire CatalogTransactionManager in NewMultiCatalogServer configuration in multicatalog.go

**Checkpoint**: At this point, User Story 6 (catalog-aware transactions) should be fully functional

---

## Phase 9: User Story 3 - Request Tracing Context Propagation (Priority: P3)

**Goal**: Propagate trace IDs and session IDs from client requests through the server

**Independent Test**: Send requests with trace/session ID headers and verify these values are accessible in the request context within catalog handlers

### Implementation for User Story 3

- [X] T037 [US3] Add metadata extraction for airport-trace-id and airport-client-session-id in flight/multicatalog.go
- [X] T038 [US3] Implement context enrichment with trace/session IDs in flight/multicatalog.go
- [X] T039 [US3] Ensure enriched context is passed to delegated catalog server methods in flight/multicatalog.go

**Checkpoint**: At this point, User Story 3 (context propagation) should be fully functional

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T040 [P] Add unit tests for MultiCatalogServer in flight/multicatalog_test.go
- [X] T041 [P] Add unit tests for high-level API in multicatalog_test.go
- [ ] T042 Add integration tests for multi-catalog routing in tests/integration/multicatalog_test.go
- [X] T043 [P] Add godoc comments to all exported types and functions
- [ ] T044 Run quickstart.md validation scenarios

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational phase completion
- **User Story 4 (Phase 4)**: Depends on User Story 1 (needs MultiCatalogServer struct)
- **User Story 5 (Phase 5)**: Depends on User Story 4 (validates AddCatalog)
- **User Story 7 (Phase 6)**: Depends on User Story 5 (uses validated internals)
- **User Story 2 (Phase 7)**: Depends on Foundational (CatalogAuthorizer interface) and User Story 7
- **User Story 6 (Phase 8)**: Depends on Foundational (CatalogTransactionManager interface) and User Story 7
- **User Story 3 (Phase 9)**: Depends on User Story 1 (context propagation in routing)
- **Polish (Phase 10)**: Depends on all user stories being complete

### User Story Dependencies

```text
Phase 1: Setup
    │
    ▼
Phase 2: Foundational ──────────────────────────────────┐
    │                                                   │
    ▼                                                   │
Phase 3: US1 (Routing) ─────────────────────────────────┤
    │                                                   │
    ▼                                                   │
Phase 4: US4 (Dynamic Management)                       │
    │                                                   │
    ▼                                                   │
Phase 5: US5 (Validation)                               │
    │                                                   │
    ▼                                                   │
Phase 6: US7 (High-Level API) ◄─────────────────────────┤
    │                                                   │
    ├───────────────────┬───────────────────┐           │
    ▼                   ▼                   ▼           │
Phase 7: US2 ◄──    Phase 8: US6 ◄──    Phase 9: US3   │
(Catalog Auth)      (Catalog Tx)        (Tracing)       │
    │                   │                   │           │
    └───────────────────┴───────────────────┘           │
                        │                               │
                        ▼                               │
                   Phase 10: Polish ◄───────────────────┘
```

### Within Each User Story

- Core struct/interface before methods
- Internal implementation before public API
- Error handling integrated throughout
- Story complete before moving to next priority

### Parallel Opportunities

- T001, T002: Setup tasks can run in parallel
- T004, T005: Interface definitions can run in parallel
- T009-T017: Delegation methods can be implemented in parallel (same file but independent functions)
- T040, T041, T043: Polish tasks can run in parallel

---

## Parallel Example: User Story 1 Delegation Methods

```bash
# Launch all delegation method implementations together:
Task: "T009 [US1] Implement DoGet delegation method"
Task: "T010 [US1] Implement DoAction delegation method"
Task: "T011 [US1] Implement DoExchange delegation method"
Task: "T012 [US1] Implement GetFlightInfo delegation method"
Task: "T013 [US1] Implement ListFlights delegation method"
Task: "T014 [US1] Implement GetSchema delegation method"
```

---

## Implementation Strategy

### MVP First (P1 Stories Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Routing)
4. Complete Phase 4: User Story 4 (Dynamic Management)
5. Complete Phase 5: User Story 5 (Validation)
6. Complete Phase 6: User Story 7 (High-Level API)
7. **STOP and VALIDATE**: Test multi-catalog server with basic routing
8. Deploy/demo if ready (Core MVP!)

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test routing → Basic routing works
3. Add User Stories 4, 5 → Test dynamic management → Runtime management works
4. Add User Story 7 → Test high-level API → MVP complete!
5. Add User Story 2 → Test catalog authorization → Security layer complete
6. Add User Story 6 → Test transactions → Transaction support complete
7. Add User Story 3 → Test tracing → Observability complete
8. Polish → Full feature complete

### File Summary

| File | Tasks | Description |
|------|-------|-------------|
| auth/context.go | T002 | Context helpers (extend existing) |
| auth/errors.go | T001 | Error types (new file) |
| auth/catalog_aware.go | T005 | CatalogAuthorizer interface (new file) |
| auth/interceptor.go | T030, T031 | Catalog-aware interceptor (modify existing) |
| catalog/transaction.go | T004 | CatalogTransactionManager interface (extend existing) |
| flight/server.go | T003 | Add Catalog() method (modify existing) |
| flight/multicatalog.go | T006-T023, T032, T037-T039 | MultiCatalogServer implementation (new file) |
| flight/multicatalog_test.go | T040 | Unit tests (new file) |
| flight/transaction.go | T034 | Transaction handling (modify existing) |
| flight/doaction.go | T035 | DoAction transaction handling (modify existing) |
| multicatalog.go | T024-T029, T033, T036, T041 | High-level API (new file) |
| multicatalog_test.go | T041 | High-level API tests (new file) |
| tests/integration/multicatalog_test.go | T042 | Integration tests (new file) |

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- P1 stories (US1, US4, US5, US7) form the MVP
- P2 stories (US2, US6) add security and transactions
- P3 story (US3) adds observability
