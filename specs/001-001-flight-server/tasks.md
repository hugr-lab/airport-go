# Tasks: Airport Go Flight Server Package

**Input**: Design documents from `/specs/001-001-flight-server/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Tests are NOT explicitly requested in the specification, so test tasks are NOT included. This follows the TDD-optional approach.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `- [ ] [ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Repository root is the airport package (`github.com/hugr-lab/airport-go`):
- Root-level `.go` files for public API
- Subpackages: `catalog/`, `flight/`, `auth/`, `internal/`
- Tests: `tests/unit/`, `tests/integration/`, `tests/benchmarks/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Initialize Go module with `go mod init github.com/hugr-lab/airport-go` in repository root
- [X] T002 [P] Create directory structure: `catalog/`, `flight/`, `auth/`, `internal/serialize/`, `internal/msgpack/`, `internal/recovery/`, `examples/basic/`, `testutil/`, `tests/unit/`, `tests/integration/`, `tests/benchmarks/`
- [X] T003 [P] Add dependencies to go.mod: `github.com/apache/arrow-go/v18/arrow`, `google.golang.org/grpc`, `github.com/klauspost/compress/zstd`, `github.com/vmihailenco/msgpack/v5`, `github.com/grpc-ecosystem/go-grpc-middleware/v2`
- [X] T004 [P] Create doc.go in repository root with package documentation
- [X] T005 [P] Configure golangci-lint with .golangci.yml in repository root
- [X] T006 [P] Create README.md with quickstart example (adapt from specs/001-001-flight-server/quickstart.md)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core types and interfaces that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 [P] Define Catalog interface in catalog/catalog.go (Catalog, Schema interfaces from contracts/catalog-interfaces.go.md)
- [X] T008 [P] Define Table interface in catalog/table.go (Table, DynamicSchemaTable interfaces from contracts/catalog-interfaces.go.md)
- [X] T009 [P] Define Function interfaces in catalog/function.go (ScalarFunction, TableFunction interfaces from contracts/catalog-interfaces.go.md)
- [X] T010 [P] Define supporting types in catalog/types.go (ScanOptions, TimePoint, SchemaRequest, FunctionSignature, ScanFunc from data-model.md)
- [X] T011 [P] Define ServerConfig struct in config.go (ServerConfig, standard errors from contracts/server-api.go.md)
- [X] T012 [P] Define Authenticator interface in auth/auth.go (Authenticator interface from contracts/server-api.go.md)
- [X] T013 Implement BearerAuth helper in auth/bearer.go (BearerAuth function from contracts/server-api.go.md)
- [X] T014 [P] Implement context helpers in auth/context.go (context value propagation for authentication)
- [X] T015 [P] Create internal/serialize package for ZStandard catalog serialization (research.md section 2)
- [X] T016 [P] Create internal/msgpack package for MessagePack parameter handling (research.md section 3)
- [X] T017 [P] Create internal/recovery package for panic recovery middleware (FR-024)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Basic Flight Server Setup (Priority: P1) 🎯 MVP

**Goal**: Register Airport Flight handlers on existing gRPC server with minimal configuration

**Independent Test**: Create a `grpc.Server`, call `airport.NewServer(grpcServer, config)`, start the server, verify it accepts Flight RPC connections from DuckDB Airport Extension client

### Implementation for User Story 1

- [X] T018 [US1] Implement NewServer function in server.go (validates ServerConfig, registers Flight handlers on grpc.Server)
- [X] T019 [P] [US1] Create Flight server struct in flight/server.go (embeds BaseFlightServer, holds catalog and allocator)
- [X] T020 [P] [US1] Implement Flight service registration in flight/server.go (RegisterFlightServiceServer pattern from research.md section 4)
- [X] T021 [US1] Add ServerConfig validation logic in config.go (check Catalog is not nil, return ErrInvalidConfig)
- [X] T022 [US1] Implement auth interceptor setup in auth/context.go (metadata extraction, token validation, context propagation from research.md section 5)
- [X] T023 [US1] Add slog integration in server.go (use slog.Default() for internal logging per FR-014)
- [X] T024 [US1] Create basic example in examples/basic/main.go (demonstrates NewServer with static catalog under 30 LOC per SC-001)

**Checkpoint**: At this point, User Story 1 should be fully functional - server can be created and started

---

## Phase 4: User Story 2 - Query Execution with Arrow Results (Priority: P1) 🎯 MVP

**Goal**: Define table scan functions returning `arrow.RecordReader` for efficient Flight client queries

**Independent Test**: Define a table with scan function, register in catalog, start server, connect via DuckDB Airport extension, verify Arrow batches stream correctly

### Implementation for User Story 2

- [X] T025 [P] [US2] Implement DoGet RPC handler in flight/doget.go (receives ticket, invokes scan function, streams RecordReader batches per research.md section 1)
- [X] T026 [P] [US2] Implement GetFlightInfo RPC handler in flight/getflightinfo.go (returns schema metadata and ticket for table queries)
- [X] T027 [US2] Add ticket generation/parsing logic in flight/ticket.go (opaque byte slice encoding schema/table names)
- [X] T028 [US2] Implement RecordReader streaming in flight/doget.go (IPC writer pattern with schema, batch-by-batch streaming per FR-018)
- [X] T029 [US2] Add context cancellation handling in flight/doget.go (respect ctx.Done(), stop streaming immediately per FR-007)
- [X] T030 [US2] Add error propagation in flight/doget.go (scan function errors returned as Flight protocol errors per FR-012)
- [X] T031 [US2] Add schema validation in flight/doget.go (verify RecordReader schema matches table ArrowSchema per FR-016)
- [X] T032 [US2] Update examples/basic/main.go (add scan function returning RecordReader with sample data)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work - clients can query tables and receive Arrow data

---

## Phase 5: User Story 3 - Catalog Discovery (Priority: P2)

**Goal**: Discover schemas, tables, and functions through Flight's `ListFlights` and `GetFlightInfo` RPCs

**Independent Test**: Build catalog with schemas/tables using builder API, call ListFlights to get serialized catalog, verify ZStandard-compressed Arrow IPC response

### Implementation for User Story 3

- [X] T033 [P] [US3] Implement CatalogBuilder struct in catalog/builder.go (fluent API for building static catalogs from contracts/builder-api.go.md)
- [X] T034 [P] [US3] Implement SchemaBuilder struct in catalog/builder.go (fluent API for adding tables/functions to schema)
- [X] T035 [P] [US3] Define SimpleTableDef struct in catalog/builder.go (name, schema, scanFunc, comment from contracts/builder-api.go.md)
- [X] T036 [US3] Implement static catalog implementation in catalog/static.go (immutable catalog from builder, implements Catalog interface)
- [X] T037 [US3] Implement static schema implementation in catalog/static.go (implements Schema interface with tables/functions from builder)
- [X] T038 [US3] Implement static table implementation in catalog/static.go (implements Table interface, wraps SimpleTableDef)
- [X] T039 [US3] Add builder validation in catalog/builder.go (validate schema/table names unique, non-empty, ArrowSchema valid per contracts/builder-api.go.md)
- [X] T040 [P] [US3] Implement ListFlights RPC handler in flight/listflights.go (serialize catalog to Arrow IPC, compress with ZStandard per research.md section 2)
- [X] T041 [P] [US3] Implement catalog serialization in internal/serialize/catalog.go (Flight SQL schema format: GetCatalogs, GetDbSchemas, GetTables per research.md section 2)
- [X] T042 [US3] Add ZStandard compression in internal/serialize/compress.go (reusable encoder, SpeedDefault level per research.md section 2)
- [X] T043 [US3] Update examples/basic/main.go (use CatalogBuilder to create catalog with schemas/tables)

**Checkpoint**: All P1 user stories + catalog discovery work - clients can explore available data dynamically

---

## Phase 6: User Story 4 - Authentication and Authorization (Priority: P2)

**Goal**: Configure authentication (bearer token) to control server access

**Independent Test**: Configure `airport.BearerAuth()` in ServerConfig, start server, verify unauthenticated requests rejected, authenticated requests succeed

### Implementation for User Story 4

- [X] T044 [P] [US4] Implement authentication function wrapper in auth/bearer.go (convert user validation func to Authenticator interface)
- [X] T045 [P] [US4] Implement NoAuth authenticator in auth/auth.go (allows all requests, for development only)
- [X] T046 [US4] Add auth interceptor registration in server.go (grpc_auth.UnaryServerInterceptor and StreamServerInterceptor per research.md section 5)
- [X] T047 [US4] Implement metadata extraction in auth/context.go (FromIncomingContext, extract "authorization" header per research.md section 5)
- [X] T048 [US4] Add token validation in auth/context.go (call Authenticator.Authenticate, propagate identity via context)
- [X] T049 [US4] Add proper gRPC status codes in auth/context.go (codes.Unauthenticated for invalid tokens)
- [X] T050 [US4] Create authenticated example in examples/auth/main.go (demonstrates BearerAuth with token validation)

**Checkpoint**: Authentication works - server can restrict access based on bearer tokens

---

## Phase 7: User Story 5 - Parameterized Queries (Priority: P3)

**Goal**: Send parameterized queries using DoPut RPC with MessagePack-serialized parameters

**Independent Test**: Send DoPut request with MessagePack parameters, verify server deserializes them, passes to handler, returns Arrow results

### Implementation for User Story 5

- [X] T051 [P] [US5] Implement DoPut RPC handler in flight/doput.go (receives client stream, deserializes parameters, invokes handler)
- [X] T052 [P] [US5] Implement MessagePack deserialization in internal/msgpack/decode.go (vmihailenco/msgpack/v5, struct tag support per research.md section 3)
- [X] T053 [US5] Add parameter validation in flight/doput.go (type checking, error handling for invalid parameters)
- [X] T054 [US5] Implement response streaming in flight/doput.go (stream Arrow batches back through DoPut response)
- [X] T055 [US5] Add parameter deserialization error handling in flight/doput.go (return clear error for invalid MessagePack format)

**Checkpoint**: Parameterized queries work - clients can send parameters and receive results

---

## Phase 8: User Story 6 - Custom Scalar Functions (Priority: P3)

**Goal**: Register custom scalar functions in catalog for DuckDB clients to call in queries

**Independent Test**: Implement scalar function (e.g., UPPERCASE), register in catalog, verify DuckDB can call it in queries via Flight

### Implementation for User Story 6

- [X] T056 [P] [US6] Implement DoAction RPC handler in flight/doaction.go (handles function invocation requests)
- [X] T057 [P] [US6] Add scalar function execution in flight/doaction.go (deserialize parameters, call Execute, serialize results)
- [X] T058 [US6] Add function signature validation in flight/doaction.go (verify parameter types match signature per data-model.md)
- [X] T059 [US6] Implement vectorized execution pattern in flight/doaction.go (process entire arrow.Record batches per contracts/catalog-interfaces.go.md)
- [X] T060 [US6] Add function error handling in flight/doaction.go (type mismatch errors, execution failures)
- [X] T061 [US6] Update catalog serialization in internal/serialize/catalog.go (include scalar functions in ListFlights response)
- [X] T062 [US6] Create scalar function example in examples/basic/functions.go (UPPERCASE function demonstrating ScalarFunction interface)

**Checkpoint**: Custom scalar functions work - clients can call user-defined functions in queries

---

## Phase 9: User Story 7 - Dynamic Catalog Implementation (Priority: P3)

**Goal**: Implement Catalog interface with custom logic for dynamic schemas (database-backed, permission-aware)

**Independent Test**: Implement custom Catalog returning different schemas based on context, verify ListFlights reflects dynamic behavior

### Implementation for User Story 7

- [X] T063 [P] [US7] Add context deadline enforcement in flight/listflights.go (apply timeout to catalog method calls per FR-023)
- [X] T064 [P] [US7] Add panic recovery in flight/listflights.go (recover from catalog implementation panics per FR-024)
- [X] T065 [P] [US7] Add panic recovery in flight/doget.go (recover from scan function panics)
- [X] T066 [US7] Add panic recovery middleware in internal/recovery/recover.go (centralized panic handling, logging, error conversion)
- [X] T067 [US7] Create dynamic catalog example in examples/dynamic/main.go (demonstrates custom Catalog implementation with state changes)
- [X] T068 [US7] Add permission-based catalog example in examples/dynamic/permissions.go (filter schemas/tables based on authenticated user)

**Checkpoint**: All user stories complete - package supports static and dynamic catalogs, authentication, functions

---

## Phase 10: DuckDB Airport Extension Protocol Support

**Purpose**: Implement DuckDB-specific Flight actions for compatibility

- [X] T069 [P] Implement table_function_flight_info action in flight/doaction.go (get schema for table function without executing per contracts/catalog-interfaces.go.md)
- [X] T070 [P] Implement endpoints action in flight/doaction.go (get flight endpoints for time-travel queries per contracts/catalog-interfaces.go.md)
- [X] T071 [P] Add TableFunction support in catalog/builder.go (SchemaBuilder.TableFunc method from contracts/builder-api.go.md)
- [X] T072 [US7] Implement DynamicSchemaTable support in catalog/table.go (SchemaForRequest method from contracts/catalog-interfaces.go.md)
- [X] T073 Add TimePoint handling in catalog/types.go (time-travel query support)

---

## Phase 11: Integration Testing with DuckDB

**Purpose**: Validate Flight server works correctly with DuckDB Airport Extension client

- [X] T074 Setup DuckDB integration test framework in integration_test.go (launch gRPC server, install Airport extension)
- [X] T075 [P] Create catalog discovery test in integration_catalog_test.go (verify ListFlights returns correct schemas/tables)
- [X] T076 [P] Create query execution test in integration_query_test.go (verify DoGet streams Arrow data correctly)
- [X] T077 [P] Create authentication test in integration_auth_test.go (verify bearer token validation)
- [X] T078 [P] Create scalar function test in integration_functions_test.go (verify DuckDB can call custom functions)
- [X] T079 [P] Create dynamic catalog test in integration_dynamic_test.go (verify catalog changes reflected in queries)

---

## Phase 12: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T080 [P] Add godoc comments to all public APIs in root package files (NewServer, ServerConfig, standard errors)
- [X] T081 [P] Add godoc comments to catalog package (all interfaces and types)
- [X] T082 [P] Add godoc comments to auth package (Authenticator, BearerAuth)
- [X] T083 [P] Create unit tests for CatalogBuilder in builder_test.go (validation, duplicate names, invalid schemas)
- [X] T084 [P] Create unit tests for static catalog in catalog/static_test.go (Schemas, Schema lookups, thread safety)
- [X] T085 [P] Create unit tests for auth in auth/auth_test.go (token validation, error cases)
- [X] T086 [P] Create unit tests for serialization in internal/serialize/catalog_test.go (Flight SQL schema format, compression)
- [X] T087 [P] Create benchmarks for catalog serialization in benchmark_test.go (measure compression ratio, speed)
- [X] T088 [P] Create benchmarks for Arrow streaming in benchmark_test.go (measure throughput, memory usage)
- [ ] T089 Run golangci-lint and fix all warnings (golangci-lint not installed)
- [ ] T090 Validate quickstart.md example (ensure it runs and produces expected output)
- [X] T091 Add memory leak detection test using memory.NewCheckedAllocator in memory_test.go (NO LEAKS FOUND)
- [X] T092 Run go test with -race flag across all packages to detect race conditions (NO RACES FOUND)
- [ ] T093 [P] Add TLS example in examples/tls/main.go (demonstrate grpc.Creds configuration)
- [ ] T094 [P] Add performance tips to README.md (batch sizes, connection pooling, streaming, context cancellation)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-9)**: All depend on Foundational phase completion
  - US1 (Phase 3): Can start after Foundational - No dependencies on other stories
  - US2 (Phase 4): Can start after Foundational - Depends on US1 (needs NewServer from US1)
  - US3 (Phase 5): Can start after Foundational - Depends on US1 (needs server registration)
  - US4 (Phase 6): Can start after Foundational - Integrates with US1 (auth interceptor in NewServer)
  - US5 (Phase 7): Can start after Foundational - Independent of other stories
  - US6 (Phase 8): Can start after US3 (needs catalog builder for functions)
  - US7 (Phase 9): Can start after Foundational - Independent (custom Catalog implementation)
- **DuckDB Protocol (Phase 10)**: Can start after US6 and US7 (needs TableFunction and DynamicSchemaTable)
- **Integration Testing (Phase 11)**: Depends on US1, US2, US3, US4, US6 being complete
- **Polish (Phase 12)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Foundation → US1 → Ready for clients
- **User Story 2 (P1)**: Foundation + US1 → US2 → Queries work
- **User Story 3 (P2)**: Foundation + US1 → US3 → Catalog discovery works
- **User Story 4 (P2)**: Foundation + US1 → US4 → Authentication works
- **User Story 5 (P3)**: Foundation → US5 → Parameterized queries work (independent)
- **User Story 6 (P3)**: Foundation + US3 → US6 → Custom functions work
- **User Story 7 (P3)**: Foundation → US7 → Dynamic catalogs work (independent)

### Within Each User Story

- Interfaces before implementations
- Core types before handlers
- Handlers before examples
- Examples before tests

### Parallel Opportunities

- **Phase 1**: T002, T003, T004, T005, T006 can all run in parallel
- **Phase 2**: T007-T017 can all run in parallel (different files, define interfaces/types)
- **Phase 3**: T019, T020 can run in parallel (different files)
- **Phase 4**: T025, T026 can run in parallel (different RPC handlers)
- **Phase 5**: T033-T039 (catalog builder), T040-T042 (serialization) can run in parallel
- **Phase 6**: T044, T045 can run in parallel
- **Phase 7**: T051, T052 can run in parallel
- **Phase 8**: T056, T057 can run in parallel
- **Phase 9**: T063-T065 can run in parallel (panic recovery in different handlers)
- **Phase 10**: T069, T070, T071 can run in parallel
- **Phase 11**: T075-T079 can all run in parallel (different integration tests)
- **Phase 12**: T080-T094 can mostly run in parallel (different files)

---

## Parallel Example: User Story 1

```bash
# Launch foundational tasks together:
Task T007: "Define Catalog interface in catalog/catalog.go"
Task T008: "Define Table interface in catalog/table.go"
Task T009: "Define Function interfaces in catalog/function.go"
Task T010: "Define supporting types in catalog/types.go"
Task T011: "Define ServerConfig struct in config.go"
Task T012: "Define Authenticator interface in auth/auth.go"

# Then launch US1 implementation tasks:
Task T019: "Create Flight server struct in flight/server.go"
Task T020: "Implement Flight service registration in flight/server.go"
```

---

## Implementation Strategy

### MVP First (User Stories 1 + 2 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Server Setup)
4. Complete Phase 4: User Story 2 (Query Execution)
5. **STOP and VALIDATE**: Test US1 + US2 independently with DuckDB client
6. Deploy/demo if ready - this is a working Flight server!

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add User Story 1 → Server can be created and started (MVP milestone 1)
3. Add User Story 2 → Queries work, Arrow data streams (MVP milestone 2)
4. Add User Story 3 → Catalog discovery works (Production-ready)
5. Add User Story 4 → Authentication works (Secure production deployment)
6. Add User Story 5 → Parameterized queries work (Enhanced functionality)
7. Add User Story 6 → Custom functions work (Advanced features)
8. Add User Story 7 → Dynamic catalogs work (Enterprise features)
9. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 + 2 (core query path) - **priority**
   - Developer B: User Story 3 (catalog builder) - can start in parallel
   - Developer C: User Story 5 (DoPut/parameters) - can start in parallel
3. After US1+2 complete:
   - Developer A: User Story 4 (authentication) - integrates with US1
   - Developer B: User Story 6 (functions) - needs US3
   - Developer C: User Story 7 (dynamic catalog) - independent
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies, can run in parallel
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Tests are NOT included (not requested in specification)
- Package uses Go 1.25+, Arrow v18, follows idiomatic Go patterns
- All public APIs require godoc comments (SC-010)
- Target 80% test coverage (SC-002)
- No panics in library code (FR-012)
- Context cancellation everywhere (FR-007)
