# Feature Specification: Repository Preparation for Production Use

**Feature Branch**: `001-repo-preparation`
**Created**: 2025-11-26
**Status**: Draft
**Input**: User description: "Preparing repo for use by reorganizing structure, adding DuckDB client examples, and creating comprehensive spec for all Airport features including DDL, DML, endpoints, and time-travel queries"

## Clarifications

### Session 2025-11-26

- Q: What should happen when CREATE SCHEMA is called with a name that already exists? → A: Return descriptive error ("schema 'X' already exists")
- Q: What is the maximum number of rows per table that DML operations should support? → A: Unlimited (implementation-defined); 100M rows for testing
- Q: What logging level and format should be used for "basic logging" mentioned in out-of-scope section? → A: Structured logging (JSON) with standard library (log/slog)
- Q: Should DDL operations (CREATE/ALTER/DROP) support IF EXISTS / IF NOT EXISTS clauses for idempotent operations? → A: Yes, support both IF EXISTS and IF NOT EXISTS clauses (aligned with DuckDB Airport extension)
- Q: What minimum version of the DuckDB Airport extension should examples require and document? → A: Latest Airport extension with DuckDB 1.4+

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Clean Repository Structure (Priority: P1)

As a developer, I need a well-organized repository with tests in conventional locations and no unused directories so that I can quickly find what I need and contribute effectively.

**Why this priority**: This is foundational work that enables all other development activities. A clean repository structure directly impacts developer productivity and project maintainability.

**Independent Test**: Can be fully tested by cloning the repository, verifying that integration tests exist in `tests/integration/`, confirming unused directories are removed, and checking that documentation accurately reflects the current structure. Delivers immediate value by reducing developer onboarding time.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** a developer looks for integration tests, **Then** they find them in `tests/integration/` directory
2. **Given** the repository structure, **When** a developer reviews directories, **Then** only actively used directories remain
3. **Given** updated documentation, **When** a developer reads it, **Then** it accurately describes the current repository structure

---

### User Story 2 - Automated Quality Assurance (Priority: P1)

As a maintainer, I need automated linting and testing through GitHub Actions so that every PR is automatically validated and quality standards are consistently enforced.

**Why this priority**: Automated CI is foundational for maintaining code quality at scale. It prevents broken code from being merged and establishes quality gates before any feature work.

**Independent Test**: Can be fully tested by creating a test PR, verifying that GitHub Actions workflow runs automatically, checking that linting and tests execute, and confirming results are reported. Delivers immediate value by catching issues before code review.

**Acceptance Scenarios**:

1. **Given** a new PR is opened, **When** the PR is created, **Then** GitHub Actions workflow runs automatically
2. **Given** the workflow is running, **When** linting detects issues, **Then** the workflow fails with clear error messages
3. **Given** the workflow is running, **When** tests fail, **Then** the workflow fails with test output
4. **Given** all checks pass, **When** the workflow completes, **Then** a green checkmark appears on the PR

---

### User Story 3 - Working Examples with DuckDB Client (Priority: P2)

As a developer integrating Airport into my application, I need working examples showing DuckDB client connections and queries so that I can quickly understand how to use the server.

**Why this priority**: Examples are the primary way developers learn to use the library. With repo structure and CI in place (P1), this provides functional demonstrations of the working system.

**Independent Test**: Can be fully tested by running each example (`examples/basic/`, `examples/auth/`, `examples/dynamic/`), verifying DuckDB client connects successfully, executing sample queries, and confirming results match expectations. Delivers immediate value by providing working reference implementations.

**Acceptance Scenarios**:

1. **Given** the basic example, **When** a developer runs it, **Then** DuckDB client connects to the Airport server and executes a SELECT query
2. **Given** the auth example, **When** a developer runs it, **Then** DuckDB client authenticates using bearer token and queries protected data
3. **Given** the dynamic example, **When** a developer runs it, **Then** DuckDB client connects to dynamically configured catalog and retrieves results
4. **Given** any example output, **When** a developer reviews it, **Then** query results are displayed clearly with row counts and data

---

### User Story 4 - DDL Operations Support (Priority: P2)

As a database administrator, I need to create and manage schemas and tables through the Airport server so that I can define the structure of my data catalog.

**Why this priority**: DDL operations are fundamental database capabilities. With working examples (P2), this extends functionality to include schema management alongside data querying.

**Independent Test**: Can be fully tested by connecting to Airport server, executing CREATE SCHEMA, CREATE TABLE, ALTER TABLE, DROP TABLE, and DROP SCHEMA commands, and verifying catalog state changes. Delivers immediate value by enabling dynamic catalog management.

**Acceptance Scenarios**:

1. **Given** a connected client, **When** CREATE SCHEMA is executed, **Then** a new schema appears in the catalog
2. **Given** an existing schema name, **When** CREATE SCHEMA IF NOT EXISTS is executed, **Then** operation succeeds without error (idempotent)
3. **Given** an existing schema, **When** CREATE TABLE is executed with Arrow schema definition, **Then** a new table appears in the schema
4. **Given** an existing table, **When** ALTER TABLE ADD COLUMN is executed, **Then** the table schema is updated with the new column
5. **Given** an existing table, **When** DROP TABLE IF EXISTS is executed, **Then** the table is removed from the catalog
6. **Given** a non-existent table, **When** DROP TABLE IF EXISTS is executed, **Then** operation succeeds without error (idempotent)
7. **Given** an empty schema, **When** DROP SCHEMA is executed, **Then** the schema is removed from the catalog

---

### User Story 5 - DML Operations Support (Priority: P2)

As a data engineer, I need to insert, update, and delete data through the Airport server so that I can modify catalog contents programmatically.

**Why this priority**: DML operations complete the basic CRUD functionality alongside DDL (P2). Together they provide a complete data management solution.

**Independent Test**: Can be fully tested by executing INSERT with Arrow record batches, UPDATE using rowid, DELETE using rowid, and verifying data changes through subsequent queries. Delivers immediate value by enabling full data lifecycle management.

**Acceptance Scenarios**:

1. **Given** a table with defined schema, **When** INSERT is executed with Arrow record batch, **Then** new rows appear in the table
2. **Given** existing rows in a table, **When** UPDATE is executed with rowid and new values, **Then** specified rows are modified
3. **Given** existing rows in a table, **When** DELETE is executed with rowid, **Then** specified rows are removed
4. **Given** a DML operation, **When** errors occur (invalid rowid, schema mismatch), **Then** descriptive error messages are returned

---

### User Story 6 - Point-in-Time Query Support (Priority: P3)

As a data analyst, I need to query historical states of data using point-in-time parameters so that I can analyze how data has changed over time.

**Why this priority**: Time-travel is an advanced feature that builds on basic querying (P2). It provides valuable analytics capabilities but isn't required for basic operation.

**Independent Test**: Can be fully tested by executing queries with `ts` and `ts_ns` parameters, verifying historical data is returned, and confirming current queries work without time parameters. Delivers value by enabling temporal analysis and audit trails.

**Acceptance Scenarios**:

1. **Given** a table with historical data, **When** a query includes `ts` parameter with Unix timestamp, **Then** data as of that timestamp is returned
2. **Given** a table with historical data, **When** a query includes `ts_ns` parameter with nanosecond timestamp, **Then** data as of that precise time is returned
3. **Given** a time-travel query for a time before data existed, **When** executed, **Then** an empty result set is returned
4. **Given** a query without time parameters, **When** executed, **Then** current data is returned

---

### User Story 7 - Endpoint Discovery and Management (Priority: P3)

As a distributed systems engineer, I need to discover and manage Flight endpoints so that I can route queries to appropriate data sources and parallelize operations.

**Why this priority**: Endpoint discovery is an advanced distributed systems feature. It's valuable for scale-out scenarios but not required for basic single-server usage.

**Independent Test**: Can be fully tested by calling `flight_info` action, verifying endpoint metadata is returned, calling `endpoints` action for endpoint list, and using `table_function_flight_info` for parameterized discovery. Delivers value by enabling distributed query routing and parallelization.

**Acceptance Scenarios**:

1. **Given** a Flight RPC connection, **When** `flight_info` action is called for a table, **Then** FlightInfo with schema and endpoint locations is returned
2. **Given** a Flight RPC connection, **When** `endpoints` action is called, **Then** a list of available endpoints with descriptions is returned
3. **Given** a table function, **When** `table_function_flight_info` action is called with parameters, **Then** FlightInfo for the parameterized query is returned
4. **Given** FlightInfo with multiple endpoints, **When** processing the query, **Then** data can be fetched from any listed endpoint

---

### Edge Cases

- What happens when integration tests directory already exists during migration?
- How does the system handle CI workflow failures due to external service unavailability?
- What happens when DuckDB client cannot connect to the Airport server in examples?
- How does DDL handle schema name conflicts when creating schemas?
- Schema name conflict: CREATE SCHEMA with existing name returns error "schema 'X' already exists" (unless IF NOT EXISTS is used)
- Idempotent operations: IF NOT EXISTS and IF EXISTS clauses enable safe retry logic
- What happens when ALTER TABLE tries to add a column that already exists?
- How does DML handle invalid rowid values in UPDATE/DELETE operations?
- What happens when time-travel queries specify timestamps in the future?
- How does endpoint discovery handle network partitions or unavailable endpoints?
- What happens when concurrent DDL operations try to modify the same table?
- How does the system handle malformed Arrow schemas in CREATE TABLE?

## Requirements *(mandatory)*

### Functional Requirements

**Repository Structure**:

- **FR-001**: System MUST relocate all integration tests to `tests/integration/` directory
- **FR-002**: System MUST remove all unused directories from the repository
- **FR-003**: System MUST update all documentation to reflect the new repository structure
- **FR-004**: System MUST maintain backwards compatibility for existing import paths

**CI/CD Automation**:

- **FR-005**: System MUST provide GitHub Actions workflow that runs on all PRs
- **FR-006**: Workflow MUST execute Go linting using standard tools
- **FR-007**: Workflow MUST run all unit tests and integration tests
- **FR-008**: Workflow MUST complete within 10 minutes
- **FR-009**: Workflow MUST fail PRs when linting or tests fail

**Example Enhancement**:

- **FR-010**: All examples MUST include DuckDB client connection code
- **FR-010a**: Example documentation MUST specify DuckDB 1.4+ and latest Airport extension as requirements
- **FR-011**: All examples MUST execute at least one SELECT query and display results
- **FR-012**: Auth example MUST demonstrate bearer token authentication
- **FR-013**: Examples MUST include clear output showing query results
- **FR-014**: Examples MUST handle connection errors gracefully with error messages

**DDL Operations**:

- **FR-015**: System MUST support CREATE SCHEMA via DoAction RPC; MUST return error if schema name already exists (unless IF NOT EXISTS specified)
- **FR-015a**: System MUST support IF NOT EXISTS clause for CREATE SCHEMA and CREATE TABLE operations
- **FR-015b**: System MUST support IF EXISTS clause for DROP SCHEMA, DROP TABLE, and ALTER TABLE operations
- **FR-016**: System MUST support DROP SCHEMA via DoAction RPC
- **FR-017**: System MUST support CREATE TABLE with Arrow schema definition via DoAction RPC
- **FR-018**: System MUST support DROP TABLE via DoAction RPC
- **FR-019**: System MUST support ALTER TABLE ADD COLUMN via DoAction RPC
- **FR-020**: System MUST support ALTER TABLE DROP COLUMN via DoAction RPC
- **FR-021**: DDL operations MUST return success/failure status
- **FR-022**: DDL operations MUST provide descriptive error messages on failure

**DML Operations**:

- **FR-023**: System MUST support INSERT with Arrow record batches via DoPut RPC
- **FR-024**: System MUST support UPDATE with rowid and Arrow data via DoPut RPC
- **FR-025**: System MUST support DELETE with rowid via DoAction RPC
- **FR-026**: DML operations MUST validate Arrow schema compatibility
- **FR-027**: DML operations MUST return affected row counts
- **FR-028**: DML operations MUST provide descriptive error messages on schema mismatch

**Point-in-Time Queries**:

- **FR-029**: System MUST support `ts` parameter for Unix timestamp (seconds) time-travel
- **FR-030**: System MUST support `ts_ns` parameter for nanosecond precision time-travel
- **FR-031**: System MUST return empty result set for queries before data existence
- **FR-032**: System MUST return current data when no time parameters are specified
- **FR-033**: System MUST validate timestamp format and range

**Endpoint Management**:

- **FR-034**: System MUST support `flight_info` action returning FlightInfo with schema and endpoints
- **FR-035**: System MUST support `endpoints` action returning list of available endpoints
- **FR-036**: System MUST support `table_function_flight_info` action for parameterized queries
- **FR-037**: FlightInfo MUST include ticket for data retrieval
- **FR-038**: Endpoint locations MUST include valid URI for data access

### Key Entities

- **Repository Structure**: The organized layout of directories and files including `tests/integration/`, `examples/`, `internal/`, `catalog/`, and documentation files
- **GitHub Actions Workflow**: CI/CD pipeline configuration defining linting, testing, and quality gates
- **Example Application**: Runnable Go programs demonstrating Airport server usage with DuckDB client integration
- **DDL Statement**: Data definition command (CREATE/DROP SCHEMA, CREATE/DROP/ALTER TABLE) executed via DoAction RPC
- **DML Statement**: Data manipulation command (INSERT, UPDATE, DELETE) executed via DoPut or DoAction RPC
- **TimePoint**: Temporal reference specified via `ts` (Unix seconds) or `ts_ns` (nanoseconds) parameters for historical queries
- **Endpoint**: Flight service location with URI and optional metadata describing data access points
- **Catalog**: Hierarchical structure of schemas, tables, and functions exposed by the Airport server

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Developers can locate integration tests and core documentation within 5 minutes of cloning repository
- **SC-002**: CI workflow completes in under 10 minutes for typical PRs
- **SC-003**: 100% of PRs have automated test results before human review
- **SC-004**: All three examples (basic, auth, dynamic) run successfully and complete within 2 minutes
- **SC-005**: DDL operations (CREATE/DROP SCHEMA, CREATE/DROP/ALTER TABLE) execute in under 1 second for empty operations
- **SC-006**: DML operations process 1000 rows in under 2 seconds for INSERT operations; test suite validates operations up to 100M rows
- **SC-007**: Point-in-time queries return results with no more than 10% performance degradation vs current queries
- **SC-008**: Endpoint discovery operations complete in under 500ms
- **SC-009**: All Airport features documented in user stories are testable through integration tests
- **SC-010**: Zero breaking changes to existing public APIs (catalog, auth, builder packages)

## Assumptions

- Repository will maintain Go 1.21+ compatibility
- GitHub Actions runners have network access for package downloads
- Examples require DuckDB 1.4+ with latest Airport extension installed
- DDL operations assume single-writer model (no distributed consensus)
- DDL operations support IF EXISTS and IF NOT EXISTS clauses (aligned with DuckDB Airport extension)
- DML operations assume row-level locking or optimistic concurrency
- DML operations support unlimited rows (implementation-defined limits); test suite validates up to 100M rows
- Time-travel assumes backing storage maintains history (not ephemeral)
- Endpoint discovery assumes stable network topology during query execution
- Arrow IPC format is used for all data exchange
- Geometry types use WKB (Well-Known Binary) encoding in Arrow extension types
- Geometry abstractions provided by github.com/paulmach/orb package
- Bearer token authentication already implemented and functional
- Serialization to Arrow IPC already implemented in `internal/serialize`
- Logging uses structured format (JSON) via standard library log/slog package

## Out of Scope

- Implementing distributed transactions or two-phase commit for DDL/DML
- Building web UI or admin dashboard for catalog management
- Implementing automated schema migration tools
- Adding support for SQL DDL/DML parsing (assumes pre-parsed commands)
- Implementing storage layer for persistent data (remains implementation detail)
- Building query optimizer or execution engine
- Adding metrics/observability beyond basic structured logging (log/slog with JSON format)
- Implementing authentication mechanisms beyond existing bearer token
- Creating language bindings for non-Go clients
- Building data replication or disaster recovery features

## Dependencies

- Apache Arrow Go v18 (already in use)
- DuckDB 1.4+ with latest Airport extension (for examples)
- github.com/paulmach/orb (geometry abstractions for geospatial types)
- GitHub Actions (for CI)
- Standard Go testing framework
- Existing catalog, auth, and serialize packages

## Risks

- **Risk**: Moving integration tests may temporarily break CI if paths are hard-coded
  - **Mitigation**: Update CI configuration and import paths in single atomic commit
- **Risk**: DuckDB examples require Airport extension which users may not have installed
  - **Mitigation**: Provide clear installation instructions for DuckDB 1.4+ and latest Airport extension in example README
- **Risk**: DDL operations without transactions may leave catalog in inconsistent state on failure
  - **Mitigation**: Document transaction limitations and provide rollback guidance
- **Risk**: Time-travel queries may have significant performance impact on large datasets
  - **Mitigation**: Document performance characteristics and recommend appropriate use cases
- **Risk**: Endpoint discovery in distributed environments may return stale information
  - **Mitigation**: Document endpoint TTL and refresh strategies

## Related Features

- Existing catalog package provides foundation for DDL operations
- Existing auth package provides bearer token authentication for secure examples
- Existing serialize package enables Arrow IPC format for DML data exchange
- Flight SQL protocol compatibility ensures standard client interoperability

## Notes

- This specification consolidates three distinct work streams (repo structure, examples, Airport features) into a single cohesive feature
- DDL/DML implementation will leverage existing `internal/serialize` for Arrow format handling
- Point-in-time queries require backing implementation to maintain historical versions
- Endpoint discovery aligns with Flight protocol's distributed query capabilities
- All P1 tasks should be completed before P2, and P2 before P3, to ensure solid foundation
