# Feature Specification: Module Reorganization

**Feature Branch**: `005-module-reorganization`
**Created**: 2025-11-29
**Status**: Complete
**Input**: User description: "Module reorganization: separate go.mod for examples, integration tests, and benchmarks. Remove unused code and packages. Create comprehensive protocol documentation."

## Clarifications

### Session 2025-11-29

- Q: Should each example have its own go.mod or share a single go.mod? → A: Single go.mod at examples/ covering all example subdirectories
- Q: Should integration tests and benchmarks share a single tests/ go.mod or have separate modules? → A: Single go.mod at tests/ covering both integration and benchmarks

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Isolated Example Development (Priority: P1)

A developer wants to explore airport-go examples without having DuckDB and test dependencies pollute their own project's go.mod when importing the library.

**Why this priority**: The main library should have minimal dependencies. Examples and tests require heavy dependencies (DuckDB, test frameworks) that should not be pulled into user projects. This directly affects adoption and developer experience.

**Independent Test**: After reorganization, running `go mod download` on the main module should not pull DuckDB or test-specific dependencies. Examples should compile and run independently with their own go.mod.

**Acceptance Scenarios**:

1. **Given** a developer imports `github.com/hugr-lab/airport-go`, **When** they run `go mod download`, **Then** only Arrow-Go, gRPC, msgpack, and compression libraries are downloaded (no DuckDB).
2. **Given** a developer clones the repository, **When** they navigate to `examples/basic` and run `go run .`, **Then** the example compiles and runs using its own go.mod.
3. **Given** a developer modifies an example, **When** they run `go mod tidy` in the example directory, **Then** the main module's go.mod is unaffected.

---

### User Story 2 - Independent Integration Testing (Priority: P1)

A contributor wants to run integration tests that require DuckDB without affecting the main library's dependencies or having test utilities leak into the public API.

**Why this priority**: Integration tests require DuckDB as a client, which is a large dependency. Test utilities should be internal to tests, not part of the public package.

**Independent Test**: Integration tests should compile and run from `tests/integration/` with their own go.mod. The main module should not contain any `_test.go` files that require DuckDB.

**Acceptance Scenarios**:

1. **Given** a contributor runs `go test ./tests/integration/...`, **When** tests execute, **Then** they use DuckDB as a Flight client to verify server functionality.
2. **Given** the tests module has its own go.mod, **When** dependencies are updated in tests, **Then** the main library's go.mod remains unchanged.
3. **Given** a CI/CD pipeline, **When** integration tests are skipped, **Then** the main module builds without requiring DuckDB installation.

---

### User Story 3 - Clean Dependency Tree (Priority: P1)

A maintainer wants to remove unused code and packages from the main go.mod to reduce build times, minimize security surface, and improve maintainability.

**Why this priority**: Unused dependencies increase attack surface, slow down builds, and confuse developers about actual requirements. This is essential for library hygiene.

**Independent Test**: After cleanup, `go mod tidy` should not add back removed dependencies, and the existing unit tests should pass.

**Acceptance Scenarios**:

1. **Given** unused packages exist in go.mod, **When** the maintainer runs cleanup, **Then** only packages actually imported by the main library remain.
2. **Given** dead code exists (unreachable functions, unused types), **When** cleanup is complete, **Then** all remaining code is referenced by the public API or tests.
3. **Given** the roadmap lists future features, **When** code is removed, **Then** future-related code is preserved or documented for later implementation.

---

### User Story 4 - Benchmark Suite Development (Priority: P2)

A performance engineer wants to run benchmarks that use DuckDB as a client to measure realistic performance characteristics, with benchmarks organized separately from production code.

**Why this priority**: Benchmarks using real DuckDB clients provide accurate performance data, but should not bloat the main module. Lower priority because basic benchmarks already exist.

**Independent Test**: Benchmarks should run from `tests/benchmarks/` with their own go.mod and produce performance metrics using DuckDB as the Flight client.

**Acceptance Scenarios**:

1. **Given** benchmarks exist in `tests/benchmarks/`, **When** a developer runs `go test -bench=. ./tests/benchmarks/...`, **Then** benchmarks execute using DuckDB as a Flight client.
2. **Given** the current `benchmark_test.go` in root, **When** reorganization is complete, **Then** benchmarks are moved to `tests/benchmarks/` with proper DuckDB integration.
3. **Given** benchmark dependencies, **When** they're isolated in tests/benchmarks, **Then** the main module's dependencies are unaffected.

---

### User Story 5 - Comprehensive Protocol Documentation (Priority: P2)

A developer integrating airport-go wants comprehensive documentation explaining the Airport protocol, message formats, action types, and implementation patterns in one central location.

**Why this priority**: Good documentation accelerates adoption and reduces support burden. Lower priority because basic docs exist in README and GoDoc, but comprehensive docs improve developer experience.

**Independent Test**: A `docs/` folder should contain structured documentation covering protocol overview, API reference, implementation guides, and examples.

**Acceptance Scenarios**:

1. **Given** a new developer, **When** they browse `docs/`, **Then** they find a protocol overview explaining Flight actions and message formats.
2. **Given** documentation in `docs/`, **When** developers look for implementation guidance, **Then** they find guides for implementing Catalog, Schema, Table, and Dynamic interfaces.
3. **Given** existing README content, **When** docs are created, **Then** README remains concise with links to detailed docs.

---

### Edge Cases

- What happens when examples reference main module during development? (Go workspaces should handle this)
- How does the system handle circular dependencies between test utilities and production code?
- What happens if a dependency is used by both main module and tests? (Each module declares its own requirements)
- How are shared test utilities between integration tests and benchmarks handled?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Main module (`github.com/hugr-lab/airport-go`) MUST contain only core library code with minimal dependencies (Arrow-Go, gRPC, msgpack, compression)
- **FR-002**: Examples MUST have a single shared go.mod at `examples/` that imports the main module (all example subdirectories share this module)
- **FR-003**: Tests MUST have a single shared go.mod at `tests/` that imports the main module and DuckDB (covers both integration/ and benchmarks/ subdirectories)
- **FR-004**: Go workspace (go.work) MUST be configured to enable local development across all modules
- **FR-005**: Main module's go.mod MUST NOT include DuckDB or test-specific dependencies
- **FR-006**: Unused code and packages MUST be removed from the main module
- **FR-007**: Code related to future roadmap features MUST be preserved or documented
- **FR-008**: Comprehensive documentation MUST be created in a `docs/` folder
- **FR-009**: README MUST be updated to reflect new project structure and link to docs

### Key Entities

- **Main Module**: `github.com/hugr-lab/airport-go` - Core library with public API
- **Examples Module**: `github.com/hugr-lab/airport-go/examples` - Single shared module for all demonstration code (auth, basic, ddl, dml, dynamic, functions, timetravel, tls subdirectories)
- **Tests Module**: `github.com/hugr-lab/airport-go/tests` - Single shared module for DuckDB-based integration tests and benchmarks (integration/ and benchmarks/ subdirectories)
- **Go Workspace**: `go.work` file at repository root for local development

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Importing `github.com/hugr-lab/airport-go` downloads fewer than 50MB of dependencies (excluding DuckDB)
- **SC-002**: All existing integration tests pass after reorganization
- **SC-003**: All examples compile and run independently with their own go.mod
- **SC-004**: Main module has zero unused dependencies after `go mod tidy`
- **SC-005**: `docs/` folder contains at least: protocol overview, API guide, implementation guide
- **SC-006**: CI/CD can build main module without DuckDB installation
- **SC-007**: Developers can use `go work` for seamless local development across all modules

## Assumptions

- Go workspaces (go.work) are available (Go 1.18+) for multi-module development
- DuckDB is available as a test client via the duckdb-go package
- The repository uses a monorepo structure where all modules share the same git repository
- Examples are not versioned independently of the main library
- Integration tests and benchmarks are not published as importable packages

## Out of Scope

- Splitting the core library into multiple independent packages
- Creating a separate repository for examples
- Automated dependency update tooling (e.g., Dependabot configuration)
- Performance benchmarking targets or regression testing
- Documentation translation to other languages
