# Feature Specification: Airport Go Flight Server Package

**Feature Branch**: `001-001-flight-server`
**Created**: 2025-11-25
**Status**: Draft
**Input**: User description: "A Go package `airport` providing a high-level API for building Arrow Flight servers compatible with the DuckDB Airport Extension"

## Clarifications

### Session 2025-11-25

- Q: DuckDB connection management strategy for the server implementation → A: DuckDB is used only for integration testing (install and load airport extensions, attach running test service, run test queries). Server does not manage DuckDB connections. For concurrent testing, multiple in-memory DuckDB instances can attach to the test service simultaneously.
- Q: Arrow record batch size strategy for streaming → A: Batch size is defined by client or developer implementing handlers. Handlers provide `arrow.RecordReader` for output data; package handles transport only without controlling batching.
- Q: TLS/SSL certificate management and server lifecycle → A: Package does not manage gRPC server or listener. User provides their own `grpc.Server` instance; package registers Flight service handlers on it. TLS configured on user's `grpc.Server` (e.g., via `grpc.Creds()`). Supports containerized deployments where TLS termination happens at ingress (Traefik, Nginx).
- Q: Error handling and logging strategy → A: Package uses global `slog` default logger for all internal logging. Users can configure via `slog.SetDefault()` before calling package APIs.
- Q: Catalog scope and mutability → A: Catalog is an interface with methods to return available schemas (also interfaces). Implementations can return different results over time (dynamic catalogs). CatalogBuilder produces one implementation; users can provide custom implementations.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Basic Flight Server Setup (Priority: P1)

As a Go developer, I want to register Airport Flight handlers on my existing gRPC server so that I can quickly expose data over the Flight protocol with minimal configuration.

**Why this priority**: This is the core MVP that enables developers to get started with the package. Without this, no other functionality matters.

**Independent Test**: Can be fully tested by creating a `grpc.Server`, calling `airport.NewServer(grpcServer, config)` to register handlers, starting the gRPC server with a listener, and verifying it accepts Flight RPC connections from DuckDB clients. Delivers a working Flight service registration.

**Acceptance Scenarios**:

1. **Given** I have a `grpc.Server` instance, **When** I call `airport.NewServer(grpcServer, config)`, **Then** Flight service handlers should be registered on the gRPC server without errors
2. **Given** I have registered Airport handlers, **When** I call `grpcServer.Serve(listener)`, **Then** the server should accept Flight RPC connections from clients (e.g., DuckDB with Airport extension)
3. **Given** a running gRPC server with Airport handlers, **When** clients send Flight requests, **Then** the registered catalog and handlers should be invoked
4. **Given** a running gRPC server, **When** I call `grpcServer.GracefulStop()`, **Then** the server should gracefully shut down, completing in-flight requests

---

### User Story 2 - Query Execution with Arrow Results (Priority: P1)

As a Go developer, I want to define table scan functions that return `arrow.RecordReader` so that Flight clients can query my data sources efficiently.

**Why this priority**: This is the primary use case for the package - defining data sources via scan functions that Flight clients (like DuckDB) can query. This must work for the package to be useful.

**Independent Test**: Can be tested by defining a table with a scan function that returns an `arrow.RecordReader`, registering it in the catalog, starting the gRPC server, connecting via DuckDB Airport extension, and verifying that Arrow record batches are correctly streamed to the client. Delivers working query handling with Arrow streaming.

**Acceptance Scenarios**:

1. **Given** a table with a registered scan function, **When** a client sends a `DoGet` request for that table, **Then** the server should invoke the scan function and stream the `arrow.RecordReader` batches to the client
2. **Given** a scan function returns an `arrow.RecordReader` with multiple batches, **When** streaming to the client, **Then** results should be sent batch-by-batch as provided by the reader (batching controlled by scan function implementation)
3. **Given** a scan function returns an error, **When** processing the request, **Then** the server should return a clear error message to the client via the Flight protocol
4. **Given** a client cancels the request, **When** streaming is in progress, **Then** the server should respect context cancellation and stop processing immediately

---

### User Story 3 - Catalog Discovery (Priority: P2)

As a client application, I want to discover available schemas, tables, and functions through Flight's `ListFlights` and `GetFlightInfo` RPCs so that I can explore what data is available without prior knowledge.

**Why this priority**: Catalog discovery enables dynamic exploration of data sources. While important for production use, a server can function with hardcoded queries if clients know the catalog in advance.

**Independent Test**: Can be tested by building a catalog with schemas and tables using the builder API, registering it with the server, and verifying that `ListFlights` returns serialized catalog information and `GetFlightInfo` returns metadata for specific tables.

**Acceptance Scenarios**:

1. **Given** a server with a catalog containing schemas and tables, **When** a client calls `ListFlights`, **Then** the server should return catalog information (schemas, tables, columns, types) serialized with ZStandard compression
2. **Given** a catalog listing is requested, **When** the response is sent, **Then** it should include schema names, table names, column names, column types, and comments
3. **Given** a client requests flight info for a specific table, **When** `GetFlightInfo` is called, **Then** the server should return schema information and ticket for `DoGet`
4. **Given** a catalog includes scalar functions, **When** `ListFlights` is called, **Then** function definitions should be included in the catalog response
5. **Given** a dynamic catalog implementation, **When** `ListFlights` is called multiple times, **Then** results may differ based on the catalog's current state (e.g., tables added/removed)

---

### User Story 4 - Authentication and Authorization (Priority: P2)

As a server administrator, I want to configure authentication (e.g., bearer token) so that I can control who accesses the server and what queries they can execute.

**Why this priority**: Security is important for production deployments but not required for initial testing or development scenarios. Can be added after core query execution works.

**Independent Test**: Can be tested by configuring `airport.BearerAuth()` in the server config, starting the server, and verifying that unauthenticated requests are rejected while authenticated requests succeed.

**Acceptance Scenarios**:

1. **Given** a server with bearer token authentication enabled, **When** a client connects without a valid token, **Then** the server should reject the connection with an authentication error
2. **Given** a client provides a valid bearer token, **When** connecting to an authenticated server, **Then** the client should be able to execute queries
3. **Given** an authenticated client, **When** executing a query, **Then** the authentication function should validate the token and return user identity
4. **Given** token validation fails, **When** a query is attempted, **Then** the server should return an unauthorized error without executing the query

---

### User Story 5 - Parameterized Queries (Priority: P3)

As a client application, I want to send parameterized queries using the `DoPut` RPC so that I can safely pass user input and enable efficient query execution.

**Why this priority**: Parameterized queries improve security and performance but are not essential for basic read-only query scenarios. Can be implemented after core functionality is stable.

**Independent Test**: Can be tested by sending a `DoPut` request with MessagePack-serialized parameters, verifying the server deserializes them correctly, passes them to the appropriate handler, and returns results.

**Acceptance Scenarios**:

1. **Given** a client wants to run a parameterized query, **When** sending a `DoPut` request with MessagePack-encoded parameters, **Then** the server should deserialize parameters and pass them to the query handler
2. **Given** parameters are deserialized, **When** the handler processes the request, **Then** it should receive strongly-typed parameters
3. **Given** a parameterized query execution, **When** results are available, **Then** the server should stream Arrow record batches back through the `DoPut` response
4. **Given** parameter deserialization fails, **When** processing a `DoPut` request, **Then** the server should return a clear error indicating invalid parameter format

---

### User Story 6 - Custom Scalar Functions (Priority: P3)

As a Go developer, I want to register custom scalar functions in the catalog so that DuckDB clients can call them in queries.

**Why this priority**: Custom functions enable domain-specific transformations but are not required for basic data retrieval. Most users will start with table scans only.

**Independent Test**: Can be tested by implementing a scalar function (e.g., `UPPERCASE()`), registering it in the catalog builder, and verifying DuckDB can call it in queries via Flight.

**Acceptance Scenarios**:

1. **Given** a custom scalar function is registered in the catalog, **When** a client calls `ListFlights`, **Then** the function should appear in the catalog with its signature
2. **Given** a registered scalar function, **When** a client invokes it in a query (e.g., `SELECT UPPERCASE(name) FROM users`), **Then** the function should be executed and results returned
3. **Given** a scalar function execution fails, **When** processing the call, **Then** the server should return a clear error message
4. **Given** function parameters have incorrect types, **When** the function is called, **Then** the server should return a type mismatch error

---

### User Story 7 - Dynamic Catalog Implementation (Priority: P3)

As an advanced Go developer, I want to implement the `Catalog` interface with custom logic so that I can provide dynamic schemas (e.g., reflecting database state, permissions-based filtering).

**Why this priority**: Dynamic catalogs enable advanced use cases (live schema reflection, multi-tenancy) but are not required for basic static data sources. Most users will use the builder API.

**Independent Test**: Can be tested by implementing a custom `Catalog` interface that returns different schemas based on context or state, registering it with the server, and verifying that `ListFlights` reflects the dynamic behavior.

**Acceptance Scenarios**:

1. **Given** a custom catalog implementation, **When** the catalog interface methods are called, **Then** the implementation should be invoked instead of the builder-based catalog
2. **Given** a dynamic catalog that changes over time, **When** `ListFlights` is called at different times, **Then** the returned schemas/tables should reflect the current state
3. **Given** a permission-aware catalog, **When** different authenticated users call `ListFlights`, **Then** each user should see only their authorized schemas/tables
4. **Given** catalog methods return errors, **When** Flight RPCs invoke them, **Then** errors should be propagated to clients via Flight protocol

---

### Edge Cases

- What happens when a scan function times out during execution? (Server should respect context deadlines and return timeout errors)
- How does the system handle concurrent queries from multiple clients? (Server should handle concurrent Flight RPC requests safely using goroutines per gRPC connection)
- What happens when Arrow schema inference fails for a scan result? (Server should return schema error before attempting to stream data)
- What happens when an `arrow.RecordReader` produces an error during iteration? (Server should detect the error and propagate it via Flight protocol)
- What happens when a client disconnects mid-stream? (gRPC should detect broken connections and clean up resources)
- What happens when a scan function returns an error instead of a reader? (Server should return clear errors via Flight protocol)
- What happens when catalog building fails due to invalid schema definitions? (Builder should return validation errors before server registration)
- How does the system handle catalog queries when no catalog is provided? (Server should return empty catalog or appropriate error)
- What happens when the user's `grpc.Server` is configured with TLS? (Package should work transparently; TLS handled by gRPC layer)
- How are internal errors logged when `slog` default logger is not configured? (Package uses `slog.Default()` which defaults to text handler writing to stderr)
- What happens when a dynamic catalog's methods are slow or block? (Server should apply context deadlines to catalog method calls to prevent hanging)
- What happens when a catalog interface method panics? (Package should recover panics, log error, and return internal error to client)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Package MUST provide `NewServer(grpcServer *grpc.Server, config ServerConfig)` function that registers Flight service handlers on the provided gRPC server
- **FR-002**: Package MUST NOT manage gRPC server lifecycle (start/stop/listen) - user controls this via their `grpc.Server` instance
- **FR-003**: Package MUST define `Catalog` interface with methods to query available schemas, tables, and functions
- **FR-004**: Package MUST provide `NewCatalogBuilder()` API that produces a `Catalog` implementation via fluent builder pattern
- **FR-005**: Package MUST allow users to provide custom `Catalog` implementations that can return dynamic results
- **FR-006**: Package MUST support registering table scan functions that return `arrow.RecordReader` for streaming Arrow IPC record batches
- **FR-007**: Package MUST support context-based cancellation for all long-running operations (scan functions, streaming, catalog queries)
- **FR-008**: Package MUST implement Flight RPC methods: `GetFlightInfo`, `DoGet`, `DoPut`, `ListFlights`, `DoAction`
- **FR-009**: Package MUST serialize catalog information (schemas, tables, columns, functions) using ZStandard compression for `ListFlights` responses
- **FR-010**: Package MUST deserialize MessagePack-encoded parameters from `DoPut` requests and pass to handlers
- **FR-011**: Package MUST support bearer token authentication via `BearerAuth(validateFunc)` configuration option
- **FR-012**: Package MUST return idiomatic Go errors (wrapped errors, not panics) for all failure scenarios
- **FR-013**: Package MUST support graceful shutdown via `grpc.Server.GracefulStop()` (no package-specific shutdown logic needed)
- **FR-014**: Package MUST log errors and important events using `slog.Default()` for all internal logging
- **FR-015**: Package MUST work transparently with user-configured TLS on `grpc.Server` (no TLS management in package)
- **FR-016**: Package MUST validate Arrow schema compatibility before streaming results from `arrow.RecordReader`
- **FR-017**: Package MUST handle concurrent Flight RPC requests safely (goroutines managed by gRPC)
- **FR-018**: Package MUST stream record batches from `arrow.RecordReader` as-is without rebatching (batching controlled by scan function implementation)
- **FR-019**: Package MUST support registering custom scalar functions in catalog that can be invoked from client queries
- **FR-020**: Package MUST allow comments on schemas, tables, columns, and functions for documentation
- **FR-021**: Package MUST allow users to configure logging via `slog.SetDefault()` before package initialization
- **FR-022**: Package MUST support dynamic catalogs where schema/table availability can change between calls
- **FR-023**: Package MUST apply context deadlines to catalog interface method calls to prevent indefinite blocking
- **FR-024**: Package MUST recover from panics in user-provided catalog implementations and return errors to clients

### Key Entities

- **ServerConfig**: Configuration structure passed to `NewServer()` containing catalog, auth settings, and optional configurations
- **Catalog**: Interface defining methods to query available schemas (e.g., `Schemas(ctx context.Context) ([]Schema, error)`). Implementations can be static (builder-based) or dynamic (database-backed, permission-aware).
- **Schema**: Interface representing a database schema with methods to query tables and functions within that schema
- **CatalogBuilder**: Fluent builder API for constructing a static `Catalog` implementation with method chaining (`.Schema().Table().ScalarFunc().Build()`)
- **SimpleTableDef**: Table definition structure containing name, schema (`arrow.Schema`), scan function, and optional comment
- **ScanFunc**: Function type `func(ctx context.Context) (arrow.RecordReader, error)` that user implements to provide table data
- **BearerAuth**: Authentication function type `func(token string) (userID string, error)` for validating bearer tokens
- **FlightDescriptor**: Flight protocol type representing a query or data location - used in `GetFlightInfo` and `DoGet` requests
- **Ticket**: Opaque byte slice representing a query handle - issued by `GetFlightInfo` and redeemed in `DoGet`

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Developers can register Flight handlers on a gRPC server and serve their first query in under 30 lines of Go code using the catalog builder API
- **SC-002**: Package achieves 80% or higher test coverage across all public APIs
- **SC-003**: Server handles at least 100 concurrent client connections without memory leaks or race conditions (verified with race detector)
- **SC-004**: Integration tests successfully connect DuckDB Airport extension to test server, execute queries, and verify Arrow data correctness
- **SC-005**: Package documentation includes runnable examples for common use cases (basic table, authentication, custom functions, dynamic catalog)
- **SC-006**: Package passes all `golangci-lint` checks with no warnings and follows Go conventions
- **SC-007**: Package works correctly with user-configured TLS on `grpc.Server` (tested with self-signed certs and mTLS)
- **SC-008**: Package preserves batching from `arrow.RecordReader` without unnecessary memory overhead (measured via memory profiling)
- **SC-009**: Package can be imported and used in external projects without dependency conflicts
- **SC-010**: All public APIs have godoc-compatible documentation comments with examples
- **SC-011**: Internal logging via `slog.Default()` is testable and configurable by users (verified in tests with custom handler)
- **SC-012**: Custom catalog implementations can be tested with mock implementations that verify interface contract
