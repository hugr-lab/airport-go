# Feature Specification: Multi-Catalog Server Support

**Feature Branch**: `001-multicatalog-server`
**Created**: 2026-01-08
**Status**: Draft
**Input**: User description: "Add multicatalog server support. Create a new flight server implementation - MultiCatalogServer, that accepts a number of existing flight.Server instances, checks that they have unique CatalogNames. Dispatch requests (DoAction, DoExchange, DoGet, etc.) to the right server based on metadata header airport-catalog. Support auth implementation that knows about catalog name to authorize requests. Also add airport-trace-id and airport-client-session-id from metadata to context."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Multi-Catalog Request Routing (Priority: P1)

As a client application, I want to connect to a single server endpoint and access multiple catalogs by specifying which catalog I want to interact with, so that I can manage multiple data sources through a unified connection without needing separate connections for each catalog.

**Why this priority**: This is the core functionality of the feature. Without request routing to the appropriate catalog server, the multi-catalog server has no value. This enables the primary use case of consolidating multiple catalog servers behind a single endpoint.

**Independent Test**: Can be fully tested by sending requests with different `airport-catalog` header values and verifying each request is handled by the correct underlying catalog server. Delivers immediate value by enabling multi-catalog access through a single connection.

**Acceptance Scenarios**:

1. **Given** a MultiCatalogServer with catalogs "sales" and "analytics", **When** a client sends a DoGet request with header `airport-catalog: sales`, **Then** the request is processed by the "sales" catalog server.
2. **Given** a MultiCatalogServer with catalogs "sales" and "analytics", **When** a client sends a DoGet request with header `airport-catalog: analytics`, **Then** the request is processed by the "analytics" catalog server.
3. **Given** a MultiCatalogServer with a default catalog (empty name), **When** a client sends a request without the `airport-catalog` header, **Then** the request is processed by the default catalog server.
4. **Given** a MultiCatalogServer without a default catalog, **When** a client sends a request without the `airport-catalog` header, **Then** the server returns an appropriate error indicating no default catalog is available.

---

### User Story 2 - Catalog-Aware Authorization (Priority: P2)

As a security administrator, I want to implement per-catalog authorization after authentication, so that I can enforce fine-grained access control policies that vary by catalog (e.g., different permissions for sales data vs. analytics data).

**Why this priority**: Security is critical but builds upon the routing functionality. Without proper catalog-aware authorization, multi-catalog deployments cannot enforce appropriate access boundaries, which is essential for production use.

**Independent Test**: Can be tested by implementing CatalogAuthorizer with different rules per catalog and verifying that access is correctly granted or denied based on the catalog being accessed.

**Acceptance Scenarios**:

1. **Given** a MultiCatalogServer with an Authenticator that also implements CatalogAuthorizer, **When** a user with "sales-reader" role requests access to the "sales" catalog, **Then** Authenticate passes, AuthorizeCatalog passes, and the request proceeds.
2. **Given** a MultiCatalogServer with CatalogAuthorizer, **When** a user with "sales-reader" role requests access to the "analytics" catalog, **Then** Authenticate passes but AuthorizeCatalog returns PermissionDenied.
3. **Given** a MultiCatalogServer with an Authenticator that does NOT implement CatalogAuthorizer, **When** any authenticated user requests access to any catalog, **Then** only Authenticate is called and the request proceeds.

---

### User Story 3 - Request Tracing Context Propagation (Priority: P3)

As a developer or operations engineer, I want trace IDs and session IDs from client requests to be propagated through the server, so that I can correlate requests across distributed systems for debugging and monitoring purposes.

**Why this priority**: Observability is important for production operations but is not required for basic functionality. This enables distributed tracing and debugging capabilities.

**Independent Test**: Can be tested by sending requests with trace/session ID headers and verifying these values are accessible in the request context within catalog handlers.

**Acceptance Scenarios**:

1. **Given** a client sends a request with `airport-trace-id` header, **When** the request is processed, **Then** the trace ID is available in the request context.
2. **Given** a client sends a request with `airport-client-session-id` header, **When** the request is processed, **Then** the session ID is available in the request context.
3. **Given** a client sends a request without trace/session ID headers, **When** the request is processed, **Then** the context has empty values for these fields (request proceeds normally).

---

### User Story 4 - Dynamic Catalog Management (Priority: P1)

As a server administrator, I want to add and remove catalog servers at runtime without restarting the MultiCatalogServer, so that I can dynamically scale the system and perform maintenance on individual catalogs without affecting others.

**Why this priority**: This is essential for operational flexibility and is tied to P1 routing functionality. Dynamic management enables zero-downtime catalog updates and elastic scaling.

**Independent Test**: Can be tested by adding/removing catalogs at runtime and verifying requests are routed correctly to newly added catalogs and fail gracefully for removed catalogs.

**Acceptance Scenarios**:

1. **Given** a running MultiCatalogServer with catalog "sales", **When** AddCatalog is called with a new "analytics" server, **Then** subsequent requests with `airport-catalog: analytics` are routed to the new server.
2. **Given** a running MultiCatalogServer with catalogs "sales" and "analytics", **When** RemoveCatalog is called for "analytics", **Then** subsequent requests for "analytics" return a "catalog not found" error.
3. **Given** a running MultiCatalogServer with catalog "sales", **When** AddCatalog is called with another server also named "sales", **Then** an error is returned indicating duplicate catalog name.
4. **Given** a running MultiCatalogServer, **When** RemoveCatalog is called for a non-existent catalog, **Then** an error is returned indicating catalog not found.
5. **Given** two flight.Server instances both with empty names (default), **When** adding the second one via AddCatalog, **Then** an error is returned indicating duplicate default catalogs.

---

### User Story 5 - Server Registration Validation (Priority: P1)

As a server administrator, I want the MultiCatalogServer to validate catalog name uniqueness both at construction and when adding catalogs, so that request routing is always deterministic and configuration errors are caught immediately.

**Why this priority**: This is essential for correct operation and is tied to P1 routing functionality. Without uniqueness validation, routing would be ambiguous and could lead to data corruption or incorrect results.

**Independent Test**: Can be tested by attempting to create a MultiCatalogServer with duplicate catalog names and verifying an error is returned.

**Acceptance Scenarios**:

1. **Given** two catalogs both named "sales", **When** creating a MultiCatalogServer with both, **Then** an error is returned indicating duplicate catalog names.
2. **Given** catalogs with unique names "sales", "analytics", and "" (default), **When** creating a MultiCatalogServer, **Then** the server is created successfully.

---

### User Story 6 - Catalog-Aware Transaction Management (Priority: P2)

As a developer using transactions, I want the transaction manager to know which catalog a transaction belongs to, so that commits and rollbacks are correctly routed to the appropriate catalog backend.

**Why this priority**: Transaction support is important for data integrity but builds on the core routing functionality. Without catalog-aware transactions, multi-catalog deployments cannot support transactional operations correctly.

**Independent Test**: Can be tested by starting transactions in different catalogs and verifying commit/rollback operations affect the correct catalog.

**Acceptance Scenarios**:

1. **Given** a MultiCatalogServer with CatalogTransactionManager, **When** a client begins a transaction for the "sales" catalog, **Then** the transaction is associated with "sales".
2. **Given** an active transaction for "sales" catalog, **When** the transaction is committed, **Then** the commit is executed on the "sales" catalog backend.
3. **Given** an active transaction for "analytics" catalog, **When** the transaction is rolled back, **Then** the rollback is executed on the "analytics" catalog backend.

---

### User Story 7 - High-Level Server Creation API (Priority: P1)

As a developer, I want a simple `NewMultiCatalogServer` function similar to the existing `NewServer`, so that I can create a multi-catalog server by providing catalogs and configuration without manually managing flight.Server instances.

**Why this priority**: Developer experience is critical. A high-level API that mirrors the existing pattern reduces adoption friction and potential for misuse.

**Independent Test**: Can be tested by creating a multi-catalog server with the high-level API and verifying all catalogs are accessible.

**Acceptance Scenarios**:

1. **Given** a list of catalogs and a gRPC server, **When** NewMultiCatalogServer is called with MultiCatalogServerConfig, **Then** the multi-catalog server is created and registered.
2. **Given** a MultiCatalogServerConfig with auth, logger, and transaction manager, **When** NewMultiCatalogServer is called, **Then** all configuration options are applied correctly.

---

### Edge Cases

- What happens when a request specifies a catalog name that doesn't exist? Server returns a "catalog not found" error with appropriate gRPC status code.
- What happens if a catalog is removed while a request is in progress? In-flight requests to the removed catalog complete normally; only new requests fail.
- What happens if the `airport-catalog` header contains multiple values? The first value is used.
- How are catalog names compared (case sensitivity)? Catalog names are compared case-sensitively as exact string matches.
- What happens if AddCatalog/RemoveCatalog is called concurrently with requests? Operations are thread-safe; catalog lookups use the state at the time of lookup.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept multiple flight.Server instances during MultiCatalogServer construction.
- **FR-002**: System MUST validate that all provided servers have unique catalog names (including empty string for default).
- **FR-003**: System MUST return an error during construction if duplicate catalog names are detected.
- **FR-004**: System MUST provide an AddCatalog operation to register a new flight.Server at runtime.
- **FR-005**: System MUST return an error from AddCatalog if the catalog name already exists.
- **FR-006**: System MUST provide a RemoveCatalog operation to unregister a flight.Server by catalog name at runtime.
- **FR-007**: System MUST return an error from RemoveCatalog if the catalog name does not exist.
- **FR-008**: System MUST ensure AddCatalog and RemoveCatalog operations are thread-safe and do not corrupt state during concurrent access.
- **FR-009**: System MUST read the `airport-catalog` metadata header from incoming gRPC requests.
- **FR-010**: System MUST route requests to the appropriate flight.Server based on the `airport-catalog` header value.
- **FR-011**: System MUST use empty string as the catalog name when `airport-catalog` header is not provided.
- **FR-012**: System MUST return a "catalog not found" error when the requested catalog name does not exist.
- **FR-013**: System MUST support routing for all Flight RPC methods: DoAction, DoExchange, DoGet, GetFlightInfo, ListFlights, GetSchema.
- **FR-014**: System MUST extract `airport-trace-id` from metadata and make it available in the request context.
- **FR-015**: System MUST extract `airport-client-session-id` from metadata and make it available in the request context.
- **FR-016**: System MUST support a CatalogAuthorizer interface with `AuthorizeCatalog(ctx, catalog, token) (context.Context, error)` for per-catalog authorization.
- **FR-017**: System MUST call AuthorizeCatalog after Authenticate if the Authenticator also implements CatalogAuthorizer.
- **FR-018**: System MUST support a CatalogTransactionManager interface where BeginTransaction receives the catalog name.
- **FR-019**: System MUST route transaction commit/rollback operations to the correct catalog based on stored transaction metadata.
- **FR-020**: System MUST provide a high-level NewMultiCatalogServer function that accepts catalogs and configuration (similar to existing NewServer).
- **FR-021**: System MUST create internal flight.Server instances for each catalog when using the high-level API.

### Key Entities

- **MultiCatalogServer**: The main server implementation that aggregates multiple flight.Server instances and routes requests based on catalog name. Holds a thread-safe map of catalog name to server instance. Supports dynamic addition and removal of catalogs via AddCatalog/RemoveCatalog operations.
- **MultiCatalogServerConfig**: Configuration struct for the high-level API, containing catalogs, allocator, logger, address, transaction manager, and authenticator (typed as `auth.Authenticator`).
- **CatalogAuthorizer**: An optional interface that Authenticator implementations can also implement. Provides `AuthorizeCatalog(ctx, catalog, token) (context.Context, error)` for per-catalog authorization after authentication.
- **CatalogTransactionManager**: An extended transaction manager interface that tracks which catalog each transaction belongs to, enabling proper routing of commit/rollback operations.
- **RequestContext Extensions**: Context values for trace ID, session ID, and catalog name, following the existing pattern in the auth package for identity propagation.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Clients can access any registered catalog through a single server connection by specifying the `airport-catalog` header.
- **SC-002**: Configuration errors (duplicate catalog names) are detected and reported immediately, both at construction and during AddCatalog operations.
- **SC-003**: Authorization decisions can be made per-catalog, enabling differentiated access control across data sources.
- **SC-004**: Trace and session IDs are propagated through the request lifecycle, enabling end-to-end request correlation in monitoring systems.
- **SC-005**: All existing Flight RPC operations (DoGet, DoAction, DoExchange, etc.) work correctly through the MultiCatalogServer without modification to client code beyond adding the catalog header.
- **SC-006**: Catalogs can be added and removed at runtime without server restart, enabling zero-downtime catalog management.

## Assumptions

- The existing `flight.Server` implementation and its methods remain unchanged; MultiCatalogServer wraps and delegates to them.
- Catalog names are stable identifiers; a server's catalog name does not change after registration.
- The first value is used when multiple values are present in metadata headers (consistent with gRPC conventions).
- The gRPC metadata extraction follows the standard `metadata.FromIncomingContext(ctx)` pattern.
- Transactions are scoped to a single catalog; cross-catalog transactions are out of scope.
- In-flight requests to a catalog being removed will complete normally; only new requests will fail after removal.
- The high-level API follows the pattern of existing `NewServer` function in the root package.
