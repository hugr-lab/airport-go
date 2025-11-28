# Feature Specification: DML Operations and Transaction Management

**Feature Branch**: `002-dml-transactions`
**Created**: 2025-11-28
**Status**: Draft
**Input**: User description: "DML operations implementation - Add Insert, Update, Delete operations support with transaction management"

## User Scenarios & Testing

### User Story 1 - Insert Data into Tables (Priority: P1)

Application developers need to insert new data rows into Airport-managed tables through their DuckDB connection, enabling them to populate tables with fresh data from their applications.

**Why this priority**: This is the foundation of data modification - without the ability to insert data, tables remain empty and unusable for most applications.

**Independent Test**: Can be fully tested by creating a table, inserting rows, and verifying the data appears in subsequent queries. Delivers immediate value by enabling data population.

**Acceptance Scenarios**:

1. **Given** a writable Airport-managed table with defined columns, **When** a developer executes an INSERT statement with matching column values, **Then** the rows are successfully stored and the operation returns the count of inserted rows
2. **Given** an INSERT statement with a RETURNING clause, **When** the operation completes, **Then** the system returns the inserted rows including any server-generated values (such as auto-incremented IDs)
3. **Given** a view or read-only table, **When** a developer attempts an INSERT operation, **Then** the system rejects the operation with a clear error message indicating the table does not support insertions

---

### User Story 2 - Update Existing Data (Priority: P2)

Application developers need to modify existing rows in Airport-managed tables to reflect changes in their data, such as updating user profiles or correcting information.

**Why this priority**: Updates are essential for maintaining accurate data but depend on having data to modify first (hence P2 after INSERT).

**Independent Test**: Can be tested by inserting test data, executing an UPDATE with a WHERE clause, and verifying the modified rows reflect the changes. Works independently once INSERT is available.

**Acceptance Scenarios**:

1. **Given** a table with a rowid pseudocolumn and existing data, **When** a developer executes an UPDATE statement with a WHERE clause, **Then** only the matching rows are modified and the operation returns the count of updated rows
2. **Given** an UPDATE statement with a RETURNING clause, **When** the operation completes, **Then** the system returns the updated rows which may include server-modified values
3. **Given** a table without a rowid pseudocolumn, **When** a developer attempts an UPDATE operation, **Then** the system rejects the operation with an error indicating the table does not support updates

---

### User Story 3 - Delete Unwanted Data (Priority: P3)

Application developers need to remove rows from Airport-managed tables to delete obsolete or incorrect data, maintaining data quality and compliance requirements.

**Why this priority**: Deletion is important for data management but is typically less frequent than inserts and updates in most applications.

**Independent Test**: Can be tested by inserting test data, executing a DELETE with a WHERE clause, and verifying the rows no longer appear in queries. Works independently once INSERT is available.

**Acceptance Scenarios**:

1. **Given** a table with a rowid pseudocolumn and existing data, **When** a developer executes a DELETE statement with a WHERE clause, **Then** only the matching rows are removed and the operation returns the count of deleted rows
2. **Given** a DELETE statement with a RETURNING clause, **When** the operation completes, **Then** the system returns information about the deleted rows before removal
3. **Given** a table without a rowid pseudocolumn, **When** a developer attempts a DELETE operation, **Then** the system rejects the operation with an error indicating the table does not support deletions

---

### User Story 4 - Coordinate Related Operations (Priority: P2)

Application developers need to ensure multiple related data operations maintain consistency, such as updating inventory across multiple tables or ensuring related records are processed together.

**Why this priority**: Transaction coordination is critical for data consistency in multi-step operations, making it as important as individual update operations.

**Independent Test**: Can be tested by configuring a transaction manager, executing multiple operations with the same transaction ID, and verifying they maintain consistency. Delivers value by enabling reliable multi-step workflows.

**Acceptance Scenarios**:

1. **Given** a configured transaction manager, **When** an application requests a new transaction, **Then** the system returns a unique transaction identifier
2. **Given** an active transaction ID, **When** data operations (GET, PUT, EXCHANGE) include the transaction ID in headers, **Then** the system ensures all operations see consistent data within that transaction scope
3. **Given** an operation fails during transaction execution, **When** the error is caught by the handler, **Then** the system automatically rolls back the transaction and notifies the transaction manager
4. **Given** an operation completes successfully, **When** no errors occurred, **Then** the system automatically commits the transaction and notifies the transaction manager
5. **Given** application code executing within a request, **When** it needs to check transaction status, **Then** helper functions indicate whether a transaction is active, aborted, or committed based on the request context

---

### Edge Cases

- What happens when a client attempts DML operations on a view or read-only table?
- How does the system handle INSERT/UPDATE/DELETE when the server fails mid-operation?
- What occurs if UPDATE or DELETE is attempted on a table without a rowid pseudocolumn?
- How does the system respond when a transaction ID is provided but no transaction manager is configured?
- What happens if an operation references a transaction ID that has already been committed or rolled back?
- How does the system handle concurrent operations with the same transaction ID?

## Requirements

### Functional Requirements

- **FR-001**: System MUST support INSERT operations on writable Airport-managed tables
- **FR-002**: System MUST return the count of successfully inserted rows after INSERT operations
- **FR-003**: System MUST support RETURNING clauses in INSERT statements to retrieve inserted rows with server-generated values
- **FR-004**: System MUST support UPDATE operations on tables that have a rowid pseudocolumn
- **FR-005**: System MUST return the count of successfully updated rows after UPDATE operations
- **FR-006**: System MUST support RETURNING clauses in UPDATE statements to retrieve modified rows
- **FR-007**: System MUST support DELETE operations on tables that have a rowid pseudocolumn
- **FR-008**: System MUST return the count of successfully deleted rows after DELETE operations
- **FR-009**: System MUST support RETURNING clauses in DELETE statements to retrieve information about deleted rows
- **FR-010**: System MUST reject INSERT/UPDATE/DELETE operations on views and read-only tables with clear error messages
- **FR-011**: System MUST reject UPDATE operations on tables without a rowid pseudocolumn with clear error messages
- **FR-012**: System MUST reject DELETE operations on tables without a rowid pseudocolumn with clear error messages
- **FR-013**: Server MUST accept an optional transaction manager interface for coordinating transactions
- **FR-014**: System MUST generate unique transaction identifiers when create_transaction action is called and a transaction manager is configured
- **FR-015**: System MUST include transaction identifiers in operation contexts when provided in request headers
- **FR-016**: System MUST invoke transaction manager rollback when operations fail
- **FR-017**: System MUST invoke transaction manager commit when operations complete successfully
- **FR-018**: System MUST provide helper functions to check transaction status from request context (active, aborted, committed)
- **FR-019**: System MUST handle operations correctly when no transaction manager is configured (allowing operations without transaction coordination)

### Key Entities

- **Table Capability**: Represents what operations a table supports (insertable, updatable, deletable) based on its characteristics (read-only status, rowid presence)
- **Transaction**: Represents a coordinated set of operations with a unique identifier, tracking its state (active, committed, aborted)
- **DML Operation**: Represents a data modification request (INSERT, UPDATE, DELETE) with associated data rows and optional transaction context
- **Operation Result**: Contains outcome information including affected row count and optionally returned row data

## Success Criteria

### Measurable Outcomes

- **SC-001**: Developers can insert data into Airport-managed tables and receive confirmation of successful storage
- **SC-002**: Developers can update existing rows in tables with rowid support and receive the count of modified rows
- **SC-003**: Developers can delete rows from tables with rowid support and receive the count of removed rows
- **SC-004**: System correctly rejects unsupported operations (updates/deletes on tables without rowid, modifications on views) with informative error messages in 100% of cases
- **SC-005**: When a transaction manager is configured, all operations complete with proper commit or rollback handling
- **SC-006**: Applications can coordinate multiple related operations using transaction identifiers with consistent data visibility
- **SC-007**: Transaction status can be checked from any point in application code during request processing

## Assumptions

- **A-001**: The Airport Flight RPC protocol implementation already exists and handles DoExchange calls
- **A-002**: Table metadata is available to determine if a table has a rowid pseudocolumn
- **A-003**: Error handling infrastructure exists to propagate Arrow Flight exceptions to clients
- **A-004**: The transaction manager interface (if provided) is implemented by users and handles concurrent transaction coordination
- **A-005**: Server-side storage implementation determines actual persistence guarantees; the Airport server acts as a protocol coordinator

## Scope

### In Scope

- INSERT operation support for writable tables
- UPDATE operation support for tables with rowid pseudocolumns
- DELETE operation support for tables with rowid pseudocolumns
- RETURNING clause support for all DML operations
- Optional transaction manager integration
- Transaction identifier propagation through operation contexts
- Automatic commit/rollback handling
- Transaction status checking helpers
- Clear error messages for unsupported operations

### Out of Scope

- Traditional ACID transaction guarantees (delegated to server-side storage)
- Multi-statement transaction batching
- Transaction isolation level configuration
- Savepoint support
- Distributed transaction coordination across multiple Airport servers
- Automatic retry logic for failed operations
- Transaction timeout enforcement (delegated to transaction manager implementation)

## Dependencies

- **D-001**: Arrow Flight RPC infrastructure for DoExchange operations
- **D-002**: Table catalog interface for querying table capabilities (rowid presence, read-only status)
- **D-003**: msgpack serialization for operation metadata messages
- **D-004**: Context propagation mechanism for passing transaction identifiers through request lifecycle
