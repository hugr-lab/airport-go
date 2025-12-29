# Feature Specification: Batch Table Interfaces for Update and Delete

**Feature Branch**: `001-batch-table-interfaces`
**Created**: 2025-12-29
**Status**: Draft
**Input**: User description: "Add modern UpdatableBatchTable and DeletableBatchTable interfaces with Update and Delete methods without rowIDs parameter - rowid column embedded in RecordReader. Refactor handlers to support both interfaces with wrapper functions for backward compatibility. Update tests, documentation and examples."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Implement UpdatableBatchTable Interface (Priority: P1)

As a library developer implementing a custom table, I want to use a simpler Update interface where the rowid column is embedded in the RecordReader, so I don't have to handle separate rowIDs array synchronization with my update logic.

**Why this priority**: This is the core feature request. The new interface simplifies implementation by keeping rowid data with row data in a single RecordReader, eliminating the need to correlate separate rowIDs array with record batches.

**Independent Test**: Can be fully tested by implementing a table that uses UpdatableBatchTable interface and executing UPDATE SQL via DuckDB. Delivers simpler API for table implementers.

**Acceptance Scenarios**:

1. **Given** a table implementing UpdatableBatchTable, **When** an UPDATE SQL is executed via DuckDB Airport, **Then** the Update method receives a RecordReader containing both rowid column and data columns in a single stream.

2. **Given** a table implementing UpdatableBatchTable, **When** the Update method is called, **Then** the implementation can extract rowid values directly from the RecordReader's rowid column.

3. **Given** a table implementing UpdatableBatchTable with RETURNING clause, **When** Update completes, **Then** RETURNING data is properly sent back to the client.

---

### User Story 2 - Implement DeletableBatchTable Interface (Priority: P1)

As a library developer implementing a custom table, I want to use a simpler Delete interface where the rowid column is provided as a RecordReader, so I can process deletions using the same batch-oriented approach as other operations.

**Why this priority**: This is the second core feature. Provides consistency with UpdatableBatchTable and simplifies Delete implementations that need to process rowids in batches.

**Independent Test**: Can be fully tested by implementing a table that uses DeletableBatchTable interface and executing DELETE SQL via DuckDB. Delivers batch-oriented API for table implementers.

**Acceptance Scenarios**:

1. **Given** a table implementing DeletableBatchTable, **When** a DELETE SQL is executed via DuckDB Airport, **Then** the Delete method receives a RecordReader containing the rowid column.

2. **Given** a table implementing DeletableBatchTable with RETURNING clause, **When** Delete completes, **Then** RETURNING data is properly sent back to the client.

---

### User Story 3 - Backward Compatibility with Legacy Interfaces (Priority: P2)

As a library user with existing table implementations using UpdatableTable and DeletableTable interfaces, I want my code to continue working without modifications after the library update.

**Why this priority**: Ensures smooth migration path. Existing implementations must not break when upgrading to the new library version.

**Independent Test**: Can be fully tested by running existing DML examples and integration tests without modification. Delivers zero breaking changes for current users.

**Acceptance Scenarios**:

1. **Given** a table implementing the existing UpdatableTable interface, **When** an UPDATE SQL is executed, **Then** the operation succeeds using the legacy interface method.

2. **Given** a table implementing the existing DeletableTable interface, **When** a DELETE SQL is executed, **Then** the operation succeeds using the legacy interface method.

3. **Given** a table implementing both UpdatableBatchTable and UpdatableTable, **When** an UPDATE is executed, **Then** the server prefers the batch interface.

---

### User Story 4 - Updated Documentation and Examples (Priority: P3)

As a library user, I want clear documentation and examples showing how to use the new batch interfaces, so I can understand the benefits and migrate my implementations.

**Why this priority**: Supporting material that helps adoption but is not required for core functionality.

**Independent Test**: Can be validated by reviewing documentation for completeness and running updated examples successfully.

**Acceptance Scenarios**:

1. **Given** the updated documentation, **When** a developer reads the API guide, **Then** they find clear descriptions of both legacy and batch interfaces with migration guidance.

2. **Given** the updated examples, **When** a developer runs the DML example, **Then** they see demonstrations of both interface styles.

---

### Edge Cases

- What happens when a RecordReader contains no rowid column? The server should return an appropriate error.
- What happens when rowid column contains null values? Null rowids should be skipped or handled gracefully.
- What happens when a table implements both batch and legacy interfaces? The batch interface should be preferred.
- What happens when the RecordReader is empty (no rows)? The operation should succeed with zero affected rows.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide an `UpdatableBatchTable` interface with an `Update` method that accepts a RecordReader containing both rowid column and data columns.

- **FR-002**: System MUST provide a `DeletableBatchTable` interface with a `Delete` method that accepts a RecordReader containing the rowid column.

- **FR-003**: The `UpdatableBatchTable.Update` method signature MUST be: `Update(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)` where rows contains the rowid column.

- **FR-004**: The `DeletableBatchTable.Delete` method signature MUST be: `Delete(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)` where rows contains the rowid column.

- **FR-005**: The server handlers MUST detect whether a table implements the batch interfaces or legacy interfaces and call the appropriate method.

- **FR-006**: When a table implements both batch and legacy interfaces, the server MUST prefer the batch interface.

- **FR-007**: The library MUST provide wrapper functions to adapt legacy interface implementations to the new batch interface pattern internally, maintaining backward compatibility.

- **FR-008**: The rowid column in the RecordReader MUST be identifiable either by the column name "rowid" or by the presence of `is_rowid` metadata with value "true".

- **FR-009**: RETURNING clause functionality MUST work identically for both batch and legacy interfaces.

- **FR-010**: All existing integration tests MUST continue to pass without modification.

- **FR-011**: New integration tests MUST be added specifically for the batch interfaces.

- **FR-012**: Documentation MUST be updated to describe both interface styles with clear migration guidance.

- **FR-013**: The DML example MUST be updated to demonstrate the new batch interfaces.

### Key Entities

- **UpdatableBatchTable**: New interface extending Table with batch-oriented Update capability. The Update method receives all data (including rowids) in a single RecordReader.

- **DeletableBatchTable**: New interface extending Table with batch-oriented Delete capability. The Delete method receives rowids as a RecordReader.

- **RecordReader with rowid**: Arrow RecordReader containing a rowid column (identifiable by name or metadata) along with other data columns.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All existing integration tests pass without modification, confirming backward compatibility.

- **SC-002**: New batch interface integration tests demonstrate successful UPDATE and DELETE operations using the new interfaces.

- **SC-003**: The DML example runs successfully demonstrating both legacy and batch interface usage.

- **SC-004**: Library users can implement UPDATE/DELETE functionality using only a RecordReader parameter without managing separate rowIDs arrays.

- **SC-005**: Documentation clearly describes the two interface styles, their differences, and when to use each.

## Assumptions

- The rowid column data type will remain compatible with current extraction logic (Int64, Int32, or Uint64).
- Existing implementations using legacy interfaces will not be deprecated immediately; both interface styles will coexist.
- The preference for batch interfaces over legacy interfaces when both are implemented is the desired behavior for gradual migration.
- The wrapper functions are internal implementation details and not exposed as public API.
