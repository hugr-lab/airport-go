# Feature Specification: Simplify Batch Table Interface Signatures

**Feature Branch**: `013-batch-table-signature`
**Created**: 2025-12-30
**Status**: Draft
**Input**: User description: "Change Update and Delete method signatures in UpdatableBatchTable and DeletableBatchTable interfaces to accept arrow.Record instead of arrow.RecordReader. The RecordReader was being built from a single record in handlers, so RecordBatch simplifies both the interface and handler implementation."

## Clarifications

### Session 2025-12-30

- Q: What happens when rowid column contains null values? → A: Return error immediately on first null rowid.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Simplified Update Method Signature (Priority: P1)

As a library developer implementing UpdatableBatchTable, I want the Update method to receive an arrow.Record directly instead of a RecordReader, so I can access the data without calling Next() and without needing to handle iterator semantics.

**Why this priority**: This is the core simplification. Since handlers build a RecordReader from a single batch anyway, passing the batch directly removes unnecessary abstraction and simplifies implementation code.

**Independent Test**: Can be fully tested by implementing a table with the new Update signature and executing UPDATE SQL via DuckDB. Delivers simpler implementation pattern for table developers.

**Acceptance Scenarios**:

1. **Given** a table implementing UpdatableBatchTable with the new signature, **When** an UPDATE SQL is executed via DuckDB Airport, **Then** the Update method receives the arrow.Record directly containing both rowid column and data columns.

2. **Given** a table implementing UpdatableBatchTable with the new signature, **When** the Update method is called, **Then** the implementation can access rowid and data columns directly from the Record without calling Next().

3. **Given** a table implementing UpdatableBatchTable with RETURNING clause, **When** Update completes, **Then** RETURNING data is properly sent back to the client.

---

### User Story 2 - Simplified Delete Method Signature (Priority: P1)

As a library developer implementing DeletableBatchTable, I want the Delete method to receive an arrow.Record directly instead of a RecordReader, so I can access rowid values without handling iterator semantics.

**Why this priority**: Provides consistency with the Update signature change and simplifies Delete implementations.

**Independent Test**: Can be fully tested by implementing a table with the new Delete signature and executing DELETE SQL via DuckDB. Delivers simpler implementation pattern for table developers.

**Acceptance Scenarios**:

1. **Given** a table implementing DeletableBatchTable with the new signature, **When** a DELETE SQL is executed via DuckDB Airport, **Then** the Delete method receives the arrow.Record directly containing the rowid column.

2. **Given** a table implementing DeletableBatchTable with the new signature, **When** the Delete method is called, **Then** the implementation can extract rowid values directly from the Record.

3. **Given** a table implementing DeletableBatchTable with RETURNING clause, **When** Delete completes, **Then** RETURNING data is properly sent back to the client.

---

### User Story 3 - Simplified Handler Implementation (Priority: P2)

As a library maintainer, I want the handleDoExchangeUpdate and handleDoExchangeDelete handlers to pass the Record directly to table methods instead of wrapping it in a RecordReader, so the handler code is simpler and more efficient.

**Why this priority**: This is the internal refactoring that enables the interface change. Removing the RecordReader construction simplifies handler logic.

**Independent Test**: Can be validated by reviewing handler code and ensuring UPDATE/DELETE operations work correctly through integration tests.

**Acceptance Scenarios**:

1. **Given** the refactored handleDoExchangeUpdate handler, **When** it receives update data from the Flight stream, **Then** it passes the Record directly to the table's Update method without creating a RecordReader wrapper.

2. **Given** the refactored handleDoExchangeDelete handler, **When** it receives delete data from the Flight stream, **Then** it passes the Record directly to the table's Delete method without creating a RecordReader wrapper.

---

### User Story 4 - Updated Examples and Documentation (Priority: P3)

As a library user, I want the examples and documentation updated to reflect the new method signatures, so I can correctly implement my custom tables.

**Why this priority**: Supporting material for adoption. Documentation must match the actual API.

**Independent Test**: Can be validated by reviewing documentation and running examples successfully.

**Acceptance Scenarios**:

1. **Given** the updated documentation, **When** a developer reads the API guide, **Then** they see the correct Record signatures for UpdatableBatchTable and DeletableBatchTable.

2. **Given** the updated examples, **When** a developer runs the DML example, **Then** they see correct usage of the Record-based interfaces.

---

### Edge Cases

- What happens when a Record contains no rowid column? The server should return an appropriate error.
- What happens when rowid column contains null values? The method MUST return an error immediately upon encountering the first null rowid value.
- What happens when the Record is empty (zero rows)? The operation should succeed with zero affected rows.
- What happens when memory needs to be managed? Caller is responsible for releasing the Record after the method returns.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The `UpdatableBatchTable.Update` method signature MUST be: `Update(ctx context.Context, rows arrow.Record, opts *DMLOptions) (*DMLResult, error)` where rows is an arrow.Record containing rowid column and data columns.

- **FR-002**: The `DeletableBatchTable.Delete` method signature MUST be: `Delete(ctx context.Context, rows arrow.Record, opts *DMLOptions) (*DMLResult, error)` where rows is an arrow.Record containing the rowid column.

- **FR-003**: The `handleDoExchangeUpdate` handler MUST be refactored to pass the Record directly to the Update method instead of wrapping it in a RecordReader.

- **FR-004**: The `handleDoExchangeDelete` handler MUST be refactored to pass the Record directly to the Delete method instead of wrapping it in a RecordReader.

- **FR-005**: All existing integration tests MUST be updated to use the new method signatures.

- **FR-006**: The DML example MUST be updated to demonstrate the new Record-based signatures.

- **FR-007**: Documentation (api-guide.md, README) MUST be updated to reflect the new method signatures.

- **FR-008**: The rowid column in the Record MUST be identifiable by the column name "rowid" or by the presence of `is_rowid` metadata with value "true".

- **FR-009**: Memory management responsibility MUST be documented: caller releases the Record after method returns.

- **FR-010**: RETURNING clause functionality MUST work correctly with the new signatures.

- **FR-011**: Update and Delete methods MUST return an error immediately upon encountering a null rowid value in the Record.

### Key Entities

- **UpdatableBatchTable**: Interface with batch-oriented Update capability. The Update method receives data as arrow.Record.

- **DeletableBatchTable**: Interface with batch-oriented Delete capability. The Delete method receives rowids as arrow.Record.

- **arrow.Record**: Apache Arrow Record (RecordBatch) containing columnar data. Used directly instead of RecordReader iterator.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All integration tests pass with the new method signatures.

- **SC-002**: The DML example runs successfully demonstrating Record-based interface usage.

- **SC-003**: Library users can implement UPDATE/DELETE functionality by receiving arrow.Record directly without handling RecordReader iteration.

- **SC-004**: Handler code (handleDoExchangeUpdate, handleDoExchangeDelete) is simplified by removing RecordReader construction logic.

- **SC-005**: Documentation and API guide correctly describe the new method signatures.

## Assumptions

- The change from RecordReader to Record is safe because handlers were always building RecordReader from a single batch.
- The existing helper function FindRowIDColumn works with arrow.Schema from Record.
- The legacy interfaces (UpdatableTable, DeletableTable with rowIDs []int64) remain unchanged for backward compatibility.
- Memory management follows the same pattern: caller releases the Record after method returns.
