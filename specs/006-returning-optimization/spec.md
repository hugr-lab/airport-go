# Feature Specification: DML RETURNING Clause Column Selection

**Feature Branch**: `006-returning-optimization`
**Created**: 2025-11-30
**Status**: Implemented
**Input**: User description: "For DML Operation with returning clause: Investigate how to get returning column names from DuckDB, provide the parameter with columns to be returned in RETURNING clause in DML table methods, extend integration tests to verify that only requested columns are returned."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Return Only Requested Columns from INSERT (Priority: P1)

A server developer implements an InsertableTable where the database stores data with many columns (e.g., 20+ columns including audit fields, timestamps, computed values). When users execute `INSERT ... RETURNING id, created_at`, the server should only return the `id` and `created_at` columns, not all columns in the table.

**Why this priority**: This is the core functionality - enabling efficient RETURNING clause that respects the user's column selection rather than returning all data. Reduces network bandwidth and processing overhead.

**Independent Test**: Can be fully tested by executing an INSERT statement with a RETURNING clause specifying a subset of columns and verifying only those columns are returned.

**Acceptance Scenarios**:

1. **Given** a table with columns (id, name, email, created_at, updated_at), **When** executing `INSERT INTO table (id, name, email) VALUES (...) RETURNING id`, **Then** only the `id` column is returned in the result.
2. **Given** a table with columns (id, name, email), **When** executing `INSERT INTO table (id, name, email) VALUES (...) RETURNING *`, **Then** all columns that the table can return are included in the result.
3. **Given** a table implementing InsertableTable without RETURNING support, **When** executing `INSERT ... RETURNING id`, **Then** the operation succeeds with ReturningData as nil (no crash, graceful handling).

---

### User Story 2 - Return Only Requested Columns from UPDATE (Priority: P2)

A server developer implements an UpdatableTable. When users execute `UPDATE ... SET name = 'X' RETURNING id, name`, the server should only return the `id` and `name` columns for the updated rows.

**Why this priority**: UPDATE with RETURNING is commonly used to get updated values. Same optimization benefit as INSERT.

**Independent Test**: Can be fully tested by executing an UPDATE statement with a RETURNING clause specifying columns and verifying only those columns are returned.

**Acceptance Scenarios**:

1. **Given** a table with rows and columns (rowid, id, name, email), **When** executing `UPDATE table SET name = 'X' WHERE id = 1 RETURNING id, name`, **Then** only `id` and `name` columns are returned for the updated row.
2. **Given** multiple rows being updated, **When** executing `UPDATE table SET email = 'x@y.com' WHERE id IN (1,2,3) RETURNING id`, **Then** three rows are returned, each with only the `id` column.

---

### User Story 3 - Return Only Requested Columns from DELETE (Priority: P2)

A server developer implements a DeletableTable. When users execute `DELETE ... RETURNING id, name`, the server should only return the `id` and `name` columns for the deleted rows.

**Why this priority**: DELETE with RETURNING is commonly used to confirm what was deleted. Same pattern as INSERT/UPDATE.

**Independent Test**: Can be fully tested by executing a DELETE statement with a RETURNING clause specifying columns and verifying only those columns are returned.

**Acceptance Scenarios**:

1. **Given** a table with rows and columns (rowid, id, name, email), **When** executing `DELETE FROM table WHERE id = 1 RETURNING id, name`, **Then** only `id` and `name` columns are returned for the deleted row.

---

### Edge Cases

- What happens when RETURNING clause requests a column that doesn't exist? The server should return an error indicating the column is not found.
- What happens when RETURNING clause is empty (no columns specified)? Follow DuckDB behavior - typically means no RETURNING clause was requested, so `return-chunks=0`.
- How does the server handle `RETURNING *` when some columns are computed or virtual? Implementation-dependent; server returns what it can.
- What happens if the DuckDB Airport extension doesn't send column information? Server falls back to current behavior (return all columns or input schema columns).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The library MUST investigate and document how DuckDB Airport extension communicates RETURNING column names (via headers, metadata, or other protocol mechanisms).
- **FR-002**: The catalog DML interfaces (InsertableTable, UpdatableTable, DeletableTable) MUST be extended with a parameter or options struct that specifies which columns to include in RETURNING results.
- **FR-003**: The DML method signatures MUST provide RETURNING column names to the implementation so it can optimize data retrieval.
- **FR-004**: The server MUST pass RETURNING column information from the protocol layer to the catalog table implementations.
- **FR-005**: Integration tests MUST verify that only the columns specified in the RETURNING clause are returned, even when the incoming RecordBatch contains more columns.
- **FR-006**: The library will add a DMLOptions struct parameter to DML interface methods. This is a one-time breaking change that requires all implementations to update their method signatures. Future enhancements can add fields to DMLOptions without breaking interfaces again.

### Key Entities

- **ReturningColumns**: A slice of column names that the client expects in RETURNING results.
- **DMLOptions**: A struct containing ReturningColumns (slice of column names to return). Designed for future extensibility - new fields can be added without breaking interface signatures.
- **InsertableTable/UpdatableTable/DeletableTable interfaces**: Will be modified with updated method signatures that include DMLOptions parameter (one-time breaking change).

## Clarifications

### Session 2025-11-30

- Q: How should the library extend DML interfaces for RETURNING columns while considering backward compatibility? → A: Add DMLOptions struct parameter to existing methods. This is a one-time breaking change, but future additions (new fields in DMLOptions) won't require interface changes.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: RETURNING clauses with column subsets return only the specified columns (verifiable via integration tests).
- **SC-002**: Network payload size for RETURNING results is reduced proportionally when fewer columns are requested (fewer columns = smaller response).
- **SC-003**: All existing DML implementations (examples, tests) are updated to the new method signatures with DMLOptions parameter.
- **SC-004**: All existing integration tests pass without modification.
- **SC-005**: New integration tests demonstrate column filtering for INSERT, UPDATE, and DELETE RETURNING clauses.
