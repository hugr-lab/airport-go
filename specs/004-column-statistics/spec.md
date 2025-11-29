# Feature Specification: Column Statistics

**Feature Branch**: `004-column-statistics`
**Created**: 2025-11-29
**Status**: Complete
**Input**: User description: "Implement column_statistics action with integration tests. Add it as a new interface that table can implement."

**Reference**: <https://airport.query.farm/server_action_column_statistics.html>

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Query Column Statistics (Priority: P1)

A data analyst wants to understand data distribution characteristics in a table column before writing queries, enabling DuckDB to optimize query execution by understanding value ranges, null prevalence, and cardinality.

**Why this priority**: This is the core functionality - without column statistics retrieval, there's no feature. DuckDB uses this information to optimize query plans.

**Independent Test**: Can be fully tested by calling the `column_statistics` action with valid table/column parameters and verifying the statistics response contains expected fields (min, max, distinct_count, etc.).

**Acceptance Scenarios**:

1. **Given** a table implementing `StatisticsTable`, **When** `column_statistics` action is called with a valid column name, **Then** statistics are returned as an Arrow RecordBatch with the documented schema.
2. **Given** a table implementing `StatisticsTable`, **When** statistics are requested for a VARCHAR column, **Then** the response includes `max_string_length` and `contains_unicode` fields.
3. **Given** a table implementing `StatisticsTable`, **When** statistics are requested for a numeric column, **Then** the response includes typed `min` and `max` values.

---

### User Story 2 - Handle Missing Statistics Gracefully (Priority: P1)

A DuckDB client queries statistics from tables that may or may not support statistics reporting. The system should gracefully handle tables that don't implement statistics support.

**Why this priority**: Not all tables can provide statistics (e.g., streaming tables, external sources). Graceful fallback is essential for system stability.

**Independent Test**: Can be tested by calling `column_statistics` on a table that doesn't implement `StatisticsTable` interface and verifying appropriate error response.

**Acceptance Scenarios**:

1. **Given** a table NOT implementing `StatisticsTable`, **When** `column_statistics` action is called, **Then** an appropriate error status is returned indicating statistics are not supported.
2. **Given** a table implementing `StatisticsTable` but column doesn't exist, **When** `column_statistics` is called, **Then** a "not found" error is returned.

---

### User Story 3 - Partial Statistics Support (Priority: P2)

A table implementation can compute some statistics but not others (e.g., has min/max but not distinct count). The response should indicate which statistics are available.

**Why this priority**: Real-world tables may have constraints on which statistics can be efficiently computed. Partial support is better than no support.

**Independent Test**: Can be tested by implementing a table that returns partial statistics and verifying the response contains null values for unavailable statistics.

**Acceptance Scenarios**:

1. **Given** a table that can only compute min/max, **When** `column_statistics` is called, **Then** the response contains min/max values with null for `distinct_count`.
2. **Given** a table that cannot determine null presence, **When** `column_statistics` is called, **Then** `has_null` and `has_not_null` fields may be null.

---

### Edge Cases

- What happens when an invalid column type is specified in the request?
- How does the system handle columns with all NULL values?
- What happens when the column name contains special characters?
- How are nested/struct column statistics handled (if at all)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST implement `StatisticsTable` interface in `catalog` package that tables can optionally implement
- **FR-002**: System MUST implement `column_statistics` action handler in the flight package
- **FR-003**: Handler MUST decode msgpack-serialized request parameters containing `flight_descriptor`, `column_name`, and `type`
- **FR-004**: Handler MUST return Arrow RecordBatch with statistics schema containing: `has_not_null` (BOOLEAN), `has_null` (BOOLEAN), `distinct_count` (UINT64), `min` (column-typed), `max` (column-typed), `max_string_length` (UINT64), `contains_unicode` (BOOLEAN)
- **FR-005**: Handler MUST return `Unimplemented` status for tables not implementing `StatisticsTable`
- **FR-006**: Handler MUST return `NotFound` status for non-existent columns
- **FR-007**: Handler MUST return `InvalidArgument` status for malformed requests
- **FR-008**: The `min` and `max` fields MUST use the same Arrow data type as the column being analyzed
- **FR-009**: String-specific statistics (`max_string_length`, `contains_unicode`) MUST be applicable only to string/text columns
- **FR-010**: All statistics fields MUST be nullable to support partial statistics reporting

### Key Entities

- **StatisticsTable**: Interface extending `Table` with `ColumnStatistics(ctx, columnName, columnType) (*ColumnStats, error)` method
- **ColumnStats**: Struct containing statistics values (has_null, has_not_null, distinct_count, min, max, max_string_length, contains_unicode)
- **ColumnStatisticsParams**: Msgpack-decoded request parameters struct

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All tables implementing `StatisticsTable` can return column statistics via the `column_statistics` action
- **SC-002**: Tables not implementing `StatisticsTable` receive clear "unimplemented" error response
- **SC-003**: Integration tests verify statistics retrieval using DuckDB as client
- **SC-004**: Integration tests verify error handling for invalid columns and non-statistics tables
- **SC-005**: Handler correctly serializes all 7 statistics fields in Arrow IPC format
- **SC-006**: Response schema dynamically uses correct type for `min`/`max` based on column type

## Assumptions

- The `type` parameter in the request uses DuckDB type names (e.g., `VARCHAR`, `INTEGER`, `TIMESTAMP WITH TIME ZONE`)
- Statistics are computed at call time or from cached metadata; no specific caching requirements are specified
- Nested/struct columns are not supported for statistics in this initial implementation
- The response is always a single-row RecordBatch

## Out of Scope

- Histogram statistics or bucket distributions
- Multi-column correlation statistics
- Statistics caching or invalidation policies
- Statistics for table functions or views
- Streaming/live statistics updates
