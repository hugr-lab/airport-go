# Research: Column Statistics

**Feature**: 004-column-statistics
**Date**: 2025-11-29

## Airport Protocol Specification

**Reference**: <https://airport.query.farm/server_action_column_statistics.html>

### Request Format

The `column_statistics` action receives msgpack-serialized parameters:

| Field | Type | Description |
|-------|------|-------------|
| `flight_descriptor` | bytes | Arrow Flight serialized FlightDescriptor identifying the table |
| `column_name` | string | Name of the column to get statistics for |
| `type` | string | DuckDB data type name (e.g., `VARCHAR`, `INTEGER`, `TIMESTAMP WITH TIME ZONE`) |

### Response Format

Response MUST be an Arrow RecordBatch serialized using Arrow IPC format with this schema:

| Field | Arrow Type | Nullable | Description |
|-------|------------|----------|-------------|
| `has_not_null` | BOOLEAN | Yes | True if column contains non-null values |
| `has_null` | BOOLEAN | Yes | True if column contains null values |
| `distinct_count` | UINT64 | Yes | Number of unique values |
| `min` | *column type* | Yes | Minimum value (typed to match column) |
| `max` | *column type* | Yes | Maximum value (typed to match column) |
| `max_string_length` | UINT64 | Yes | Maximum string length (strings only) |
| `contains_unicode` | BOOLEAN | Yes | True if strings contain unicode |

**Critical**: The `min` and `max` fields MUST use the Arrow type corresponding to the column's DuckDB type, not a fixed type.

### DuckDB Type to Arrow Type Mapping

Based on existing codebase patterns and Arrow-Go types:

| DuckDB Type | Arrow Type |
|-------------|------------|
| `BOOLEAN` | `arrow.FixedWidthTypes.Boolean` |
| `TINYINT` | `arrow.PrimitiveTypes.Int8` |
| `SMALLINT` | `arrow.PrimitiveTypes.Int16` |
| `INTEGER` | `arrow.PrimitiveTypes.Int32` |
| `BIGINT` | `arrow.PrimitiveTypes.Int64` |
| `UTINYINT` | `arrow.PrimitiveTypes.Uint8` |
| `USMALLINT` | `arrow.PrimitiveTypes.Uint16` |
| `UINTEGER` | `arrow.PrimitiveTypes.Uint32` |
| `UBIGINT` | `arrow.PrimitiveTypes.Uint64` |
| `FLOAT` | `arrow.PrimitiveTypes.Float32` |
| `DOUBLE` | `arrow.PrimitiveTypes.Float64` |
| `VARCHAR` | `arrow.BinaryTypes.String` |
| `DATE` | `arrow.FixedWidthTypes.Date32` |
| `TIMESTAMP` | `arrow.FixedWidthTypes.Timestamp_us` |
| `TIMESTAMP WITH TIME ZONE` | `arrow.FixedWidthTypes.Timestamp_us` with UTC timezone |

## Implementation Decisions

### Decision 1: Interface Location

**Decision**: Add `StatisticsTable` interface to `catalog/table.go`

**Rationale**: Co-locates with other table interfaces (`InsertableTable`, `UpdatableTable`, `DeletableTable`) for discoverability and consistency.

**Alternatives Considered**:
- New file `catalog/statistics.go` - rejected, adds unnecessary file for single interface

### Decision 2: Statistics Struct Design

**Decision**: Use `ColumnStats` struct with `any` typed min/max fields

```go
type ColumnStats struct {
    HasNotNull       *bool
    HasNull          *bool
    DistinctCount    *uint64
    Min              any    // Must match column Arrow type
    Max              any    // Must match column Arrow type
    MaxStringLength  *uint64
    ContainsUnicode  *bool
}
```

**Rationale**: Go generics would require type parameter on interface, complicating catalog design. Using `any` with runtime type assertion matches Arrow builder patterns.

**Alternatives Considered**:
- Generic `ColumnStats[T]` - rejected, interface would need type parameter
- Separate struct per type - rejected, excessive boilerplate

### Decision 3: Handler File Organization

**Decision**: Create `flight/doaction_statistics.go` for statistics-related handlers

**Rationale**: Follows pattern of `doaction_ddl.go` for DDL operations. Keeps handlers organized by feature area.

### Decision 4: Error Handling

**Decision**: Map errors to gRPC status codes:

| Condition | gRPC Code |
|-----------|-----------|
| Table doesn't implement StatisticsTable | `Unimplemented` |
| Column not found | `NotFound` |
| Invalid request payload | `InvalidArgument` |
| Internal error during statistics computation | `Internal` |

**Rationale**: Matches existing error handling patterns in DDL handlers.

### Decision 5: Response Serialization

**Decision**: Build Arrow RecordBatch with IPC serialization (same as Flight schema serialization)

**Rationale**: Airport protocol specifies Arrow IPC format. Existing codebase uses `flight.SerializeSchema` for schema serialization; will use `ipc.NewWriter` for RecordBatch.

### Decision 6: DuckDB Type Parsing

**Decision**: Implement type mapping function to convert DuckDB type strings to Arrow types

**Rationale**: The `type` parameter uses DuckDB type names. Need deterministic mapping to construct correct min/max field types in response schema.

## Integration Test Strategy

Per user requirement: "Check that the statistic action is called if the DuckDB filtered by columns."

### Test 1: Basic Statistics Query

1. Create mock table implementing `StatisticsTable`
2. Call `column_statistics` action via DuckDB client
3. Verify response contains expected statistics values

### Test 2: Statistics Used for Query Optimization

1. Create mock table with known statistics (e.g., min=0, max=100)
2. Execute DuckDB query with WHERE clause filter (e.g., `WHERE id > 50`)
3. Verify `column_statistics` action was called (via logging or mock counter)
4. Verify query returns correct results

### Test 3: Non-StatisticsTable Error

1. Create table NOT implementing `StatisticsTable`
2. Call `column_statistics` action
3. Verify `Unimplemented` error returned

### Test 4: Column Not Found Error

1. Create mock table implementing `StatisticsTable`
2. Call `column_statistics` with non-existent column
3. Verify `NotFound` error returned

## Open Questions (Resolved)

1. **Q**: How does DuckDB trigger `column_statistics`?
   **A**: DuckDB calls this action during query planning when it needs statistics for a column. It's called automatically when DuckDB's optimizer evaluates filter predicates.

2. **Q**: What if statistics are expensive to compute?
   **A**: Implementation is responsible for caching or returning partial statistics (null fields). The interface contract doesn't mandate computation strategy.

3. **Q**: Should we support nested column statistics?
   **A**: No, per spec.md "Out of Scope" section. Nested/struct columns are not supported initially.
