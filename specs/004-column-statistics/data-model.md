# Data Model: Column Statistics

**Feature**: 004-column-statistics
**Date**: 2025-11-29

## Entities

### ColumnStats

Statistics for a single table column.

| Field | Go Type | Description | Nullable |
|-------|---------|-------------|----------|
| HasNotNull | `*bool` | Column contains non-null values | Yes |
| HasNull | `*bool` | Column contains null values | Yes |
| DistinctCount | `*uint64` | Number of unique values | Yes |
| Min | `any` | Minimum value (typed to match column) | Yes |
| Max | `any` | Maximum value (typed to match column) | Yes |
| MaxStringLength | `*uint64` | Maximum string length | Yes |
| ContainsUnicode | `*bool` | Strings contain unicode | Yes |

**Type Constraint**: `Min` and `Max` must be:
- `nil` for unavailable statistics
- Arrow-compatible Go type matching the column's Arrow type (e.g., `int64` for Int64, `string` for String)

### ColumnStatisticsParams

Request parameters for `column_statistics` action.

| Field | Go Type | Msgpack Key | Description |
|-------|---------|-------------|-------------|
| FlightDescriptor | `[]byte` | `flight_descriptor` | Serialized FlightDescriptor |
| ColumnName | `string` | `column_name` | Column to get statistics for |
| Type | `string` | `type` | DuckDB type name |

## Interfaces

### StatisticsTable

Extends `Table` with column statistics capability.

```go
type StatisticsTable interface {
    Table

    // ColumnStatistics returns statistics for a specific column.
    // columnName identifies the column.
    // columnType is the DuckDB type name (e.g., "VARCHAR", "INTEGER").
    // Returns nil stats fields for unavailable statistics.
    // Returns ErrNotFound if column doesn't exist.
    ColumnStatistics(ctx context.Context, columnName string, columnType string) (*ColumnStats, error)
}
```

**Implementation Notes**:
- Tables MAY return partial statistics (nil fields for unavailable values)
- String-specific fields (MaxStringLength, ContainsUnicode) should only be populated for string types
- Min/Max type MUST match the Arrow type for the column

## State Transitions

N/A - This feature is stateless metadata query.

## Relationships

```
Catalog
  └── Schema
       └── Table (may implement StatisticsTable)
              └── ColumnStatistics() → ColumnStats
```

## Validation Rules

1. **Column Name**: Must be non-empty string
2. **Column Type**: Must be valid DuckDB type name
3. **Min/Max Type**: Must match Arrow type for column (enforced at serialization)

## Arrow Schema for Response

Dynamic schema based on column type:

```go
func buildStatisticsSchema(columnType arrow.DataType) *arrow.Schema {
    return arrow.NewSchema([]arrow.Field{
        {Name: "has_not_null", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
        {Name: "has_null", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
        {Name: "distinct_count", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
        {Name: "min", Type: columnType, Nullable: true},
        {Name: "max", Type: columnType, Nullable: true},
        {Name: "max_string_length", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
        {Name: "contains_unicode", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
    }, nil)
}
```
