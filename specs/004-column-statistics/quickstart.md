# Quickstart: Column Statistics

## Overview

This feature adds the `column_statistics` action to Airport-Go, enabling DuckDB to query column statistics for query optimization.

## Implementing StatisticsTable

To enable statistics for your table, implement the `StatisticsTable` interface:

```go
package main

import (
    "context"

    "github.com/hugr-lab/airport-go/catalog"
)

type MyTable struct {
    // ... table fields
}

// Implement StatisticsTable interface
func (t *MyTable) ColumnStatistics(ctx context.Context, columnName string, columnType string) (*catalog.ColumnStats, error) {
    // Check if column exists
    if !t.hasColumn(columnName) {
        return nil, catalog.ErrNotFound
    }

    // Return statistics (nil fields for unavailable stats)
    minVal := int64(0)
    maxVal := int64(1000)
    distinctCount := uint64(500)
    hasNull := false
    hasNotNull := true

    return &catalog.ColumnStats{
        HasNotNull:    &hasNotNull,
        HasNull:       &hasNull,
        DistinctCount: &distinctCount,
        Min:           minVal,  // Type must match column's Arrow type
        Max:           maxVal,
    }, nil
}
```

## Partial Statistics

Return `nil` for any statistics you can't compute:

```go
func (t *MyTable) ColumnStatistics(ctx context.Context, columnName string, columnType string) (*catalog.ColumnStats, error) {
    // Only have min/max, no distinct count
    return &catalog.ColumnStats{
        Min: t.getMinValue(columnName),
        Max: t.getMaxValue(columnName),
        // Other fields are nil (not available)
    }, nil
}
```

## String Column Statistics

For string columns, include string-specific metrics:

```go
func (t *MyTable) ColumnStatistics(ctx context.Context, columnName string, columnType string) (*catalog.ColumnStats, error) {
    if columnType == "VARCHAR" {
        maxLen := uint64(255)
        hasUnicode := true
        return &catalog.ColumnStats{
            MaxStringLength:  &maxLen,
            ContainsUnicode:  &hasUnicode,
            Min:              "aaa",
            Max:              "zzz",
        }, nil
    }
    // ... handle other types
}
```

## Testing with DuckDB

Statistics are automatically requested by DuckDB during query planning:

```sql
-- Connect to your Airport server
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');

-- This query triggers column_statistics for 'id' column
SELECT * FROM demo.main.users WHERE id > 100;

-- DuckDB uses statistics to optimize the query plan
EXPLAIN SELECT * FROM demo.main.users WHERE id BETWEEN 50 AND 150;
```

## Type Mapping

Ensure your Min/Max values match the column's Arrow type:

| DuckDB Type | Go Value Type |
|-------------|---------------|
| `INTEGER` | `int32` |
| `BIGINT` | `int64` |
| `DOUBLE` | `float64` |
| `VARCHAR` | `string` |
| `BOOLEAN` | `bool` |
| `DATE` | `arrow.Date32` |
| `TIMESTAMP` | `arrow.Timestamp` |
