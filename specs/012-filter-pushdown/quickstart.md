# Quickstart: Filter Pushdown Package

**Feature**: 012-filter-pushdown | **Date**: 2025-12-29

## Overview

The `filter` package parses DuckDB Airport extension filter pushdown JSON and encodes it to SQL strings. Use this when implementing a Flight server that delegates filtering to a SQL backend.

## Installation

The filter package is part of airport-go. Import it alongside the main package:

```go
import (
    "github.com/hugr-lab/airport-go"
    "github.com/hugr-lab/airport-go/catalog"
    "github.com/hugr-lab/airport-go/filter"
)
```

## Basic Usage

### Parse and Encode Filters

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Parse the filter JSON
    if len(opts.Filter) > 0 {
        fp, err := filter.Parse(opts.Filter)
        if err != nil {
            // Invalid JSON - log and continue without filter
            log.Printf("failed to parse filter: %v", err)
        } else {
            // Encode to SQL
            enc := filter.NewDuckDBEncoder(nil)
            whereClause := enc.EncodeFilters(fp)

            if whereClause != "" {
                query := fmt.Sprintf("SELECT * FROM %s WHERE %s", t.name, whereClause)
                // Execute query with filter
            }
        }
    }

    // Fall back to full table scan
    return t.fullScan(ctx, opts)
}
```

### Column Name Mapping

When your backend uses different column names:

```go
enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
    ColumnMapping: map[string]string{
        "user_id":    "uid",          // DuckDB "user_id" → backend "uid"
        "created_at": "creation_ts",  // DuckDB "created_at" → backend "creation_ts"
    },
})

whereClause := enc.EncodeFilters(fp)
// WHERE uid = 42 AND creation_ts > '2024-01-01'
```

### Column Expression Mapping

When Flight table columns map to computed expressions:

```go
enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
    ColumnExpressions: map[string]string{
        "full_name": "CONCAT(first_name, ' ', last_name)",
        "age":       "DATE_DIFF('year', birth_date, CURRENT_DATE)",
    },
})

whereClause := enc.EncodeFilters(fp)
// WHERE CONCAT(first_name, ' ', last_name) LIKE '%John%' AND DATE_DIFF('year', birth_date, CURRENT_DATE) > 18
```

## Working with Expressions

### Type Switch Pattern

```go
func processExpression(expr filter.Expression) {
    switch e := expr.(type) {
    case *filter.ComparisonExpression:
        fmt.Printf("Comparison: %s\n", e.Type())
        processExpression(e.Left)
        processExpression(e.Right)

    case *filter.ConjunctionExpression:
        op := "AND"
        if e.Type() == filter.TypeConjunctionOr {
            op = "OR"
        }
        fmt.Printf("%s with %d children\n", op, len(e.Children))
        for _, child := range e.Children {
            processExpression(child)
        }

    case *filter.ConstantExpression:
        fmt.Printf("Constant: %v (type: %s)\n", e.Value.Data, e.Value.Type.ID)

    case *filter.ColumnRefExpression:
        fmt.Printf("Column: index=%d\n", e.Binding.ColumnIndex)

    case *filter.FunctionExpression:
        fmt.Printf("Function: %s\n", e.Name)
    }
}

// Iterate all filters
for _, expr := range fp.Filters {
    processExpression(expr)
}
```

### Resolving Column Names

```go
fp, _ := filter.Parse(filterJSON)

// Get column name from a column reference
colRef := findColumnRef(fp.Filters[0])
name, err := fp.ColumnName(colRef)
if err != nil {
    // Invalid column binding index
}
fmt.Printf("Column: %s\n", name)
```

## Handling Unsupported Expressions

The encoder gracefully skips unsupported expressions:

```go
enc := filter.NewDuckDBEncoder(nil)

// Filter: (a = 1) AND (WINDOW_FUNC() > 0) AND (b = 2)
// Window functions are not supported for pushdown
whereClause := enc.EncodeFilters(fp)
// Result: (a = 1) AND (b = 2)
// The unsupported window function is skipped

// For OR: (a = 1) OR (WINDOW_FUNC() > 0)
// Entire OR is skipped because any unsupported child makes the OR unsafe
whereClause := enc.EncodeFilters(fp)
// Result: ""
```

**Rules**:
- **AND**: Skip unsupported child, keep others
- **OR**: Skip entire OR if any child unsupported
- **All unsupported**: Return empty string (no WHERE clause)

This is safe because DuckDB client applies filters client-side as fallback.

## Custom SQL Dialects

Implement the `Encoder` interface for other databases:

```go
type PostgreSQLEncoder struct {
    opts *filter.EncoderOptions
}

func (e *PostgreSQLEncoder) Encode(expr filter.Expression) string {
    switch ex := expr.(type) {
    case *filter.ComparisonExpression:
        // PostgreSQL-specific handling
        return e.encodeComparison(ex)
    // ... handle other expression types
    }
    return "" // Unsupported
}

func (e *PostgreSQLEncoder) EncodeFilters(fp *filter.FilterPushdown) string {
    // Same logic as DuckDB encoder, using PostgreSQL syntax
}

// String concatenation differs between dialects
func (e *PostgreSQLEncoder) encodeConcatOperator(args []filter.Expression) string {
    // PostgreSQL: arg1 || arg2
    // MySQL: CONCAT(arg1, arg2)
    // DuckDB: arg1 || arg2
}
```

## Common Patterns

### Check if Filter is Supported

```go
fp, _ := filter.Parse(filterJSON)
enc := filter.NewDuckDBEncoder(nil)

whereClause := enc.EncodeFilters(fp)
if whereClause == "" {
    // No filters could be encoded - do full scan
    return fullTableScan(ctx)
}

// Some or all filters were encoded
return filteredQuery(ctx, whereClause)
```

### Combine with Projection

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    var parts []string

    // Build SELECT clause
    if len(opts.Columns) > 0 {
        parts = append(parts, "SELECT "+strings.Join(opts.Columns, ", "))
    } else {
        parts = append(parts, "SELECT *")
    }

    parts = append(parts, "FROM "+t.name)

    // Build WHERE clause from filter
    if len(opts.Filter) > 0 {
        if fp, err := filter.Parse(opts.Filter); err == nil {
            enc := filter.NewDuckDBEncoder(nil)
            if where := enc.EncodeFilters(fp); where != "" {
                parts = append(parts, "WHERE "+where)
            }
        }
    }

    // Build LIMIT clause
    if opts.Limit > 0 {
        parts = append(parts, fmt.Sprintf("LIMIT %d", opts.Limit))
    }

    query := strings.Join(parts, " ")
    return executeQuery(ctx, query)
}
```

## Error Handling

```go
fp, err := filter.Parse(filterJSON)
if err != nil {
    // Malformed JSON - cannot parse at all
    // Options:
    // 1. Return error (strict)
    // 2. Log and continue with full scan (lenient)
    return nil, fmt.Errorf("invalid filter JSON: %w", err)
}

// Even if parsing succeeds, some expressions may be unsupported
// The encoder handles this gracefully - no error, just skips them
enc := filter.NewDuckDBEncoder(nil)
whereClause := enc.EncodeFilters(fp)
// whereClause may be empty if all filters unsupported
```

## Performance Tips

1. **Parse once, encode multiple times**: If you need to encode to different dialects, parse the filter once and use multiple encoders.

2. **Column mapping at encoder level**: Create encoder with column mapping once, reuse for all filters on the same table.

3. **Skip parsing if not needed**: Only parse `opts.Filter` if you actually use pushdown. Many implementations ignore filters entirely.

```go
// Only parse if filter is present and you can use it
if len(opts.Filter) > 0 && t.supportsPushdown {
    fp, _ := filter.Parse(opts.Filter)
    // ...
}
```

## Supported Expression Types

| Expression | Supported | Notes |
|------------|-----------|-------|
| Comparisons (=, <>, <, >, <=, >=) | ✅ | |
| IN / NOT IN | ✅ | |
| BETWEEN / NOT BETWEEN | ✅ | |
| IS NULL / IS NOT NULL | ✅ | |
| AND / OR | ✅ | OR skipped if any child unsupported |
| Functions (LOWER, LENGTH, etc.) | ✅ | Common functions supported |
| CAST / TRY_CAST | ✅ | |
| CASE WHEN | ✅ | |
| Aggregates (SUM, COUNT) | ❌ | Not applicable to row filtering |
| Window functions | ❌ | Not applicable to row filtering |
| Subqueries | ❌ | Complex, skipped |

## Next Steps

- See `examples/filter/main.go` for a complete working example
- See `filter_pushdown.md` for JSON format documentation
- See `tests/integration/filter_pushdown_test.go` for 20+ test cases
