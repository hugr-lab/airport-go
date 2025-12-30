# DuckDB Airport Extension - Filter Pushdown JSON Structure

This document describes the JSON structure used for predicate pushdown from DuckDB to Arrow Flight servers via the Airport extension.

## Overview

When executing a SELECT query on a table connected via Airport (Arrow Flight), DuckDB serializes filter expressions to JSON format and sends them to the Flight server. The server can then use these filters to optimize data retrieval.

## Top-Level JSON Structure

```json
{
  "filters": [
    { /* Expression object */ },
    { /* Expression object */ },
    ...
  ],
  "column_binding_names_by_index": ["column1", "column2", "rowid", ...]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `filters` | Array\<Expression\> | Array of serialized filter expressions |
| `column_binding_names_by_index` | Array\<string\> | Column names mapped by their binding index. Special value `"rowid"` for row ID column |

---

## Expression Base Structure

Every expression object contains these base fields:

```json
{
  "expression_class": "BOUND_COMPARISON",
  "type": "COMPARE_EQUAL",
  "alias": "",
  "query_location": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `expression_class` | string | The class/category of expression (see ExpressionClass enum) |
| `type` | string | Specific type of expression (see ExpressionType enum) |
| `alias` | string | Optional alias for the expression |
| `query_location` | number \| null | Optional position in the original query |

---

## ExpressionClass Enum Values

These are the possible values for the `expression_class` field:

### Bound Expressions (used in filter pushdown)

| Value | Numeric | Description |
|-------|---------|-------------|
| `BOUND_AGGREGATE` | 25 | Aggregate function expression |
| `BOUND_CASE` | 26 | CASE expression |
| `BOUND_CAST` | 27 | Type cast expression |
| `BOUND_COLUMN_REF` | 28 | Column reference |
| `BOUND_COMPARISON` | 29 | Comparison expression (=, <, >, etc.) |
| `BOUND_CONJUNCTION` | 30 | AND/OR conjunction |
| `BOUND_CONSTANT` | 31 | Constant value |
| `BOUND_DEFAULT` | 32 | DEFAULT expression |
| `BOUND_FUNCTION` | 33 | Function call |
| `BOUND_OPERATOR` | 34 | Operator expression |
| `BOUND_PARAMETER` | 35 | Parameter reference |
| `BOUND_REF` | 36 | Reference expression |
| `BOUND_SUBQUERY` | 37 | Subquery expression |
| `BOUND_WINDOW` | 38 | Window function |
| `BOUND_BETWEEN` | 39 | BETWEEN expression |
| `BOUND_UNNEST` | 40 | UNNEST expression |
| `BOUND_LAMBDA` | 41 | Lambda expression |
| `BOUND_LAMBDA_REF` | 42 | Lambda reference |

---

## ExpressionType Enum Values

These are the possible values for the `type` field:

### Comparison Operators

| Value | Numeric | SQL Equivalent |
|-------|---------|----------------|
| `COMPARE_EQUAL` | 25 | `=` |
| `COMPARE_NOTEQUAL` | 26 | `<>` or `!=` |
| `COMPARE_LESSTHAN` | 27 | `<` |
| `COMPARE_GREATERTHAN` | 28 | `>` |
| `COMPARE_LESSTHANOREQUALTO` | 29 | `<=` |
| `COMPARE_GREATERTHANOREQUALTO` | 30 | `>=` |
| `COMPARE_IN` | 35 | `IN (...)` |
| `COMPARE_NOT_IN` | 36 | `NOT IN (...)` |
| `COMPARE_DISTINCT_FROM` | 37 | `IS DISTINCT FROM` |
| `COMPARE_BETWEEN` | 38 | `BETWEEN` |
| `COMPARE_NOT_BETWEEN` | 39 | `NOT BETWEEN` |
| `COMPARE_NOT_DISTINCT_FROM` | 40 | `IS NOT DISTINCT FROM` |

### Conjunction Operators

| Value | Numeric | SQL Equivalent |
|-------|---------|----------------|
| `CONJUNCTION_AND` | 50 | `AND` |
| `CONJUNCTION_OR` | 51 | `OR` |

### Unary Operators

| Value | Numeric | SQL Equivalent |
|-------|---------|----------------|
| `OPERATOR_NOT` | 13 | `NOT` |
| `OPERATOR_IS_NULL` | 14 | `IS NULL` |
| `OPERATOR_IS_NOT_NULL` | 15 | `IS NOT NULL` |

### Value Types

| Value | Numeric | Description |
|-------|---------|-------------|
| `VALUE_CONSTANT` | 75 | Constant value |
| `VALUE_PARAMETER` | 76 | Parameter value |
| `VALUE_NULL` | 79 | NULL value |
| `VALUE_DEFAULT` | 82 | DEFAULT value |

### Function Types

| Value | Numeric | Description |
|-------|---------|-------------|
| `FUNCTION` | 140 | Function call |
| `BOUND_FUNCTION` | 141 | Bound function call |

### Aggregate Types

| Value | Numeric | Description |
|-------|---------|-------------|
| `AGGREGATE` | 100 | Aggregate function |
| `BOUND_AGGREGATE` | 101 | Bound aggregate function |

### Window Function Types

| Value | Numeric | Description |
|-------|---------|-------------|
| `WINDOW_AGGREGATE` | 110 | Window aggregate |
| `WINDOW_RANK` | 120 | RANK() |
| `WINDOW_RANK_DENSE` | 121 | DENSE_RANK() |
| `WINDOW_NTILE` | 122 | NTILE() |
| `WINDOW_PERCENT_RANK` | 123 | PERCENT_RANK() |
| `WINDOW_CUME_DIST` | 124 | CUME_DIST() |
| `WINDOW_ROW_NUMBER` | 125 | ROW_NUMBER() |
| `WINDOW_FIRST_VALUE` | 130 | FIRST_VALUE() |
| `WINDOW_LAST_VALUE` | 131 | LAST_VALUE() |
| `WINDOW_LEAD` | 132 | LEAD() |
| `WINDOW_LAG` | 133 | LAG() |
| `WINDOW_NTH_VALUE` | 134 | NTH_VALUE() |

### Other Operators

| Value | Numeric | Description |
|-------|---------|-------------|
| `CASE_EXPR` | 150 | CASE expression |
| `OPERATOR_NULLIF` | 151 | NULLIF() |
| `OPERATOR_COALESCE` | 152 | COALESCE() |
| `ARRAY_EXTRACT` | 153 | Array element access |
| `ARRAY_SLICE` | 154 | Array slice |
| `STRUCT_EXTRACT` | 155 | Struct field access |
| `ARRAY_CONSTRUCTOR` | 156 | Array constructor |
| `CAST` | 225 | Type cast |
| `BOUND_REF` | 227 | Bound reference |
| `BOUND_COLUMN_REF` | 228 | Bound column reference |
| `BOUND_UNNEST` | 229 | UNNEST |

---

## Expression Type-Specific Fields

### BoundComparisonExpression

For `expression_class: "BOUND_COMPARISON"`

```json
{
  "expression_class": "BOUND_COMPARISON",
  "type": "COMPARE_EQUAL",
  "alias": "",
  "left": { /* Expression */ },
  "right": { /* Expression */ }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `left` | Expression | Left operand |
| `right` | Expression | Right operand |

---

### BoundConjunctionExpression

For `expression_class: "BOUND_CONJUNCTION"`

```json
{
  "expression_class": "BOUND_CONJUNCTION",
  "type": "CONJUNCTION_AND",
  "alias": "",
  "children": [
    { /* Expression */ },
    { /* Expression */ }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `children` | Array\<Expression\> | Child expressions to combine |

---

### BoundConstantExpression

For `expression_class: "BOUND_CONSTANT"`

```json
{
  "expression_class": "BOUND_CONSTANT",
  "type": "VALUE_CONSTANT",
  "alias": "",
  "value": { /* Value object */ }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `value` | Value | The constant value |

---

### BoundColumnRefExpression

For `expression_class: "BOUND_COLUMN_REF"`

```json
{
  "expression_class": "BOUND_COLUMN_REF",
  "type": "BOUND_COLUMN_REF",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "binding": {
    "table_index": 0,
    "column_index": 0
  },
  "depth": 0
}
```

| Field | Type | Description |
|-------|------|-------------|
| `return_type` | LogicalType | The type of the column |
| `binding` | ColumnBinding | Column binding information |
| `depth` | number | Depth in nested queries |

**Note**: Use `column_binding_names_by_index[binding.column_index]` to get the actual column name.

---

### BoundFunctionExpression

For `expression_class: "BOUND_FUNCTION"`

```json
{
  "expression_class": "BOUND_FUNCTION",
  "type": "BOUND_FUNCTION",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "children": [
    { /* Expression */ }
  ],
  "name": "function_name",
  "arguments": [ /* LogicalType[] */ ],
  "original_arguments": [ /* LogicalType[] */ ],
  "catalog_name": "",
  "schema_name": "",
  "has_serialize": false,
  "is_operator": false
}
```

| Field | Type | Description |
|-------|------|-------------|
| `return_type` | LogicalType | Return type of the function |
| `children` | Array\<Expression\> | Function arguments |
| `name` | string | Function name |
| `arguments` | Array\<LogicalType\> | Argument types |
| `original_arguments` | Array\<LogicalType\> | Original argument types before resolution |
| `catalog_name` | string | Catalog name (optional) |
| `schema_name` | string | Schema name (optional) |
| `has_serialize` | boolean | Whether function has custom serialization |
| `is_operator` | boolean | Whether this is an operator expression |

If `has_serialize` is true, there will be an additional `function_data` field with function-specific serialized data.

---

### BoundCastExpression

For `expression_class: "BOUND_CAST"`

```json
{
  "expression_class": "BOUND_CAST",
  "type": "CAST",
  "alias": "",
  "child": { /* Expression */ },
  "return_type": { /* LogicalType */ },
  "try_cast": false
}
```

| Field | Type | Description |
|-------|------|-------------|
| `child` | Expression | Expression to cast |
| `return_type` | LogicalType | Target type |
| `try_cast` | boolean | If true, returns NULL on cast failure instead of error |

---

### BoundBetweenExpression

For `expression_class: "BOUND_BETWEEN"`

```json
{
  "expression_class": "BOUND_BETWEEN",
  "type": "COMPARE_BETWEEN",
  "alias": "",
  "input": { /* Expression */ },
  "lower": { /* Expression */ },
  "upper": { /* Expression */ },
  "lower_inclusive": true,
  "upper_inclusive": true
}
```

| Field | Type | Description |
|-------|------|-------------|
| `input` | Expression | Value to test |
| `lower` | Expression | Lower bound |
| `upper` | Expression | Upper bound |
| `lower_inclusive` | boolean | Whether lower bound is inclusive |
| `upper_inclusive` | boolean | Whether upper bound is inclusive |

---

### BoundOperatorExpression

For `expression_class: "BOUND_OPERATOR"`

```json
{
  "expression_class": "BOUND_OPERATOR",
  "type": "OPERATOR_IS_NULL",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "children": [
    { /* Expression */ }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `return_type` | LogicalType | Return type |
| `children` | Array\<Expression\> | Operand expressions |

---

### BoundCaseExpression

For `expression_class: "BOUND_CASE"`

```json
{
  "expression_class": "BOUND_CASE",
  "type": "CASE_EXPR",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "case_checks": [
    {
      "when_expr": { /* Expression */ },
      "then_expr": { /* Expression */ }
    }
  ],
  "else_expr": { /* Expression */ }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `return_type` | LogicalType | Return type |
| `case_checks` | Array\<BoundCaseCheck\> | WHEN...THEN pairs |
| `else_expr` | Expression | ELSE expression |

---

### BoundAggregateExpression

For `expression_class: "BOUND_AGGREGATE"`

```json
{
  "expression_class": "BOUND_AGGREGATE",
  "type": "BOUND_AGGREGATE",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "children": [ /* Expression[] */ ],
  "name": "sum",
  "arguments": [ /* LogicalType[] */ ],
  "original_arguments": [ /* LogicalType[] */ ],
  "has_serialize": false,
  "aggregate_type": "NON_DISTINCT",
  "filter": null,
  "order_bys": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `return_type` | LogicalType | Return type |
| `children` | Array\<Expression\> | Aggregate arguments |
| `name` | string | Aggregate function name |
| `aggregate_type` | string | `"NON_DISTINCT"` or `"DISTINCT"` |
| `filter` | Expression \| null | Optional FILTER clause |
| `order_bys` | BoundOrderModifier \| null | Optional ORDER BY within aggregate |

---

### BoundWindowExpression

For `expression_class: "BOUND_WINDOW"`

```json
{
  "expression_class": "BOUND_WINDOW",
  "type": "WINDOW_ROW_NUMBER",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "children": [ /* Expression[] */ ],
  "partitions": [ /* Expression[] */ ],
  "orders": [ /* BoundOrderByNode[] */ ],
  "filters": null,
  "ignore_nulls": false,
  "start": "UNBOUNDED_PRECEDING",
  "end": "CURRENT_ROW_RANGE",
  "start_expr": null,
  "end_expr": null,
  "offset_expr": null,
  "default_expr": null,
  "exclude_clause": "NO_OTHER",
  "distinct": false,
  "arg_orders": []
}
```

---

### BoundParameterExpression

For `expression_class: "BOUND_PARAMETER"`

```json
{
  "expression_class": "BOUND_PARAMETER",
  "type": "VALUE_PARAMETER",
  "alias": "",
  "identifier": "$1",
  "return_type": { /* LogicalType */ },
  "parameter_data": { /* parameter value data */ }
}
```

---

### BoundReferenceExpression

For `expression_class: "BOUND_REF"`

```json
{
  "expression_class": "BOUND_REF",
  "type": "BOUND_REF",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "index": 0
}
```

---

### BoundLambdaExpression

For `expression_class: "BOUND_LAMBDA"`

```json
{
  "expression_class": "BOUND_LAMBDA",
  "type": "LAMBDA",
  "alias": "",
  "return_type": { /* LogicalType */ },
  "lambda_expr": { /* Expression */ },
  "captures": [ /* Expression[] */ ],
  "parameter_count": 1
}
```

---

## Value Structure

Values are serialized with their type information:

```json
{
  "type": { /* LogicalType */ },
  "is_null": false,
  "value": <actual_value>
}
```

The `value` field type depends on `type.id`:

| LogicalType | Value JSON Type | Example |
|-------------|-----------------|---------|
| BOOLEAN | boolean | `true`, `false` |
| TINYINT, SMALLINT, INTEGER, BIGINT | number (integer) | `42`, `-100` |
| UTINYINT, USMALLINT, UINTEGER, UBIGINT | number (unsigned) | `42` |
| FLOAT, DOUBLE | number (decimal) | `3.14` |
| VARCHAR | string | `"hello"` |
| BLOB | string (base64 or hex) | see below |
| DATE | number (days since epoch) | `19000` |
| TIME | number (microseconds since midnight) | `43200000000` |
| TIMESTAMP | number (microseconds since epoch) | `1609459200000000` |
| INTERVAL | object | `{"months": 1, "days": 2, "micros": 3}` |
| DECIMAL | string or number | `"123.45"` |
| UUID | string | `"550e8400-e29b-41d4-a716-446655440000"` |
| HUGEINT | object | `{"upper": 0, "lower": 123}` |
| UHUGEINT | object | `{"upper": 0, "lower": 123}` |
| LIST | object with children | `{"children": [...]}` |
| STRUCT | object with children | `{"children": [...]}` |
| MAP | object with children | `{"children": [...]}` |

### Hugeint/Uhugeint Structure

For 128-bit integers:

```json
{
  "upper": 0,
  "lower": 12345678901234567890
}
```

### Non-UTF8 String Handling

If a string value contains non-UTF8 bytes, it is encoded as:

```json
{
  "base64": "SGVsbG8gV29ybGQ="
}
```

---

## LogicalType Structure

```json
{
  "id": "INTEGER",
  "type_info": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | LogicalTypeId value |
| `type_info` | object \| null | Extra type information for complex types |

### LogicalTypeId Values

| Value | Numeric | Description |
|-------|---------|-------------|
| `INVALID` | 0 | Invalid type |
| `SQLNULL` | 1 | NULL type |
| `UNKNOWN` | 2 | Unknown type |
| `ANY` | 3 | Any type |
| `BOOLEAN` | 10 | Boolean |
| `TINYINT` | 11 | 8-bit signed integer |
| `SMALLINT` | 12 | 16-bit signed integer |
| `INTEGER` | 13 | 32-bit signed integer |
| `BIGINT` | 14 | 64-bit signed integer |
| `DATE` | 15 | Date |
| `TIME` | 16 | Time |
| `TIMESTAMP_SEC` | 17 | Timestamp (seconds) |
| `TIMESTAMP_MS` | 18 | Timestamp (milliseconds) |
| `TIMESTAMP` | 19 | Timestamp (microseconds) |
| `TIMESTAMP_NS` | 20 | Timestamp (nanoseconds) |
| `DECIMAL` | 21 | Decimal |
| `FLOAT` | 22 | 32-bit float |
| `DOUBLE` | 23 | 64-bit float |
| `CHAR` | 24 | Fixed-length string |
| `VARCHAR` | 25 | Variable-length string |
| `BLOB` | 26 | Binary data |
| `INTERVAL` | 27 | Time interval |
| `UTINYINT` | 28 | 8-bit unsigned integer |
| `USMALLINT` | 29 | 16-bit unsigned integer |
| `UINTEGER` | 30 | 32-bit unsigned integer |
| `UBIGINT` | 31 | 64-bit unsigned integer |
| `TIMESTAMP_TZ` | 32 | Timestamp with timezone |
| `TIME_TZ` | 34 | Time with timezone |
| `HUGEINT` | 50 | 128-bit signed integer |
| `UHUGEINT` | 49 | 128-bit unsigned integer |
| `UUID` | 54 | UUID |
| `STRUCT` | 100 | Struct |
| `LIST` | 101 | List/Array |
| `MAP` | 102 | Map |
| `ENUM` | 104 | Enum |
| `ARRAY` | 108 | Fixed-size array |

### ExtraTypeInfo

For complex types, `type_info` contains additional information:

#### DecimalTypeInfo
```json
{
  "type": "DECIMAL_TYPE_INFO",
  "alias": "",
  "width": 18,
  "scale": 3
}
```

#### ListTypeInfo
```json
{
  "type": "LIST_TYPE_INFO",
  "alias": "",
  "child_type": { /* LogicalType */ }
}
```

#### StructTypeInfo
```json
{
  "type": "STRUCT_TYPE_INFO",
  "alias": "",
  "child_types": [
    {"first": "field_name", "second": { /* LogicalType */ }}
  ]
}
```

#### ArrayTypeInfo
```json
{
  "type": "ARRAY_TYPE_INFO",
  "alias": "",
  "child_type": { /* LogicalType */ },
  "size": 10
}
```

---

## ColumnBinding Structure

```json
{
  "table_index": 0,
  "column_index": 2
}
```

| Field | Type | Description |
|-------|------|-------------|
| `table_index` | number | Table index in the query plan |
| `column_index` | number | Column index within the table |

Use `column_binding_names_by_index[column_index]` from the top-level structure to get the column name.

---

## BoundOrderByNode Structure

Used in aggregate and window functions:

```json
{
  "type": "ASCENDING",
  "null_order": "NULLS_LAST",
  "expression": { /* Expression */ }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | `"ASCENDING"` or `"DESCENDING"` |
| `null_order` | string | `"NULLS_FIRST"` or `"NULLS_LAST"` |
| `expression` | Expression | Expression to order by |

---

## Complete Examples

### Simple Equality Filter

SQL: `SELECT * FROM table WHERE id = 42`

```json
{
  "filters": [
    {
      "expression_class": "BOUND_COMPARISON",
      "type": "COMPARE_EQUAL",
      "alias": "",
      "left": {
        "expression_class": "BOUND_COLUMN_REF",
        "type": "BOUND_COLUMN_REF",
        "alias": "",
        "return_type": {
          "id": "INTEGER",
          "type_info": null
        },
        "binding": {
          "table_index": 0,
          "column_index": 0
        },
        "depth": 0
      },
      "right": {
        "expression_class": "BOUND_CONSTANT",
        "type": "VALUE_CONSTANT",
        "alias": "",
        "value": {
          "type": {
            "id": "INTEGER",
            "type_info": null
          },
          "is_null": false,
          "value": 42
        }
      }
    }
  ],
  "column_binding_names_by_index": ["id", "name", "value"]
}
```

### AND Conjunction

SQL: `SELECT * FROM table WHERE status = 'active' AND age > 18`

```json
{
  "filters": [
    {
      "expression_class": "BOUND_CONJUNCTION",
      "type": "CONJUNCTION_AND",
      "alias": "",
      "children": [
        {
          "expression_class": "BOUND_COMPARISON",
          "type": "COMPARE_EQUAL",
          "alias": "",
          "left": {
            "expression_class": "BOUND_COLUMN_REF",
            "type": "BOUND_COLUMN_REF",
            "alias": "",
            "return_type": {"id": "VARCHAR", "type_info": null},
            "binding": {"table_index": 0, "column_index": 0},
            "depth": 0
          },
          "right": {
            "expression_class": "BOUND_CONSTANT",
            "type": "VALUE_CONSTANT",
            "alias": "",
            "value": {
              "type": {"id": "VARCHAR", "type_info": null},
              "is_null": false,
              "value": "active"
            }
          }
        },
        {
          "expression_class": "BOUND_COMPARISON",
          "type": "COMPARE_GREATERTHAN",
          "alias": "",
          "left": {
            "expression_class": "BOUND_COLUMN_REF",
            "type": "BOUND_COLUMN_REF",
            "alias": "",
            "return_type": {"id": "INTEGER", "type_info": null},
            "binding": {"table_index": 0, "column_index": 1},
            "depth": 0
          },
          "right": {
            "expression_class": "BOUND_CONSTANT",
            "type": "VALUE_CONSTANT",
            "alias": "",
            "value": {
              "type": {"id": "INTEGER", "type_info": null},
              "is_null": false,
              "value": 18
            }
          }
        }
      ]
    }
  ],
  "column_binding_names_by_index": ["status", "age"]
}
```

### IS NULL Check

SQL: `SELECT * FROM table WHERE deleted_at IS NULL`

```json
{
  "filters": [
    {
      "expression_class": "BOUND_OPERATOR",
      "type": "OPERATOR_IS_NULL",
      "alias": "",
      "return_type": {"id": "BOOLEAN", "type_info": null},
      "children": [
        {
          "expression_class": "BOUND_COLUMN_REF",
          "type": "BOUND_COLUMN_REF",
          "alias": "",
          "return_type": {"id": "TIMESTAMP", "type_info": null},
          "binding": {"table_index": 0, "column_index": 0},
          "depth": 0
        }
      ]
    }
  ],
  "column_binding_names_by_index": ["deleted_at"]
}
```

### Function Call in Filter

SQL: `SELECT * FROM table WHERE LOWER(name) = 'john'`

```json
{
  "filters": [
    {
      "expression_class": "BOUND_COMPARISON",
      "type": "COMPARE_EQUAL",
      "alias": "",
      "left": {
        "expression_class": "BOUND_FUNCTION",
        "type": "BOUND_FUNCTION",
        "alias": "",
        "return_type": {"id": "VARCHAR", "type_info": null},
        "children": [
          {
            "expression_class": "BOUND_COLUMN_REF",
            "type": "BOUND_COLUMN_REF",
            "alias": "",
            "return_type": {"id": "VARCHAR", "type_info": null},
            "binding": {"table_index": 0, "column_index": 0},
            "depth": 0
          }
        ],
        "name": "lower",
        "arguments": [{"id": "VARCHAR", "type_info": null}],
        "original_arguments": [{"id": "VARCHAR", "type_info": null}],
        "catalog_name": "",
        "schema_name": "",
        "has_serialize": false,
        "is_operator": false
      },
      "right": {
        "expression_class": "BOUND_CONSTANT",
        "type": "VALUE_CONSTANT",
        "alias": "",
        "value": {
          "type": {"id": "VARCHAR", "type_info": null},
          "is_null": false,
          "value": "john"
        }
      }
    }
  ],
  "column_binding_names_by_index": ["name"]
}
```

### BETWEEN Expression

SQL: `SELECT * FROM table WHERE price BETWEEN 100 AND 500`

```json
{
  "filters": [
    {
      "expression_class": "BOUND_BETWEEN",
      "type": "COMPARE_BETWEEN",
      "alias": "",
      "input": {
        "expression_class": "BOUND_COLUMN_REF",
        "type": "BOUND_COLUMN_REF",
        "alias": "",
        "return_type": {"id": "DECIMAL", "type_info": {"type": "DECIMAL_TYPE_INFO", "width": 10, "scale": 2}},
        "binding": {"table_index": 0, "column_index": 0},
        "depth": 0
      },
      "lower": {
        "expression_class": "BOUND_CONSTANT",
        "type": "VALUE_CONSTANT",
        "alias": "",
        "value": {
          "type": {"id": "INTEGER", "type_info": null},
          "is_null": false,
          "value": 100
        }
      },
      "upper": {
        "expression_class": "BOUND_CONSTANT",
        "type": "VALUE_CONSTANT",
        "alias": "",
        "value": {
          "type": {"id": "INTEGER", "type_info": null},
          "is_null": false,
          "value": 500
        }
      },
      "lower_inclusive": true,
      "upper_inclusive": true
    }
  ],
  "column_binding_names_by_index": ["price"]
}
```

### IN Expression

SQL: `SELECT * FROM table WHERE status IN ('active', 'pending', 'review')`

```json
{
  "filters": [
    {
      "expression_class": "BOUND_COMPARISON",
      "type": "COMPARE_IN",
      "alias": "",
      "left": {
        "expression_class": "BOUND_COLUMN_REF",
        "type": "BOUND_COLUMN_REF",
        "alias": "",
        "return_type": {"id": "VARCHAR", "type_info": null},
        "binding": {"table_index": 0, "column_index": 0},
        "depth": 0
      },
      "right": {
        "expression_class": "BOUND_FUNCTION",
        "type": "BOUND_FUNCTION",
        "alias": "",
        "return_type": {"id": "LIST", "type_info": {"type": "LIST_TYPE_INFO", "child_type": {"id": "VARCHAR"}}},
        "children": [
          {"expression_class": "BOUND_CONSTANT", "type": "VALUE_CONSTANT", "value": {"type": {"id": "VARCHAR"}, "is_null": false, "value": "active"}},
          {"expression_class": "BOUND_CONSTANT", "type": "VALUE_CONSTANT", "value": {"type": {"id": "VARCHAR"}, "is_null": false, "value": "pending"}},
          {"expression_class": "BOUND_CONSTANT", "type": "VALUE_CONSTANT", "value": {"type": {"id": "VARCHAR"}, "is_null": false, "value": "review"}}
        ],
        "name": "list_value",
        "has_serialize": false,
        "is_operator": false
      }
    }
  ],
  "column_binding_names_by_index": ["status"]
}
```

---

## Parsing Guidelines for Go/Python

### Recommended Parsing Approach

1. **Parse the top-level structure** to get `filters` array and `column_binding_names_by_index`

2. **For each expression**, switch on `expression_class`:
   - `BOUND_COMPARISON`: Binary comparison with `left` and `right`
   - `BOUND_CONJUNCTION`: AND/OR with `children` array
   - `BOUND_CONSTANT`: Literal value in `value`
   - `BOUND_COLUMN_REF`: Column reference with `binding.column_index`
   - `BOUND_FUNCTION`: Function call with `name` and `children`
   - `BOUND_OPERATOR`: Unary/n-ary operator with `children`
   - `BOUND_CAST`: Type cast with `child` and `return_type`
   - `BOUND_BETWEEN`: BETWEEN with `input`, `lower`, `upper`

3. **Resolve column names** using `column_binding_names_by_index[binding.column_index]`

4. **Handle nested expressions** recursively

### SQL Generation Tips

- For `BOUND_COMPARISON`: `{left} {operator} {right}`
- For `CONJUNCTION_AND`: `({child1} AND {child2} AND ...)`
- For `CONJUNCTION_OR`: `({child1} OR {child2} OR ...)`
- For `OPERATOR_IS_NULL`: `{child} IS NULL`
- For `OPERATOR_IS_NOT_NULL`: `{child} IS NOT NULL`
- For `BOUND_FUNCTION`: `{name}({children[0]}, {children[1]}, ...)`
- For `BOUND_CAST`: `CAST({child} AS {return_type})`
- For `BOUND_BETWEEN`: `{input} BETWEEN {lower} AND {upper}`

### Type Mapping

Map `LogicalTypeId` to your target SQL dialect's types appropriately. DuckDB types map to standard SQL types in most cases.

---

## DuckDB Functions Reference for WHERE Clause

This section lists common DuckDB functions that may appear in filter expressions. When a function is used in a WHERE clause, it will be serialized as `BoundFunctionExpression`.

### String Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `lower` | `(string)` | VARCHAR | Convert to lowercase |
| `upper` | `(string)` | VARCHAR | Convert to uppercase |
| `length` | `(string)` | INTEGER | String length in characters |
| `strlen` | `(string)` | INTEGER | String length in bytes |
| `trim` | `(string[, characters])` | VARCHAR | Remove leading/trailing characters |
| `ltrim` | `(string[, characters])` | VARCHAR | Remove leading characters |
| `rtrim` | `(string[, characters])` | VARCHAR | Remove trailing characters |
| `left` | `(string, count)` | VARCHAR | Get leftmost N characters |
| `right` | `(string, count)` | VARCHAR | Get rightmost N characters |
| `substring` | `(string, start[, length])` | VARCHAR | Extract substring |
| `replace` | `(string, source, target)` | VARCHAR | Replace occurrences |
| `reverse` | `(string)` | VARCHAR | Reverse string |
| `repeat` | `(string, count)` | VARCHAR | Repeat string N times |
| `concat` | `(value, ...)` | VARCHAR | Concatenate values |
| `concat_ws` | `(separator, string, ...)` | VARCHAR | Concatenate with separator |
| `lpad` | `(string, count, char)` | VARCHAR | Left pad to length |
| `rpad` | `(string, count, char)` | VARCHAR | Right pad to length |
| `split_part` | `(string, separator, index)` | VARCHAR | Get Nth part after split |
| `string_split` | `(string, separator)` | LIST | Split into array |

### String Predicate Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `contains` | `(string, search)` | BOOLEAN | Check if contains substring |
| `starts_with` | `(string, prefix)` | BOOLEAN | Check if starts with prefix |
| `prefix` | `(string, prefix)` | BOOLEAN | Alias for starts_with |
| `ends_with` | `(string, suffix)` | BOOLEAN | Check if ends with suffix |
| `suffix` | `(string, suffix)` | BOOLEAN | Alias for ends_with |
| `like_escape` | `(string, pattern, escape)` | BOOLEAN | LIKE with escape character |
| `ilike_escape` | `(string, pattern, escape)` | BOOLEAN | Case-insensitive LIKE |
| `regexp_matches` | `(string, regex[, options])` | BOOLEAN | Check regex match |
| `regexp_full_match` | `(string, regex[, options])` | BOOLEAN | Check full regex match |

### String Extraction Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `regexp_extract` | `(string, regex[, group])` | VARCHAR | Extract regex match |
| `regexp_extract_all` | `(string, regex[, group])` | LIST | Extract all regex matches |
| `regexp_replace` | `(string, regex, replacement)` | VARCHAR | Replace regex matches |
| `instr` | `(string, search)` | INTEGER | Find position of substring |
| `position` | `(search IN string)` | INTEGER | Find position of substring |

### String Similarity Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `levenshtein` | `(s1, s2)` | INTEGER | Edit distance |
| `damerau_levenshtein` | `(s1, s2)` | INTEGER | Damerau-Levenshtein distance |
| `hamming` | `(s1, s2)` | INTEGER | Hamming distance |
| `jaccard` | `(s1, s2)` | DOUBLE | Jaccard similarity |
| `jaro_similarity` | `(s1, s2)` | DOUBLE | Jaro similarity |
| `jaro_winkler_similarity` | `(s1, s2)` | DOUBLE | Jaro-Winkler similarity |

### Hash Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `hash` | `(value, ...)` | UBIGINT | Hash value(s) |
| `md5` | `(string)` | VARCHAR | MD5 hash (hex string) |
| `sha1` | `(value)` | VARCHAR | SHA-1 hash |
| `sha256` | `(value)` | VARCHAR | SHA-256 hash |

### Numeric Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `abs` | `(x)` | numeric | Absolute value |
| `ceil` / `ceiling` | `(x)` | INTEGER | Round up |
| `floor` | `(x)` | INTEGER | Round down |
| `round` | `(v[, s])` | numeric | Round to scale |
| `trunc` | `(x)` | INTEGER | Truncate toward zero |
| `sign` | `(x)` | INTEGER | Sign (-1, 0, 1) |
| `sqrt` | `(x)` | numeric | Square root |
| `cbrt` | `(x)` | numeric | Cube root |
| `pow` / `power` | `(x, y)` | numeric | Power |
| `exp` | `(x)` | numeric | e^x |
| `ln` | `(x)` | numeric | Natural log |
| `log` / `log10` | `(x)` | numeric | Log base 10 |
| `log2` | `(x)` | numeric | Log base 2 |
| `sin` | `(x)` | numeric | Sine |
| `cos` | `(x)` | numeric | Cosine |
| `tan` | `(x)` | numeric | Tangent |
| `asin` | `(x)` | numeric | Inverse sine |
| `acos` | `(x)` | numeric | Inverse cosine |
| `atan` | `(x)` | numeric | Inverse tangent |
| `atan2` | `(y, x)` | numeric | Two-argument arctangent |
| `degrees` | `(x)` | numeric | Radians to degrees |
| `radians` | `(x)` | numeric | Degrees to radians |
| `pi` | `()` | numeric | Pi constant |
| `random` | `()` | DOUBLE | Random number [0, 1) |

### Numeric Predicate Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `isfinite` | `(x)` | BOOLEAN | Check if finite |
| `isinf` | `(x)` | BOOLEAN | Check if infinite |
| `isnan` | `(x)` | BOOLEAN | Check if NaN |
| `signbit` | `(x)` | BOOLEAN | Check sign bit |

### Numeric Aggregate-like Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `greatest` | `(x1, x2, ...)` | numeric | Maximum value |
| `least` | `(x1, x2, ...)` | numeric | Minimum value |
| `gcd` | `(x, y)` | INTEGER | Greatest common divisor |
| `lcm` | `(x, y)` | INTEGER | Least common multiple |

### Arithmetic Operators (as functions)

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `add` | `(x, y)` | numeric | x + y |
| `subtract` | `(x, y)` | numeric | x - y |
| `multiply` | `(x, y)` | numeric | x * y |
| `divide` | `(x, y)` | INTEGER | x // y (integer division) |
| `fdiv` | `(x, y)` | DOUBLE | x / y (float division) |
| `fmod` | `(x, y)` | DOUBLE | Floating modulo |

### Bitwise Operators (as functions)

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `bit_count` | `(x)` | INTEGER | Count set bits |
| `xor` | `(x, y)` | INTEGER | Bitwise XOR |

### Date Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `today` | `()` | DATE | Current date |
| `current_date` | `()` | DATE | Current date |
| `date_part` | `(part, date)` | INTEGER | Extract date component |
| `extract` | `(part FROM date)` | INTEGER | Extract date component |
| `date_trunc` | `(part, date)` | DATE | Truncate to precision |
| `date_add` | `(date, interval)` | TIMESTAMP | Add interval |
| `date_sub` | `(part, date1, date2)` | INTEGER | Subtract dates |
| `date_diff` | `(part, date1, date2)` | INTEGER | Difference in units |
| `dayname` | `(date)` | VARCHAR | Day name |
| `monthname` | `(date)` | VARCHAR | Month name |
| `last_day` | `(date)` | DATE | Last day of month |
| `make_date` | `(year, month, day)` | DATE | Create date |
| `julian` | `(date)` | DOUBLE | Julian day number |

### Date Part Values

Used with `date_part`, `extract`, `date_trunc`, `date_diff`:

| Part | Description |
|------|-------------|
| `'year'` / `'years'` | Year |
| `'quarter'` | Quarter (1-4) |
| `'month'` / `'months'` | Month (1-12) |
| `'week'` / `'weeks'` | Week of year |
| `'day'` / `'days'` | Day of month |
| `'dayofweek'` / `'dow'` | Day of week (0=Sunday) |
| `'dayofyear'` / `'doy'` | Day of year |
| `'hour'` / `'hours'` | Hour (0-23) |
| `'minute'` / `'minutes'` | Minute (0-59) |
| `'second'` / `'seconds'` | Second (0-59) |
| `'millisecond'` / `'milliseconds'` | Milliseconds |
| `'microsecond'` / `'microseconds'` | Microseconds |
| `'epoch'` | Seconds since epoch |

### Timestamp Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `now` | `()` | TIMESTAMP WITH TIME ZONE | Current timestamp |
| `current_timestamp` | `()` | TIMESTAMP WITH TIME ZONE | Current timestamp |
| `current_localtimestamp` | `()` | TIMESTAMP | Local timestamp |
| `age` | `(timestamp[, timestamp])` | INTERVAL | Age/difference |
| `century` | `(timestamp)` | INTEGER | Century |
| `date_part` | `(part, timestamp)` | NUMERIC | Extract component |
| `date_trunc` | `(part, timestamp)` | TIMESTAMP | Truncate to precision |
| `epoch` | `(timestamp)` | BIGINT | Seconds since epoch |
| `epoch_ms` | `(timestamp)` | BIGINT | Milliseconds since epoch |
| `epoch_us` | `(timestamp)` | BIGINT | Microseconds since epoch |
| `epoch_ns` | `(timestamp)` | HUGEINT | Nanoseconds since epoch |
| `strftime` | `(timestamp, format)` | VARCHAR | Format timestamp |
| `strptime` | `(string, format)` | TIMESTAMP | Parse timestamp |
| `try_strptime` | `(string, format)` | TIMESTAMP | Parse timestamp (NULL on error) |
| `make_timestamp` | `(y, m, d, h, min, sec)` | TIMESTAMP | Create timestamp |
| `time_bucket` | `(interval, timestamp)` | TIMESTAMP | Bucket timestamp |

### Interval Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `to_years` | `(integer)` | INTERVAL | Create year interval |
| `to_months` | `(integer)` | INTERVAL | Create month interval |
| `to_days` | `(integer)` | INTERVAL | Create day interval |
| `to_hours` | `(integer)` | INTERVAL | Create hour interval |
| `to_minutes` | `(integer)` | INTERVAL | Create minute interval |
| `to_seconds` | `(integer)` | INTERVAL | Create second interval |
| `to_milliseconds` | `(integer)` | INTERVAL | Create millisecond interval |
| `to_microseconds` | `(integer)` | INTERVAL | Create microsecond interval |

### Type Conversion Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `cast` | `(value AS type)` | target type | Explicit cast |
| `try_cast` | `(value AS type)` | target type | Cast, NULL on error |
| `typeof` | `(value)` | VARCHAR | Get type name |

### NULL Handling Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `coalesce` | `(value, ...)` | any | First non-NULL value |
| `nullif` | `(a, b)` | any | NULL if a = b |
| `ifnull` | `(value, default)` | any | Default if NULL |

### List Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `list_value` | `(value, ...)` | LIST | Create list |
| `array_length` | `(list)` | INTEGER | List length |
| `list_contains` | `(list, element)` | BOOLEAN | Check if list contains |
| `list_position` | `(list, element)` | INTEGER | Position of element |
| `list_extract` | `(list, index)` | element type | Get element at index |
| `array_extract` | `(list, index)` | element type | Alias for list_extract |
| `list_slice` | `(list, begin, end)` | LIST | Slice list |
| `list_concat` | `(list1, list2)` | LIST | Concatenate lists |
| `list_distinct` | `(list)` | LIST | Unique elements |
| `list_sort` | `(list)` | LIST | Sort elements |
| `list_reverse` | `(list)` | LIST | Reverse list |
| `flatten` | `(nested_list)` | LIST | Flatten nested list |
| `unnest` | `(list)` | TABLE | Expand list to rows |

### Struct Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `struct_pack` | `(k1 := v1, ...)` | STRUCT | Create struct |
| `struct_extract` | `(struct, 'field')` | field type | Get struct field |
| `row` | `(value, ...)` | STRUCT | Create unnamed struct |

### Map Functions

| Function | Signature | Return Type | Description |
|----------|-----------|-------------|-------------|
| `map` | `(keys_list, values_list)` | MAP | Create map |
| `map_contains` | `(map, key)` | BOOLEAN | Check if key exists |
| `map_keys` | `(map)` | LIST | Get all keys |
| `map_values` | `(map)` | LIST | Get all values |
| `map_extract` | `(map, key)` | value type | Get value by key |
| `element_at` | `(map, key)` | value type | Get value by key |

### JSON Handling Note

When these functions appear in filters, they are serialized with:
- `name`: function name (e.g., `"lower"`, `"date_part"`)
- `children`: array of argument expressions
- `arguments`: array of argument types
- `return_type`: the function's return type

Example JSON for `lower(name)`:
```json
{
  "expression_class": "BOUND_FUNCTION",
  "type": "BOUND_FUNCTION",
  "name": "lower",
  "return_type": {"id": "VARCHAR", "type_info": null},
  "children": [
    {
      "expression_class": "BOUND_COLUMN_REF",
      "type": "BOUND_COLUMN_REF",
      "return_type": {"id": "VARCHAR", "type_info": null},
      "binding": {"table_index": 0, "column_index": 0},
      "depth": 0
    }
  ],
  "arguments": [{"id": "VARCHAR", "type_info": null}],
  "original_arguments": [{"id": "VARCHAR", "type_info": null}],
  "has_serialize": false,
  "is_operator": false
}
```

### Operators Represented as Functions

Some operators are serialized as `BoundFunctionExpression` with `is_operator: true`:

| Operator | Function Name | Description |
|----------|---------------|-------------|
| `+` | `+` | Addition |
| `-` | `-` | Subtraction |
| `*` | `*` | Multiplication |
| `/` | `/` | Division |
| `%` | `%` | Modulo |
| `\|\|` | `concat` | String concatenation |
| `~~` | `~~` | LIKE operator |
| `!~~` | `!~~` | NOT LIKE |
| `~~*` | `~~*` | ILIKE (case-insensitive LIKE) |
| `!~~*` | `!~~*` | NOT ILIKE |
| `~` | `~` | Regex match |
| `!~` | `!~` | Not regex match |
| `~*` | `~*` | Case-insensitive regex |
| `!~*` | `!~*` | Not case-insensitive regex |

---

## Source Code Reference

The JSON serialization is implemented in:
- `src/airport_take_flight.cpp:509` - `AirportTakeFlightComplexFilterPushdown()`
- `src/airport_json_serializer.cpp` - JSON serializer implementation
- DuckDB source: `src/storage/serialization/serialize_expression.cpp` - Expression serialization

## External References

- [DuckDB Functions Overview](https://duckdb.org/docs/stable/sql/functions/overview)
- [DuckDB Text Functions](https://duckdb.org/docs/stable/sql/functions/text)
- [DuckDB Numeric Functions](https://duckdb.org/docs/stable/sql/functions/numeric)
- [DuckDB Date Functions](https://duckdb.org/docs/stable/sql/functions/date)
- [DuckDB Timestamp Functions](https://duckdb.org/docs/stable/sql/functions/timestamp)
