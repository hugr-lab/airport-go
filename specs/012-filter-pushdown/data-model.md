# Data Model: Filter Pushdown Encoder-Decoder

**Feature**: 012-filter-pushdown | **Date**: 2025-12-29

## Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        FilterPushdown                            │
│  - Filters: []Expression                                        │
│  - ColumnBindings: []string                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ contains
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Expression (interface)                       │
│  + Class() ExpressionClass                                      │
│  + Type() ExpressionType                                        │
│  + Alias() string                                               │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┬────────────────┬──────────────────┐
          │                   │                   │                │                  │
          ▼                   ▼                   ▼                ▼                  ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌────────────────┐
│ ComparisonExpr  │ │ ConjunctionExpr │ │  ConstantExpr   │ │ColumnRefExpr │ │  FunctionExpr  │
│ - Left: Expr    │ │ - Children:     │ │ - Value: Value  │ │ - Binding:   │ │ - Name: string │
│ - Right: Expr   │ │   []Expression  │ │                 │ │   ColBinding │ │ - Children:    │
└─────────────────┘ └─────────────────┘ └─────────────────┘ │ - ReturnType │ │   []Expression │
                                                            │ - Depth: int │ │ - IsOperator   │
                                                            └──────────────┘ └────────────────┘
          │                   │
          │                   ├───────────────────┬────────────────┬──────────────────┐
          │                   │                   │                │                  │
          ▼                   ▼                   ▼                ▼                  ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌────────────────┐
│   CastExpr      │ │  BetweenExpr    │ │  OperatorExpr   │ │   CaseExpr   │ │  ParameterExpr │
│ - Child: Expr   │ │ - Input: Expr   │ │ - Children:     │ │ - CaseChecks │ │ - Identifier   │
│ - TargetType    │ │ - Lower: Expr   │ │   []Expression  │ │ - ElseExpr   │ │ - ReturnType   │
│ - TryCast: bool │ │ - Upper: Expr   │ │ - ReturnType    │ │              │ │                │
└─────────────────┘ │ - LowerIncl     │ └─────────────────┘ └──────────────┘ └────────────────┘
                    │ - UpperIncl     │
                    └─────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                          Value                                   │
│  - Type: LogicalType                                            │
│  - IsNull: bool                                                 │
│  - Data: any (type-specific: int64, string, float64, etc.)     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ has type
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        LogicalType                               │
│  - ID: LogicalTypeID                                            │
│  - TypeInfo: ExtraTypeInfo (for DECIMAL, LIST, STRUCT, etc.)   │
└─────────────────────────────────────────────────────────────────┘
```

## Core Entities

### FilterPushdown

Top-level container for parsed filter JSON.

| Field | Type | Description |
|-------|------|-------------|
| Filters | []Expression | Array of filter expressions (implicitly AND'ed) |
| ColumnBindings | []string | Column names by binding index |

**Validation Rules**:
- ColumnBindings must be non-empty if any ColumnRefExpression is present
- Each filter must be a valid Expression

### Expression (Interface)

Base interface for all expression types. Implemented by concrete types.

| Method | Return | Description |
|--------|--------|-------------|
| Class() | ExpressionClass | Expression class (BOUND_COMPARISON, etc.) |
| Type() | ExpressionType | Specific expression type |
| Alias() | string | Optional alias (usually empty) |

### ComparisonExpression

Binary comparison operation.

| Field | Type | Description |
|-------|------|-------------|
| Left | Expression | Left operand |
| Right | Expression | Right operand |
| ComparisonType | ExpressionType | COMPARE_EQUAL, COMPARE_LESSTHAN, etc. |

**Validation Rules**:
- Left and Right must not be nil
- ComparisonType must be a valid comparison operator

### ConjunctionExpression

Logical AND/OR with multiple children.

| Field | Type | Description |
|-------|------|-------------|
| Children | []Expression | Child expressions to combine |
| ConjunctionType | ExpressionType | CONJUNCTION_AND or CONJUNCTION_OR |

**Validation Rules**:
- Children must have at least 2 elements
- ConjunctionType must be AND or OR

### ConstantExpression

Literal value with type information.

| Field | Type | Description |
|-------|------|-------------|
| Val | Value | The constant value |

### ColumnRefExpression

Reference to a table column.

| Field | Type | Description |
|-------|------|-------------|
| Binding | ColumnBinding | Table and column indices |
| ReturnType | LogicalType | Column's data type |
| Depth | int | Depth in nested queries (usually 0) |

**Resolved Property**:
- ColumnName: Resolved from FilterPushdown.ColumnBindings[Binding.ColumnIndex]

### FunctionExpression

Function call with arguments.

| Field | Type | Description |
|-------|------|-------------|
| Name | string | Function name (e.g., "lower", "length") |
| Children | []Expression | Function arguments |
| ReturnType | LogicalType | Return type |
| IsOperator | bool | Whether this represents an operator |
| CatalogName | string | Catalog (optional) |
| SchemaName | string | Schema (optional) |

### CastExpression

Type cast operation.

| Field | Type | Description |
|-------|------|-------------|
| Child | Expression | Expression to cast |
| TargetType | LogicalType | Target type |
| TryCast | bool | If true, return NULL on failure |

### BetweenExpression

BETWEEN lower AND upper check.

| Field | Type | Description |
|-------|------|-------------|
| Input | Expression | Value to test |
| Lower | Expression | Lower bound |
| Upper | Expression | Upper bound |
| LowerInclusive | bool | Include lower bound (usually true) |
| UpperInclusive | bool | Include upper bound (usually true) |

### OperatorExpression

Unary or n-ary operator.

| Field | Type | Description |
|-------|------|-------------|
| Children | []Expression | Operand(s) |
| OperatorType | ExpressionType | OPERATOR_IS_NULL, OPERATOR_NOT, etc. |
| ReturnType | LogicalType | Return type |

### CaseExpression

SQL CASE expression.

| Field | Type | Description |
|-------|------|-------------|
| CaseChecks | []CaseCheck | WHEN...THEN pairs |
| ElseExpr | Expression | ELSE expression (optional) |
| ReturnType | LogicalType | Return type |

### CaseCheck

Single WHEN...THEN pair.

| Field | Type | Description |
|-------|------|-------------|
| WhenExpr | Expression | WHEN condition |
| ThenExpr | Expression | THEN result |

## Value Types

### Value

Typed value container.

| Field | Type | Description |
|-------|------|-------------|
| Type | LogicalType | Value's logical type |
| IsNull | bool | Whether value is NULL |
| Data | any | Type-specific data |

**Data field types by LogicalTypeID**:

| LogicalTypeID | Go Data Type |
|---------------|--------------|
| BOOLEAN | bool |
| TINYINT, SMALLINT, INTEGER, BIGINT | int64 |
| UTINYINT, USMALLINT, UINTEGER, UBIGINT | uint64 |
| HUGEINT, UHUGEINT | HugeInt struct |
| FLOAT, DOUBLE | float64 |
| DECIMAL | string |
| VARCHAR, CHAR | string |
| BLOB | []byte or Base64String |
| DATE | int64 (days since epoch) |
| TIME, TIME_TZ | int64 (microseconds) |
| TIMESTAMP, TIMESTAMP_* | int64 (microseconds) |
| INTERVAL | Interval struct |
| UUID | string |
| LIST, ARRAY | []Value |
| STRUCT | map[string]Value |
| MAP | []MapEntry |

### HugeInt

128-bit integer representation.

| Field | Type | Description |
|-------|------|-------------|
| Upper | int64 | Upper 64 bits |
| Lower | uint64 | Lower 64 bits |

### Interval

Time interval representation.

| Field | Type | Description |
|-------|------|-------------|
| Months | int32 | Number of months |
| Days | int32 | Number of days |
| Micros | int64 | Microseconds |

### LogicalType

Type information with optional extra info.

| Field | Type | Description |
|-------|------|-------------|
| ID | LogicalTypeID | Type identifier |
| TypeInfo | ExtraTypeInfo | Additional type info (nullable) |

### ExtraTypeInfo (variants)

**DecimalTypeInfo**:
| Field | Type | Description |
|-------|------|-------------|
| Width | int | Total digits |
| Scale | int | Decimal places |

**ListTypeInfo**:
| Field | Type | Description |
|-------|------|-------------|
| ChildType | LogicalType | Element type |

**StructTypeInfo**:
| Field | Type | Description |
|-------|------|-------------|
| ChildTypes | []StructField | Field definitions |

**ArrayTypeInfo**:
| Field | Type | Description |
|-------|------|-------------|
| ChildType | LogicalType | Element type |
| Size | int | Fixed array size |

## Supporting Types

### ColumnBinding

Column reference binding.

| Field | Type | Description |
|-------|------|-------------|
| TableIndex | int | Table index in query plan |
| ColumnIndex | int | Column index within table |

### ExpressionClass (enum)

| Value | Description |
|-------|-------------|
| BoundComparison | Comparison expression |
| BoundConjunction | AND/OR conjunction |
| BoundConstant | Constant value |
| BoundColumnRef | Column reference |
| BoundFunction | Function call |
| BoundCast | Type cast |
| BoundBetween | BETWEEN expression |
| BoundOperator | Operator expression |
| BoundCase | CASE expression |
| BoundAggregate | Aggregate function |
| BoundWindow | Window function |
| BoundParameter | Parameter reference |
| BoundRef | Reference expression |
| BoundLambda | Lambda expression |

### ExpressionType (enum)

See filter_pushdown.md for complete enumeration. Key values:

| Category | Values |
|----------|--------|
| Comparison | COMPARE_EQUAL, COMPARE_NOTEQUAL, COMPARE_LESSTHAN, COMPARE_GREATERTHAN, COMPARE_LESSTHANOREQUALTO, COMPARE_GREATERTHANOREQUALTO, COMPARE_IN, COMPARE_NOT_IN, COMPARE_BETWEEN, COMPARE_NOT_BETWEEN, COMPARE_DISTINCT_FROM, COMPARE_NOT_DISTINCT_FROM |
| Conjunction | CONJUNCTION_AND, CONJUNCTION_OR |
| Operator | OPERATOR_NOT, OPERATOR_IS_NULL, OPERATOR_IS_NOT_NULL |
| Value | VALUE_CONSTANT, VALUE_NULL |
| Function | BOUND_FUNCTION |
| Cast | CAST |

### LogicalTypeID (enum)

| Category | Values |
|----------|--------|
| Boolean | BOOLEAN |
| Integers | TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT |
| Floats | FLOAT, DOUBLE, DECIMAL |
| Strings | VARCHAR, CHAR, BLOB |
| Temporal | DATE, TIME, TIME_TZ, TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMP_SEC, INTERVAL |
| Special | UUID, NULL, UNKNOWN, ANY |
| Nested | LIST, STRUCT, MAP, ARRAY, ENUM |

## State Transitions

Not applicable - entities are immutable after parsing.

## Indexes and Constraints

Not applicable - this is an in-memory data model, not a database schema.
