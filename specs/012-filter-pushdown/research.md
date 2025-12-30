# Research: Filter Pushdown Encoder-Decoder

**Feature**: 012-filter-pushdown | **Date**: 2025-12-29

## Research Tasks

### 1. JSON Parsing Strategy

**Decision**: Use `encoding/json` with custom `UnmarshalJSON` for polymorphic expression types.

**Rationale**:
- Standard library, no additional dependencies
- Well-tested, performant for typical JSON sizes
- Custom unmarshaling handles polymorphic `Expression` types based on `expression_class` field

**Alternatives Considered**:
- `json-iterator/go`: Faster but adds dependency, not needed for <1MB payloads
- `easyjson`: Code generation complexity not justified for this use case
- Manual parsing: Error-prone, harder to maintain

**Implementation Approach**:
```go
// Two-phase parsing:
// 1. Parse into intermediate rawExpression to read expression_class
// 2. Based on expression_class, unmarshal into specific type
type rawExpression struct {
    ExpressionClass string          `json:"expression_class"`
    Type            string          `json:"type"`
    Alias           string          `json:"alias"`
    Raw             json.RawMessage `json:"-"` // Full JSON for second pass
}
```

### 2. Expression Type Hierarchy

**Decision**: Use Go interfaces with concrete struct types (no reflection at runtime).

**Rationale**:
- Type-safe at compile time
- Clear, idiomatic Go pattern
- Easy to extend with new expression types

**Alternatives Considered**:
- Single struct with union fields: Wasteful memory, unclear which fields apply
- `any` with type assertions: Loses type safety, harder to use
- Code generation: Overhead not justified for ~15 expression types

**Implementation Pattern**:
```go
// Expression is the base interface for all filter expressions
type Expression interface {
    expressionMarker() // Marker method to prevent external implementation
    Class() ExpressionClass
    Type() ExpressionType
}

// Each expression type implements Expression
type ComparisonExpression struct {
    BaseExpression
    Left  Expression
    Right Expression
}
```

### 3. SQL Encoding Architecture

**Decision**: Encoder interface with dialect implementations, using strings.Builder for output.

**Rationale**:
- Interface allows custom dialects without modifying core code
- strings.Builder is efficient and idiomatic for string concatenation
- Single-pass traversal with recursive encoding

**Alternatives Considered**:
- Template-based SQL generation: Over-engineered for expression trees
- AST transformation then printing: Extra complexity, no benefit
- Direct string concatenation: Less efficient, harder to optimize

**Encoder Interface**:
```go
// Encoder converts parsed expressions to SQL strings
type Encoder interface {
    // Encode converts an expression to SQL, returning empty string if unsupported
    Encode(expr Expression) string

    // EncodeFilters converts all filters to a WHERE clause (without "WHERE" keyword)
    // Returns empty string if no supported filters
    EncodeFilters(fp *FilterPushdown) string
}

// EncoderOptions configures encoding behavior
type EncoderOptions struct {
    // ColumnMapping maps original column names to target names
    // If a column is not in the map, original name is used
    ColumnMapping map[string]string

    // ColumnExpressions maps column names to SQL expressions
    // Takes precedence over ColumnMapping if both are set for a column
    ColumnExpressions map[string]string
}
```

### 4. Unsupported Expression Handling

**Decision**: Skip unsupported expressions gracefully, applying conjunction-aware rules.

**Rationale**:
- DuckDB client applies filters client-side as fallback
- Wider server-side filter is safe (returns superset of matching rows)
- Better UX than errors for partially supported filters

**Rules**:
1. **AND conjunction**: Skip unsupported child, encode remaining children
2. **OR conjunction**: If any child unsupported, skip entire OR (cannot safely skip part)
3. **Top-level**: Each filter is implicitly AND'ed; skip unsupported top-level filters
4. **All unsupported**: Return empty string (no WHERE clause)

**Implementation**:
```go
func (e *DuckDBEncoder) encodeConjunction(c *ConjunctionExpression) string {
    var parts []string
    for _, child := range c.Children {
        if encoded := e.Encode(child); encoded != "" {
            parts = append(parts, encoded)
        }
    }

    if c.Type == ConjunctionOR && len(parts) != len(c.Children) {
        // OR with unsupported child: skip entire expression
        return ""
    }

    if len(parts) == 0 {
        return ""
    }
    if len(parts) == 1 {
        return parts[0]
    }

    op := " AND "
    if c.Type == ConjunctionOR {
        op = " OR "
    }
    return "(" + strings.Join(parts, op) + ")"
}
```

### 5. Value Serialization

**Decision**: Type-aware serialization with proper SQL literal syntax for each DuckDB type.

**Rationale**:
- Different types require different SQL syntax (strings quoted, dates cast, etc.)
- HUGEINT/UHUGEINT need special handling (upper/lower components)
- Base64 for non-UTF8 blobs

**Type Mapping**:

| DuckDB Type | Go Type | SQL Output |
|-------------|---------|------------|
| BOOLEAN | bool | `TRUE` / `FALSE` |
| TINYINT..BIGINT | int64 | `123` |
| UTINYINT..UBIGINT | uint64 | `123` |
| HUGEINT/UHUGEINT | struct{Upper,Lower} | Computed decimal string |
| FLOAT/DOUBLE | float64 | `123.456` |
| DECIMAL | string | `123.45` (preserve precision) |
| VARCHAR/CHAR | string | `'escaped''value'` |
| BLOB | []byte/base64 | `'\x...'` or decode base64 |
| DATE | int (days) | `DATE '2024-01-15'` |
| TIME | int (micros) | `TIME '12:30:00'` |
| TIMESTAMP | int (micros) | `TIMESTAMP '2024-01-15 12:30:00'` |
| INTERVAL | struct | `INTERVAL '...'` |
| UUID | string | `'550e8400-...'` |
| NULL | nil | `NULL` |

### 6. String Escaping

**Decision**: Double single quotes for string values (standard SQL escaping).

**Rationale**:
- DuckDB follows standard SQL string escaping
- Simple, well-understood pattern
- No special characters beyond quotes need escaping in DuckDB

**Implementation**:
```go
func escapeString(s string) string {
    return strings.ReplaceAll(s, "'", "''")
}

func quoteLiteral(s string) string {
    return "'" + escapeString(s) + "'"
}
```

### 7. Column Name Handling

**Decision**: Support both simple name mapping and expression substitution.

**Rationale**:
- Simple mapping covers schema translation use case
- Expression substitution covers computed columns
- Both can coexist (expression takes precedence)

**Priority Order**:
1. If column has expression mapping → substitute raw SQL expression
2. If column has name mapping → use mapped name
3. Otherwise → use original column name (quoted if needed)

**Column Quoting**:
```go
func quoteIdentifier(name string) string {
    // DuckDB uses double quotes for identifiers
    // Only quote if contains special characters or is reserved word
    if needsQuoting(name) {
        return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
    }
    return name
}
```

### 8. Testing Strategy

**Decision**: Three-tier testing approach.

**Unit Tests** (filter package):
- Parse individual expression types
- Encode each expression type to SQL
- Column mapping/expression substitution
- Unsupported expression handling (AND/OR rules)
- All data type value serialization

**Integration Tests** (tests module with DuckDB):
- 20+ WHERE conditions covering:
  - Simple comparisons: `=`, `<>`, `<`, `>`, `<=`, `>=`
  - IN/NOT IN with value lists
  - BETWEEN expressions
  - IS NULL / IS NOT NULL
  - AND/OR conjunctions (nested)
  - Functions: LOWER, UPPER, LENGTH, etc.
  - Type casts
  - Mixed data types: integers, strings, dates, timestamps, UUID

**Example Tests** (examples module):
- Runnable example demonstrating parse → encode flow
- Column mapping example
- Custom dialect example (skeleton)

## Dependencies

No new external dependencies required. All functionality uses Go standard library:
- `encoding/json` - JSON parsing
- `strings` - String manipulation, Builder
- `strconv` - Number formatting
- `fmt` - Formatting
- `math/big` - HUGEINT calculations (if needed, otherwise bit manipulation)

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| DuckDB JSON format changes | Medium | High | Version field in JSON, document compatibility |
| Unsupported function in WHERE | High | Low | Skip gracefully, DuckDB applies client-side |
| Complex nested types (STRUCT, LIST) | Medium | Medium | Support basic comparisons, skip complex operations |
| Performance with very large filters | Low | Medium | Profile if reported, optimize hot paths |

## Open Questions (Resolved)

All questions resolved through spec clarifications and research:

1. ✅ Unsupported expression handling → Skip gracefully (clarified in spec)
2. ✅ JSON parsing strategy → Standard library with custom unmarshaling
3. ✅ Encoder architecture → Interface with dialect implementations
4. ✅ String escaping → Double single quotes (standard SQL)
5. ✅ Testing approach → Unit + integration + examples
