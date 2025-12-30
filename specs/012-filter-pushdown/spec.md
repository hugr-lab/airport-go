# Feature Specification: Filter Pushdown Encoder-Decoder Package

**Feature Branch**: `012-filter-pushdown`
**Created**: 2025-12-29
**Status**: Draft
**Input**: User description: "Implement filter pushdown encoder-decoder package that parses DuckDB filter JSON to Go types and encodes to configurable SQL dialects with column mapping support"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Parse Filter JSON to Go Types (Priority: P1)

As a Flight server developer, I want to parse the filter pushdown JSON received from DuckDB into strongly-typed Go structures so that I can programmatically inspect and process filter conditions.

**Why this priority**: This is the foundational capability. Without parsing JSON to Go types, no other filter processing is possible. Server developers need to understand what filters were requested to make decisions about data retrieval optimization.

**Independent Test**: Can be fully tested by providing sample filter JSON bytes and validating the resulting Go struct matches expected expression tree. Delivers immediate value for server developers who want to inspect filters.

**Acceptance Scenarios**:

1. **Given** a filter JSON containing a simple equality comparison (e.g., `WHERE id = 42`), **When** parsing the JSON, **Then** the result is a Go struct with expression class BOUND_COMPARISON, type COMPARE_EQUAL, and correctly parsed left/right operands
2. **Given** a filter JSON containing nested AND/OR conditions, **When** parsing the JSON, **Then** the result is a properly nested expression tree with conjunction types and child expressions
3. **Given** a filter JSON with column references, **When** parsing the JSON, **Then** column names are resolved from the column_binding_names_by_index array
4. **Given** invalid or malformed JSON, **When** attempting to parse, **Then** a clear error is returned indicating the parsing failure

---

### User Story 2 - Encode Filters to DuckDB SQL (Priority: P1)

As a Flight server developer, I want to encode parsed filter expressions back to DuckDB SQL syntax so that I can forward filter conditions to underlying DuckDB databases or use SQL-based storage engines.

**Why this priority**: DuckDB SQL encoding is the primary use case specified by the user. Most server implementations will delegate filtering to a SQL-capable backend, making this essential.

**Independent Test**: Can be tested by parsing a filter JSON, encoding to DuckDB SQL, and validating the output SQL string matches expected syntax. Can be end-to-end tested with actual DuckDB queries.

**Acceptance Scenarios**:

1. **Given** a parsed comparison expression (=, <, >, <=, >=, <>), **When** encoding to DuckDB SQL, **Then** the output is valid SQL with correct operator syntax
2. **Given** a parsed conjunction (AND/OR) with multiple children, **When** encoding to DuckDB SQL, **Then** the output correctly groups conditions with parentheses
3. **Given** a parsed function expression (e.g., LOWER, LENGTH), **When** encoding to DuckDB SQL, **Then** the output uses correct DuckDB function syntax
4. **Given** a parsed BETWEEN expression, **When** encoding to DuckDB SQL, **Then** the output produces `column BETWEEN lower AND upper` syntax
5. **Given** an IS NULL/IS NOT NULL operator, **When** encoding to DuckDB SQL, **Then** the output produces correct NULL checking syntax
6. **Given** an AND conjunction with one unsupported child expression, **When** encoding to DuckDB SQL, **Then** the unsupported child is skipped and remaining conditions are encoded
7. **Given** an OR conjunction with one or more unsupported child expressions, **When** encoding to DuckDB SQL, **Then** the entire OR expression is skipped (produces widest filter)
8. **Given** all filter expressions are unsupported, **When** encoding to DuckDB SQL, **Then** no WHERE clause is produced (widest possible filter, client applies filtering)

---

### User Story 3 - Column Name Mapping (Priority: P2)

As a Flight server developer, I want to map column names during encoding so that I can translate DuckDB column names to different column names in my backend storage system.

**Why this priority**: Column name mapping is essential for real-world use where Flight table schemas may differ from backend storage schemas. This enables flexible integration with various data sources.

**Independent Test**: Can be tested by providing a column mapping dictionary and verifying encoded SQL uses mapped names instead of original names.

**Acceptance Scenarios**:

1. **Given** a filter referencing column "user_id" and a mapping {"user_id": "uid"}, **When** encoding to SQL, **Then** the output uses "uid" instead of "user_id"
2. **Given** a filter with unmapped columns, **When** encoding to SQL, **Then** unmapped columns retain their original names
3. **Given** a complex expression with multiple column references, **When** encoding with mappings, **Then** all column references are correctly mapped

---

### User Story 4 - Column Expression Replacement (Priority: P2)

As a Flight server developer, I want to replace column names with SQL expressions during encoding so that I can handle computed columns or complex transformations.

**Why this priority**: This extends column mapping to support computed columns, which is common in data virtualization scenarios where Flight tables present transformed views of underlying data.

**Independent Test**: Can be tested by providing expression mappings and verifying encoded SQL substitutes column references with the specified expressions.

**Acceptance Scenarios**:

1. **Given** a filter on column "full_name" and expression mapping {"full_name": "CONCAT(first_name, ' ', last_name)"}, **When** encoding to SQL, **Then** the output replaces "full_name" with the CONCAT expression
2. **Given** nested expressions using mapped columns, **When** encoding, **Then** expression replacements are correctly integrated into the SQL

---

### User Story 5 - Custom SQL Dialect Support (Priority: P3)

As a Flight server developer, I want to implement custom SQL dialect encoders so that I can target different database backends (PostgreSQL, MySQL, etc.) with their specific syntax.

**Why this priority**: While DuckDB is the primary target, extensibility to other SQL dialects enables broader adoption. This is lower priority since DuckDB encoding covers the primary use case.

**Independent Test**: Can be tested by implementing a custom dialect encoder and verifying output matches expected dialect-specific syntax.

**Acceptance Scenarios**:

1. **Given** a custom dialect encoder with different function names (e.g., PostgreSQL's "lower" vs DuckDB's "lower"), **When** encoding functions, **Then** the dialect-specific function name is used
2. **Given** a custom dialect with different type casting syntax, **When** encoding CAST expressions, **Then** the dialect-specific cast syntax is used
3. **Given** operators that differ between dialects (e.g., string concatenation), **When** encoding, **Then** the correct operator for the target dialect is produced

---

### User Story 6 - All DuckDB Data Types Support (Priority: P1)

As a Flight server developer, I want filter parsing and encoding to support all data types that DuckDB can send through Airport so that I can handle any filter regardless of column types.

**Why this priority**: Complete data type support is essential for production use. Incomplete type support would limit the feature's usefulness.

**Independent Test**: Can be tested by creating filters on columns of each supported type and verifying both parsing and encoding work correctly.

**Acceptance Scenarios**:

1. **Given** filters on numeric types (TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, FLOAT, DOUBLE, DECIMAL), **When** parsing and encoding, **Then** values are correctly preserved with appropriate type handling
2. **Given** filters on string types (VARCHAR, CHAR, BLOB), **When** parsing and encoding, **Then** string values are correctly quoted and escaped
3. **Given** filters on temporal types (DATE, TIME, TIME_TZ, TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMP_SEC, INTERVAL), **When** parsing and encoding, **Then** temporal values use correct SQL literal syntax
4. **Given** filters on special types (BOOLEAN, UUID, NULL), **When** parsing and encoding, **Then** values use correct SQL syntax
5. **Given** filters on complex types (LIST, STRUCT, MAP, ARRAY), **When** parsing and encoding, **Then** complex values are correctly represented or an informative error indicates unsupported operations

---

### Edge Cases

- What happens when filter JSON contains expression types not yet supported by the encoder? Ignore the unsupported expression to produce widest filter. For AND: skip unsupported child, keep others. For OR: skip entire OR expression if any child unsupported. DuckDB client applies filters client-side as fallback.
- How does the encoder handle NULL values in comparisons? Produce appropriate IS NULL / IS NOT NULL syntax as needed
- What happens with deeply nested conjunctions (more than 10 levels)? Process correctly without stack overflow or excessive memory use
- How are special characters in string values handled? Properly escape quotes and special characters according to target SQL dialect
- What happens when column_binding_names_by_index references an invalid index? Return an error indicating the invalid column binding
- How are HUGEINT/UHUGEINT 128-bit values encoded? Convert upper/lower components to proper numeric representation
- What happens with non-UTF8 blob values? Handle base64 encoded values correctly

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST parse filter pushdown JSON from DuckDB Airport extension into Go data structures
- **FR-002**: System MUST support all expression classes defined in the filter pushdown specification: BOUND_COMPARISON, BOUND_CONJUNCTION, BOUND_CONSTANT, BOUND_COLUMN_REF, BOUND_FUNCTION, BOUND_CAST, BOUND_BETWEEN, BOUND_OPERATOR, BOUND_CASE, BOUND_AGGREGATE, BOUND_WINDOW, BOUND_PARAMETER, BOUND_REF, BOUND_LAMBDA
- **FR-003**: System MUST support all comparison operators: EQUAL, NOTEQUAL, LESSTHAN, GREATERTHAN, LESSTHANOREQUALTO, GREATERTHANOREQUALTO, IN, NOT_IN, DISTINCT_FROM, NOT_DISTINCT_FROM, BETWEEN, NOT_BETWEEN
- **FR-004**: System MUST support conjunction operators: AND, OR
- **FR-005**: System MUST support unary operators: NOT, IS_NULL, IS_NOT_NULL
- **FR-006**: System MUST resolve column names from column_binding_names_by_index when parsing BOUND_COLUMN_REF expressions
- **FR-007**: System MUST provide a DuckDB SQL encoder that produces valid DuckDB SQL from parsed expressions
- **FR-008**: System MUST support column name mapping during encoding, allowing original column names to be replaced with alternative names
- **FR-009**: System MUST support column expression mapping during encoding, allowing column references to be replaced with SQL expressions
- **FR-010**: System MUST provide an extensible interface for implementing custom SQL dialect encoders
- **FR-011**: System MUST support all LogicalType values from DuckDB: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, FLOAT, DOUBLE, DECIMAL, VARCHAR, CHAR, BLOB, DATE, TIME, TIME_TZ, TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMP_SEC, INTERVAL, UUID, STRUCT, LIST, MAP, ARRAY, ENUM
- **FR-012**: System MUST handle Value serialization including null values, hugeint/uhugeint upper/lower components, and base64-encoded non-UTF8 strings
- **FR-013**: System MUST handle function expressions including the function name, arguments, return type, and is_operator flag
- **FR-014**: System MUST handle CAST and TRY_CAST expressions with proper type conversion syntax
- **FR-015**: System MUST properly escape string literals according to target SQL dialect rules
- **FR-016**: System MUST gracefully handle unsupported expressions during encoding: ignore unsupported expressions to produce the widest possible filter (DuckDB client applies filters client-side as fallback). For AND conjunctions, skip only the unsupported child while preserving other conditions. For OR conjunctions, if any child is unsupported, skip the entire OR expression. System MUST return informative errors only for malformed JSON input.
- **FR-017**: Package MUST be in a separate module/package from core airport-go functionality to allow optional import
- **FR-018**: Package MUST NOT modify existing airport-go public APIs - ScanOpts.Filter remains []byte
- **FR-019**: System MUST provide example code demonstrating filter parsing and SQL encoding
- **FR-020**: System MUST include integration tests using DuckDB client with at least 20 different WHERE conditions covering all supported expression types and data types

### Key Entities

- **Expression**: Base interface for all filter expression types, containing expression class, type, and alias
- **ComparisonExpression**: Binary comparison with left and right operands
- **ConjunctionExpression**: AND/OR expression with child expressions
- **ConstantExpression**: Literal value with type information
- **ColumnRefExpression**: Reference to a table column via binding index
- **FunctionExpression**: Function call with name and argument expressions
- **CastExpression**: Type cast with child expression and target type
- **BetweenExpression**: BETWEEN check with input, lower, and upper bounds
- **OperatorExpression**: Unary or n-ary operator with children
- **LogicalType**: Type information including type ID and optional extra type info
- **Value**: Typed value with null indicator and actual value data
- **ColumnBinding**: Table and column index pair for column references
- **FilterPushdown**: Top-level structure containing filters array and column name mappings
- **SQLEncoder**: Interface for encoding expressions to SQL strings
- **ColumnMapper**: Interface for translating column names/expressions during encoding

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Filter JSON parsing completes in under 1 millisecond for typical single-table filters with up to 10 conditions
- **SC-002**: SQL encoding produces syntactically valid SQL for all 20+ integration test cases
- **SC-003**: 100% of DuckDB logical types are correctly parsed and can be encoded to SQL
- **SC-004**: Package has zero dependencies on DuckDB C library - pure Go implementation
- **SC-005**: All integration tests pass using DuckDB client executing queries with filter pushdown
- **SC-006**: Example code compiles and runs successfully demonstrating all major features
- **SC-007**: Custom dialect encoder implementation requires implementing only a minimal interface (standard adapter pattern)
- **SC-008**: Memory allocation for parsing and encoding is proportional to expression tree size with no memory leaks

## Clarifications

### Session 2025-12-29

- Q: How should unsupported expressions be handled during encoding? → A: Ignore unsupported expressions to produce widest possible filter (DuckDB client applies filters client-side anyway). For AND: skip unsupported child, keep others. For OR: if any child unsupported, skip entire OR expression.

## Assumptions

- DuckDB Airport extension JSON format follows the structure documented in filter_pushdown.md
- JSON serialization format may change between DuckDB versions; the implementation should be version-aware where practical
- Integration tests will use the DuckDB Airport extension available from airport.query.farm
- The package will be implemented in Go following the project's existing code style and patterns
- Standard SQL string escaping (doubling single quotes) is acceptable for DuckDB dialect
- Complex nested types (STRUCT, LIST, MAP) filtering may have limited support for operations beyond direct comparison
