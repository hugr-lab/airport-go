# Tasks: Filter Pushdown Encoder-Decoder Package

**Input**: Design documents from `/specs/012-filter-pushdown/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Unit tests and integration tests are required per spec FR-020 and constitution Testing Standards.

**Organization**: Tasks grouped by user story for independent implementation and testing.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (US1-US6)
- File paths relative to repository root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Package initialization and basic structure

- [ ] T001 Create filter/ package directory structure per plan.md in filter/
- [ ] T002 [P] Create package documentation in filter/doc.go
- [ ] T003 [P] Define ExpressionClass enum constants in filter/types.go
- [ ] T004 [P] Define ExpressionType enum constants in filter/types.go
- [ ] T005 [P] Define LogicalTypeID enum constants in filter/logical_types.go

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core types that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T006 Define Expression interface with Class(), Type(), Alias() methods in filter/types.go
- [ ] T007 Define BaseExpression struct embedding common fields in filter/types.go
- [ ] T008 Define LogicalType struct with ID and TypeInfo in filter/logical_types.go
- [ ] T009 Define ExtraTypeInfo interface and variants (DecimalTypeInfo, ListTypeInfo, StructTypeInfo, ArrayTypeInfo) in filter/logical_types.go
- [ ] T010 Define Value struct with Type, IsNull, Data in filter/logical_types.go
- [ ] T011 Define ColumnBinding struct with TableIndex, ColumnIndex in filter/types.go
- [ ] T012 Define FilterPushdown struct with Filters and ColumnBindings in filter/types.go
- [ ] T013 Define HugeInt struct for 128-bit integers in filter/logical_types.go
- [ ] T014 Define Interval struct for temporal intervals in filter/logical_types.go

**Checkpoint**: Foundation ready - user story implementation can now begin

---

## Phase 3: User Story 1 - Parse Filter JSON to Go Types (Priority: P1) 🎯 MVP

**Goal**: Parse DuckDB filter pushdown JSON into strongly-typed Go structures

**Independent Test**: Provide sample filter JSON bytes, validate resulting Go struct matches expected expression tree

### Expression Types for US1

- [ ] T015 [P] [US1] Define ComparisonExpression struct with Left, Right fields in filter/types.go
- [ ] T016 [P] [US1] Define ConjunctionExpression struct with Children field in filter/types.go
- [ ] T017 [P] [US1] Define ConstantExpression struct with Value field in filter/types.go
- [ ] T018 [P] [US1] Define ColumnRefExpression struct with Binding, ReturnType, Depth in filter/types.go
- [ ] T019 [P] [US1] Define FunctionExpression struct with Name, Children, ReturnType, IsOperator in filter/types.go
- [ ] T020 [P] [US1] Define CastExpression struct with Child, TargetType, TryCast in filter/types.go
- [ ] T021 [P] [US1] Define BetweenExpression struct with Input, Lower, Upper bounds in filter/types.go
- [ ] T022 [P] [US1] Define OperatorExpression struct with Children, OperatorType in filter/types.go
- [ ] T023 [P] [US1] Define CaseExpression and CaseCheck structs in filter/types.go

### JSON Parsing Implementation for US1

- [ ] T024 [US1] Implement rawExpression intermediate struct for two-phase parsing in filter/parse.go
- [ ] T025 [US1] Implement parseExpression() with expression_class dispatch in filter/parse.go
- [ ] T026 [US1] Implement parseComparisonExpression() in filter/parse.go
- [ ] T027 [US1] Implement parseConjunctionExpression() in filter/parse.go
- [ ] T028 [US1] Implement parseConstantExpression() with Value parsing in filter/parse.go
- [ ] T029 [US1] Implement parseColumnRefExpression() with binding resolution in filter/parse.go
- [ ] T030 [US1] Implement parseFunctionExpression() in filter/parse.go
- [ ] T031 [US1] Implement parseCastExpression() in filter/parse.go
- [ ] T032 [US1] Implement parseBetweenExpression() in filter/parse.go
- [ ] T033 [US1] Implement parseOperatorExpression() in filter/parse.go
- [ ] T034 [US1] Implement parseCaseExpression() in filter/parse.go
- [ ] T035 [US1] Implement parseLogicalType() with ExtraTypeInfo variants in filter/parse.go
- [ ] T036 [US1] Implement parseValue() with type-specific data extraction in filter/parse.go
- [ ] T037 [US1] Implement public Parse([]byte) (*FilterPushdown, error) function in filter/parse.go
- [ ] T038 [US1] Implement FilterPushdown.ColumnName(ref) helper method in filter/parse.go

### Unit Tests for US1

- [ ] T039 [P] [US1] Test parsing simple equality comparison in filter/parse_test.go
- [ ] T040 [P] [US1] Test parsing nested AND/OR conjunctions in filter/parse_test.go
- [ ] T041 [P] [US1] Test parsing column references with binding resolution in filter/parse_test.go
- [ ] T042 [P] [US1] Test parsing function expressions in filter/parse_test.go
- [ ] T043 [P] [US1] Test parsing CAST expressions in filter/parse_test.go
- [ ] T044 [P] [US1] Test parsing BETWEEN expressions in filter/parse_test.go
- [ ] T045 [P] [US1] Test parsing IS NULL/IS NOT NULL operators in filter/parse_test.go
- [ ] T046 [P] [US1] Test error handling for malformed JSON in filter/parse_test.go

**Checkpoint**: US1 complete - can parse filter JSON to Go types

---

## Phase 4: User Story 6 - All DuckDB Data Types Support (Priority: P1)

**Goal**: Support all DuckDB logical types in Value parsing and encoding

**Independent Test**: Create filters on columns of each supported type, verify parsing and encoding

**Note**: This is P1 and foundational for US2, so implemented before US2

### Value Type Handling for US6

- [ ] T047 [P] [US6] Implement boolean value parsing and encoding in filter/logical_types.go
- [ ] T048 [P] [US6] Implement signed integer value parsing (TINYINT-BIGINT) in filter/logical_types.go
- [ ] T049 [P] [US6] Implement unsigned integer value parsing (UTINYINT-UBIGINT) in filter/logical_types.go
- [ ] T050 [P] [US6] Implement HugeInt value parsing (upper/lower components) in filter/logical_types.go
- [ ] T051 [P] [US6] Implement float/double value parsing in filter/logical_types.go
- [ ] T052 [P] [US6] Implement decimal value parsing (string preservation) in filter/logical_types.go
- [ ] T053 [P] [US6] Implement string value parsing (VARCHAR, CHAR) in filter/logical_types.go
- [ ] T054 [P] [US6] Implement blob value parsing (base64 handling) in filter/logical_types.go
- [ ] T055 [P] [US6] Implement date value parsing (days since epoch) in filter/logical_types.go
- [ ] T056 [P] [US6] Implement time value parsing (microseconds) in filter/logical_types.go
- [ ] T057 [P] [US6] Implement timestamp value parsing (all variants) in filter/logical_types.go
- [ ] T058 [P] [US6] Implement interval value parsing (months, days, micros) in filter/logical_types.go
- [ ] T059 [P] [US6] Implement UUID value parsing in filter/logical_types.go
- [ ] T060 [P] [US6] Implement LIST/ARRAY value parsing in filter/logical_types.go
- [ ] T061 [P] [US6] Implement STRUCT value parsing in filter/logical_types.go
- [ ] T062 [P] [US6] Implement MAP value parsing in filter/logical_types.go

### Unit Tests for US6

- [ ] T063 [P] [US6] Test numeric type value parsing and SQL encoding in filter/logical_types_test.go
- [ ] T064 [P] [US6] Test string type value parsing and escaping in filter/logical_types_test.go
- [ ] T065 [P] [US6] Test temporal type value parsing and SQL literals in filter/logical_types_test.go
- [ ] T066 [P] [US6] Test special types (BOOLEAN, UUID, NULL) in filter/logical_types_test.go
- [ ] T067 [P] [US6] Test complex types (LIST, STRUCT, MAP) basic parsing in filter/logical_types_test.go

**Checkpoint**: US6 complete - all data types supported

---

## Phase 5: User Story 2 - Encode Filters to DuckDB SQL (Priority: P1)

**Goal**: Encode parsed filter expressions to valid DuckDB SQL syntax

**Independent Test**: Parse filter JSON, encode to SQL, validate output matches expected syntax

### Encoder Interface and Infrastructure for US2

- [ ] T068 [US2] Define Encoder interface with Encode() and EncodeFilters() in filter/encode.go
- [ ] T069 [US2] Define EncoderOptions struct with ColumnMapping and ColumnExpressions in filter/encode.go
- [ ] T070 [US2] Implement escapeString() and quoteLiteral() helpers in filter/encode.go
- [ ] T071 [US2] Implement quoteIdentifier() for column names in filter/encode.go

### DuckDB Encoder Implementation for US2

- [ ] T072 [US2] Create DuckDBEncoder struct implementing Encoder in filter/duckdb.go
- [ ] T073 [US2] Implement NewDuckDBEncoder(opts) constructor in filter/duckdb.go
- [ ] T074 [US2] Implement encodeComparison() with operator mapping in filter/duckdb.go
- [ ] T075 [US2] Implement encodeConjunction() with AND/OR and unsupported handling in filter/duckdb.go
- [ ] T076 [US2] Implement encodeConstant() with type-aware value formatting in filter/duckdb.go
- [ ] T077 [US2] Implement encodeColumnRef() with name resolution in filter/duckdb.go
- [ ] T078 [US2] Implement encodeFunction() with is_operator handling in filter/duckdb.go
- [ ] T079 [US2] Implement encodeCast() with CAST/TRY_CAST syntax in filter/duckdb.go
- [ ] T080 [US2] Implement encodeBetween() with inclusive bounds in filter/duckdb.go
- [ ] T081 [US2] Implement encodeOperator() for IS NULL, IS NOT NULL, NOT in filter/duckdb.go
- [ ] T082 [US2] Implement encodeCase() for CASE WHEN expressions in filter/duckdb.go
- [ ] T083 [US2] Implement encodeIN() for IN/NOT IN with value lists in filter/duckdb.go
- [ ] T084 [US2] Implement Encode() main dispatch method in filter/duckdb.go
- [ ] T085 [US2] Implement EncodeFilters() combining multiple filters with AND in filter/duckdb.go

### Value SQL Formatting for US2

- [ ] T086 [P] [US2] Implement formatBoolValue() returning TRUE/FALSE in filter/duckdb.go
- [ ] T087 [P] [US2] Implement formatIntValue() for numeric literals in filter/duckdb.go
- [ ] T088 [P] [US2] Implement formatHugeIntValue() computing decimal string in filter/duckdb.go
- [ ] T089 [P] [US2] Implement formatFloatValue() for float literals in filter/duckdb.go
- [ ] T090 [P] [US2] Implement formatStringValue() with quote escaping in filter/duckdb.go
- [ ] T091 [P] [US2] Implement formatDateValue() as DATE 'YYYY-MM-DD' in filter/duckdb.go
- [ ] T092 [P] [US2] Implement formatTimeValue() as TIME 'HH:MM:SS' in filter/duckdb.go
- [ ] T093 [P] [US2] Implement formatTimestampValue() as TIMESTAMP '...' in filter/duckdb.go
- [ ] T094 [P] [US2] Implement formatIntervalValue() as INTERVAL '...' in filter/duckdb.go
- [ ] T095 [P] [US2] Implement formatUUIDValue() as quoted string in filter/duckdb.go
- [ ] T096 [P] [US2] Implement formatBlobValue() as hex literal in filter/duckdb.go

### Unit Tests for US2

- [ ] T097 [P] [US2] Test encoding comparison operators (=, <>, <, >, <=, >=) in filter/duckdb_test.go
- [ ] T098 [P] [US2] Test encoding AND conjunctions in filter/duckdb_test.go
- [ ] T099 [P] [US2] Test encoding OR conjunctions in filter/duckdb_test.go
- [ ] T100 [P] [US2] Test encoding function expressions in filter/duckdb_test.go
- [ ] T101 [P] [US2] Test encoding BETWEEN expressions in filter/duckdb_test.go
- [ ] T102 [P] [US2] Test encoding IS NULL/IS NOT NULL in filter/duckdb_test.go
- [ ] T103 [P] [US2] Test encoding IN/NOT IN expressions in filter/duckdb_test.go
- [ ] T104 [P] [US2] Test encoding CAST expressions in filter/duckdb_test.go
- [ ] T105 [P] [US2] Test unsupported expression skipping in AND in filter/duckdb_test.go
- [ ] T106 [P] [US2] Test unsupported expression skipping entire OR in filter/duckdb_test.go
- [ ] T107 [P] [US2] Test all expressions unsupported returns empty string in filter/duckdb_test.go
- [ ] T108 [P] [US2] Test string value escaping (quotes, special chars) in filter/duckdb_test.go

**Checkpoint**: US2 complete - can encode filters to valid DuckDB SQL

---

## Phase 6: User Story 3 - Column Name Mapping (Priority: P2)

**Goal**: Map column names during encoding to translate to backend storage names

**Independent Test**: Provide mapping dictionary, verify encoded SQL uses mapped names

### Implementation for US3

- [ ] T109 [US3] Implement column name lookup with ColumnMapping in encodeColumnRef() in filter/duckdb.go
- [ ] T110 [US3] Handle unmapped columns (use original name) in filter/duckdb.go

### Unit Tests for US3

- [ ] T111 [P] [US3] Test column mapping single column in filter/duckdb_test.go
- [ ] T112 [P] [US3] Test column mapping multiple columns in filter/duckdb_test.go
- [ ] T113 [P] [US3] Test unmapped columns retain original name in filter/duckdb_test.go
- [ ] T114 [P] [US3] Test column mapping in complex nested expressions in filter/duckdb_test.go

**Checkpoint**: US3 complete - column name mapping works

---

## Phase 7: User Story 4 - Column Expression Replacement (Priority: P2)

**Goal**: Replace column names with SQL expressions during encoding

**Independent Test**: Provide expression mapping, verify encoded SQL substitutes expressions

### Implementation for US4

- [ ] T115 [US4] Implement column expression substitution (precedence over name mapping) in filter/duckdb.go
- [ ] T116 [US4] Handle expression substitution in nested contexts in filter/duckdb.go

### Unit Tests for US4

- [ ] T117 [P] [US4] Test expression replacement for computed column in filter/duckdb_test.go
- [ ] T118 [P] [US4] Test expression replacement takes precedence over name mapping in filter/duckdb_test.go
- [ ] T119 [P] [US4] Test expression replacement in nested expressions in filter/duckdb_test.go

**Checkpoint**: US4 complete - column expression replacement works

---

## Phase 8: User Story 5 - Custom SQL Dialect Support (Priority: P3)

**Goal**: Extensible interface for implementing custom SQL dialect encoders

**Independent Test**: Implement custom dialect encoder, verify dialect-specific output

### Implementation for US5

- [ ] T120 [US5] Document Encoder interface extension points in filter/encode.go
- [ ] T121 [US5] Add dialect customization hooks (function names, cast syntax, operators) to DuckDBEncoder in filter/duckdb.go

### Unit Tests for US5

- [ ] T122 [P] [US5] Test creating custom encoder implementing Encoder interface in filter/encode_test.go
- [ ] T123 [P] [US5] Test dialect-specific function name mapping in filter/encode_test.go

**Checkpoint**: US5 complete - custom dialect support available

---

## Phase 9: Integration Tests

**Purpose**: End-to-end testing with DuckDB client (FR-020 requires 20+ WHERE conditions)

- [ ] T124 Create integration test file in tests/integration/filter_pushdown_test.go
- [ ] T125 Implement test Flight server that captures filter and encodes to SQL in tests/integration/filter_pushdown_test.go
- [ ] T126 [P] Test simple equality: WHERE id = 42 in tests/integration/filter_pushdown_test.go
- [ ] T127 [P] Test inequality: WHERE id <> 0 in tests/integration/filter_pushdown_test.go
- [ ] T128 [P] Test less than: WHERE age < 30 in tests/integration/filter_pushdown_test.go
- [ ] T129 [P] Test greater than: WHERE price > 100 in tests/integration/filter_pushdown_test.go
- [ ] T130 [P] Test less than or equal: WHERE score <= 100 in tests/integration/filter_pushdown_test.go
- [ ] T131 [P] Test greater than or equal: WHERE rating >= 4.0 in tests/integration/filter_pushdown_test.go
- [ ] T132 [P] Test IN clause: WHERE status IN ('active', 'pending') in tests/integration/filter_pushdown_test.go
- [ ] T133 [P] Test NOT IN clause: WHERE type NOT IN (1, 2, 3) in tests/integration/filter_pushdown_test.go
- [ ] T134 [P] Test BETWEEN: WHERE created BETWEEN '2024-01-01' AND '2024-12-31' in tests/integration/filter_pushdown_test.go
- [ ] T135 [P] Test IS NULL: WHERE deleted_at IS NULL in tests/integration/filter_pushdown_test.go
- [ ] T136 [P] Test IS NOT NULL: WHERE email IS NOT NULL in tests/integration/filter_pushdown_test.go
- [ ] T137 [P] Test AND conjunction: WHERE a > 0 AND b < 100 in tests/integration/filter_pushdown_test.go
- [ ] T138 [P] Test OR conjunction: WHERE x = 1 OR y = 2 in tests/integration/filter_pushdown_test.go
- [ ] T139 [P] Test nested AND/OR: WHERE (a = 1 AND b = 2) OR (c = 3 AND d = 4) in tests/integration/filter_pushdown_test.go
- [ ] T140 [P] Test function LOWER: WHERE LOWER(name) = 'john' in tests/integration/filter_pushdown_test.go
- [ ] T141 [P] Test function LENGTH: WHERE LENGTH(code) = 5 in tests/integration/filter_pushdown_test.go
- [ ] T142 [P] Test function prefix: WHERE prefix(name, 'J') in tests/integration/filter_pushdown_test.go
- [ ] T143 [P] Test CAST expression: WHERE CAST(value AS INTEGER) > 10 in tests/integration/filter_pushdown_test.go
- [ ] T144 [P] Test DATE type: WHERE birth_date = DATE '1990-05-15' in tests/integration/filter_pushdown_test.go
- [ ] T145 [P] Test TIMESTAMP type: WHERE created_at > TIMESTAMP '2024-01-01 00:00:00' in tests/integration/filter_pushdown_test.go
- [ ] T146 [P] Test UUID type: WHERE user_id = '550e8400-e29b-41d4-a716-446655440000' in tests/integration/filter_pushdown_test.go
- [ ] T147 [P] Test BOOLEAN type: WHERE is_active = TRUE in tests/integration/filter_pushdown_test.go
- [ ] T148 [P] Test string escaping: WHERE name = 'O''Brien' in tests/integration/filter_pushdown_test.go

**Checkpoint**: Integration tests complete - 20+ WHERE conditions verified

---

## Phase 10: Examples and Documentation

**Purpose**: Example code and documentation (FR-019)

- [ ] T149 Create examples/filter directory in examples/filter/
- [ ] T150 [P] Implement basic parse and encode example in examples/filter/main.go
- [ ] T151 [P] Add column mapping example in examples/filter/main.go
- [ ] T152 [P] Add expression replacement example in examples/filter/main.go
- [ ] T153 Add go.mod for examples module if needed in examples/go.mod
- [ ] T154 Update examples README with filter example in examples/README.md

**Checkpoint**: Examples complete - demonstrates all major features

---

## Phase 11: Polish & Cross-Cutting Concerns

**Purpose**: Final cleanup and validation

- [ ] T155 Run gofmt on all filter/ files
- [ ] T156 Run golangci-lint on filter/ package
- [ ] T157 Verify all public types/functions have godoc comments in filter/
- [ ] T158 Run tests with race detector: go test -race ./filter/...
- [ ] T159 Run integration tests with race detector in tests/integration/
- [ ] T160 Validate quickstart.md examples compile and run
- [ ] T161 Performance verification: ensure <1ms parsing for typical filters

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - start immediately
- **Foundational (Phase 2)**: Depends on Setup - BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational - parsing core
- **US6 (Phase 4)**: Depends on US1 - data types (supports US2)
- **US2 (Phase 5)**: Depends on US1 + US6 - encoding core
- **US3 (Phase 6)**: Depends on US2 - column mapping
- **US4 (Phase 7)**: Depends on US2 - expression replacement
- **US5 (Phase 8)**: Depends on US2 - custom dialects
- **Integration (Phase 9)**: Depends on US1 + US2 + US6
- **Examples (Phase 10)**: Depends on US1 + US2 + US3 + US4
- **Polish (Phase 11)**: Depends on all user stories

### User Story Dependencies

```
US1 (Parse) ─────────────────┐
                             │
US6 (Data Types) ←───────────┤
                             │
US2 (Encode) ←───────────────┼──→ US3 (Column Mapping)
                             │
                             ├──→ US4 (Expression Replace)
                             │
                             └──→ US5 (Custom Dialects)
```

### Parallel Opportunities

**Within Phase 1 (Setup)**:
- T002, T003, T004, T005 can run in parallel

**Within Phase 3 (US1)**:
- T015-T023 expression types can run in parallel
- T039-T046 unit tests can run in parallel

**Within Phase 4 (US6)**:
- T047-T062 value type implementations can run in parallel
- T063-T067 unit tests can run in parallel

**Within Phase 5 (US2)**:
- T086-T096 value formatters can run in parallel
- T097-T108 unit tests can run in parallel

**Within Phase 9 (Integration)**:
- T126-T148 test cases can run in parallel

---

## Parallel Example: User Story 1 Types

```bash
# Launch all expression type definitions together:
Task: "Define ComparisonExpression struct in filter/types.go"
Task: "Define ConjunctionExpression struct in filter/types.go"
Task: "Define ConstantExpression struct in filter/types.go"
Task: "Define ColumnRefExpression struct in filter/types.go"
Task: "Define FunctionExpression struct in filter/types.go"
Task: "Define CastExpression struct in filter/types.go"
Task: "Define BetweenExpression struct in filter/types.go"
Task: "Define OperatorExpression struct in filter/types.go"
Task: "Define CaseExpression struct in filter/types.go"
```

---

## Implementation Strategy

### MVP First (US1 + US6 + US2 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational types
3. Complete Phase 3: US1 - Parse JSON
4. Complete Phase 4: US6 - Data types
5. Complete Phase 5: US2 - Encode to SQL
6. **STOP and VALIDATE**: Can parse and encode basic filters
7. Run integration tests subset

### Incremental Delivery

1. Setup + Foundational → Core types ready
2. US1 → Can parse filter JSON → Test independently
3. US6 → All data types work → Test independently
4. US2 → Can encode to SQL → Test end-to-end (MVP!)
5. US3 → Column mapping → Enhance MVP
6. US4 → Expression replacement → Enhance MVP
7. US5 → Custom dialects → Full feature set
8. Integration tests + Examples → Production ready

### Suggested MVP Scope

**MVP = US1 + US6 + US2**: Parse filter JSON, support all data types, encode to DuckDB SQL

This delivers the core value proposition:
- Server developers can receive filter JSON
- Parse into strongly-typed Go structures
- Encode to valid DuckDB SQL for backend queries

Column mapping (US3/US4) and custom dialects (US5) are valuable enhancements but not required for initial use.

---

## Notes

- [P] tasks = different files or independent work, no dependencies
- [Story] label maps task to specific user story
- Each user story should be independently testable after completion
- Tests explicitly required per FR-020 and constitution
- Commit after each task or logical group
- Stop at any checkpoint to validate progress
