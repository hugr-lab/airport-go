package airport_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	airport "github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/filter"
)

// =============================================================================
// Filter Pushdown Tests
// =============================================================================
// These tests verify that filter pushdown JSON is properly received by tables
// and can be parsed and encoded to SQL using the filter package.
//
// FR-020 requires 20+ different WHERE conditions covering all supported
// expression types and data types.

// TestFilterPushdownSimpleComparisons tests basic comparison operators.
func TestFilterPushdownSimpleComparisons(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string // substrings that must appear in encoded SQL
	}{
		// 1. Simple equality
		{
			name:     "Equality",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id = 42",
			contains: []string{"id", "=", "42"},
		},
		// 2. Inequality
		{
			name:     "Inequality",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id <> 0",
			contains: []string{"id", "<>", "0"},
		},
		// 3. Less than
		{
			name:     "LessThan",
			query:    "SELECT * FROM %s.filter_schema.data WHERE age < 30",
			contains: []string{"age", "<", "30"},
		},
		// 4. Greater than
		{
			name:     "GreaterThan",
			query:    "SELECT * FROM %s.filter_schema.data WHERE price > 100",
			contains: []string{"price", ">", "100"},
		},
		// 5. Less than or equal
		{
			name:     "LessThanOrEqual",
			query:    "SELECT * FROM %s.filter_schema.data WHERE score <= 100",
			contains: []string{"score", "<=", "100"},
		},
		// 6. Greater than or equal
		{
			name:     "GreaterThanOrEqual",
			query:    "SELECT * FROM %s.filter_schema.data WHERE rating >= 4",
			contains: []string{"rating", ">=", "4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			// Verify filter was received
			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			// Parse and encode the filter
			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			// Verify expected patterns appear in SQL
			for _, expected := range tt.contains {
				if !strings.Contains(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownConjunctions tests AND/OR conjunctions.
func TestFilterPushdownConjunctions(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 7. AND conjunction
		{
			name:     "AndConjunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id > 0 AND age < 100",
			contains: []string{"id", ">", "0", "AND", "age", "<", "100"},
		},
		// 8. OR conjunction
		{
			name:     "OrConjunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id = 1 OR id = 2",
			contains: []string{"id", "=", "1", "OR", "id", "=", "2"},
		},
		// 9. Nested AND/OR: (A AND B) OR (C AND D)
		{
			name:     "NestedAndOr",
			query:    "SELECT * FROM %s.filter_schema.data WHERE (id = 1 AND age > 18) OR (id = 2 AND age > 21)",
			contains: []string{"AND", "OR"},
		},
		// 10. Multiple ANDs
		{
			name:     "MultipleAnds",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id > 0 AND age > 18 AND is_active = TRUE",
			contains: []string{"AND"},
		},
		// 11. Nested OR in AND: A AND (B OR C)
		{
			name:     "NestedOrInAnd",
			query:    "SELECT * FROM %s.filter_schema.data WHERE is_active = TRUE AND (status = 'active' OR status = 'pending')",
			contains: []string{"AND", "OR"},
		},
		// 12. Deep nesting: ((A AND B) OR C) AND D
		{
			name:     "DeepNesting",
			query:    "SELECT * FROM %s.filter_schema.data WHERE ((id = 1 AND age > 18) OR id = 99) AND is_active = TRUE",
			contains: []string{"AND", "OR"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !strings.Contains(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownInBetween tests IN, NOT IN, BETWEEN, and NOT BETWEEN expressions.
func TestFilterPushdownInBetween(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 13. IN clause with strings
		{
			name:     "InClauseStrings",
			query:    "SELECT * FROM %s.filter_schema.data WHERE status IN ('active', 'pending')",
			contains: []string{"status", "IN", "active", "pending"},
		},
		// 14. IN clause with integers
		{
			name:     "InClauseIntegers",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id IN (1, 2, 3)",
			contains: []string{"id", "IN", "1", "2", "3"},
		},
		// 15. NOT IN clause
		{
			name:     "NotInClause",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id NOT IN (1, 2, 3)",
			contains: []string{"id", "NOT IN", "1", "2", "3"},
		},
		// 16. BETWEEN
		{
			name:     "Between",
			query:    "SELECT * FROM %s.filter_schema.data WHERE age BETWEEN 18 AND 65",
			contains: []string{"age", "BETWEEN", "18", "AND", "65"},
		},
		// 17. NOT BETWEEN (DuckDB encodes as NOT (col >= X AND col <= Y))
		{
			name:     "NotBetween",
			query:    "SELECT * FROM %s.filter_schema.data WHERE age NOT BETWEEN 0 AND 17",
			contains: []string{"age", "NOT", "0", "17"},
		},
		// 18. BETWEEN with floats
		{
			name:     "BetweenFloats",
			query:    "SELECT * FROM %s.filter_schema.data WHERE price BETWEEN 10.0 AND 100.0",
			contains: []string{"price", "BETWEEN", "10", "AND", "100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !strings.Contains(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownNullChecks tests IS NULL and IS NOT NULL.
func TestFilterPushdownNullChecks(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 19. IS NULL
		{
			name:     "IsNull",
			query:    "SELECT * FROM %s.filter_schema.data WHERE deleted_at IS NULL",
			contains: []string{"deleted_at", "IS NULL"},
		},
		// 20. IS NOT NULL
		{
			name:     "IsNotNull",
			query:    "SELECT * FROM %s.filter_schema.data WHERE email IS NOT NULL",
			contains: []string{"email", "IS NOT NULL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !strings.Contains(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownStringOperations tests LIKE, ILIKE, regex, and other string functions.
func TestFilterPushdownStringOperations(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 21. LIKE pattern (DuckDB may optimize LIKE 'x%' to prefix(col, 'x'))
		{
			name:     "Like",
			query:    "SELECT * FROM %s.filter_schema.data WHERE name LIKE 'test%%'",
			contains: []string{"name", "test"}, // DuckDB optimizes to prefix(name, 'test')
		},
		// 22. NOT LIKE pattern (DuckDB may optimize to NOT (prefix(...)))
		{
			name:     "NotLike",
			query:    "SELECT * FROM %s.filter_schema.data WHERE name NOT LIKE 'test%%'",
			contains: []string{"name", "NOT", "test"}, // DuckDB optimizes to NOT (prefix(name, 'test'))
		},
		// 23. ILIKE (case-insensitive)
		{
			name:     "ILike",
			query:    "SELECT * FROM %s.filter_schema.data WHERE name ILIKE 'JOHN%%'",
			contains: []string{"name", "ILIKE", "JOHN"},
		},
		// 24. regexp_matches function (DuckDB's way of regex matching)
		{
			name:     "RegexpMatches",
			query:    "SELECT * FROM %s.filter_schema.data WHERE regexp_matches(name, '^J.*')",
			contains: []string{"regexp_matches", "name"},
		},
		// 27. LOWER function
		{
			name:     "LowerFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE LOWER(name) = 'john'",
			contains: []string{"lower", "name", "john"},
		},
		// 28. UPPER function
		{
			name:     "UpperFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE UPPER(name) = 'JOHN'",
			contains: []string{"upper", "name", "JOHN"},
		},
		// 29. LENGTH function
		{
			name:     "LengthFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE LENGTH(name) > 5",
			contains: []string{"length", "name", ">", "5"},
		},
		// 30. SUBSTRING function
		{
			name:     "SubstringFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE SUBSTRING(name, 1, 1) = 'J'",
			contains: []string{"substring", "name"},
		},
		// 31. TRIM function
		{
			name:     "TrimFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE TRIM(name) = 'John'",
			contains: []string{"trim", "name", "John"},
		},
		// 32. CONCAT function (DuckDB may optimize LIKE '%x%' to contains())
		{
			name:     "ConcatFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CONCAT(name, email) LIKE '%%example%%'",
			contains: []string{"concat", "name", "email", "example"},
		},
		// 33. prefix function (starts_with)
		{
			name:     "PrefixFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE prefix(name, 'J')",
			contains: []string{"prefix", "name"},
		},
		// 34. suffix function (ends_with)
		{
			name:     "SuffixFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE suffix(email, '.com')",
			contains: []string{"suffix", "email"},
		},
		// 35. contains function
		{
			name:     "ContainsFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE contains(name, 'oh')",
			contains: []string{"contains", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q (case-insensitive), got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownMathOperations tests math functions and operators.
func TestFilterPushdownMathOperations(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 36. Modulo operator
		{
			name:     "ModuloOperator",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id %% 5 = 0",
			contains: []string{"id", "%", "5", "=", "0"},
		},
		// 37. Addition (DuckDB optimizes: id + 10 > 50 becomes id > 40)
		{
			name:     "Addition",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id + age > 100",
			contains: []string{"id", "+", "age", ">", "100"},
		},
		// 38. Subtraction (DuckDB optimizes: col - const >= 0 becomes col >= const)
		{
			name:     "Subtraction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE age - id >= 10",
			contains: []string{"age", "-", "id", ">=", "10"},
		},
		// 39. Multiplication
		{
			name:     "Multiplication",
			query:    "SELECT * FROM %s.filter_schema.data WHERE price * 2 < 1000",
			contains: []string{"price", "*", "2", "<", "1000"},
		},
		// 40. Division
		{
			name:     "Division",
			query:    "SELECT * FROM %s.filter_schema.data WHERE price / 10 > 5",
			contains: []string{"price", "/", "10", ">", "5"},
		},
		// 41. ABS function
		{
			name:     "AbsFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE ABS(score - 50) < 10",
			contains: []string{"abs", "score", "<", "10"},
		},
		// 42. ROUND function
		{
			name:     "RoundFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE ROUND(rating) = 4",
			contains: []string{"round", "rating", "=", "4"},
		},
		// 43. FLOOR function
		{
			name:     "FloorFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE FLOOR(rating) >= 3",
			contains: []string{"floor", "rating", ">=", "3"},
		},
		// 44. CEIL function
		{
			name:     "CeilFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CEIL(rating) <= 5",
			contains: []string{"ceil", "rating", "<=", "5"},
		},
		// 45. Negation
		{
			name:     "Negation",
			query:    "SELECT * FROM %s.filter_schema.data WHERE -score < 0",
			contains: []string{"-", "score", "<", "0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownTemporalOperations tests date, time, timestamp filters and functions.
func TestFilterPushdownTemporalOperations(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 46. Date comparison
		{
			name:     "DateComparison",
			query:    "SELECT * FROM %s.filter_schema.data WHERE birth_date > DATE '1990-01-01'",
			contains: []string{"birth_date", ">"},
		},
		// 47. Date BETWEEN
		{
			name:     "DateBetween",
			query:    "SELECT * FROM %s.filter_schema.data WHERE birth_date BETWEEN DATE '1980-01-01' AND DATE '2000-12-31'",
			contains: []string{"birth_date", "BETWEEN"},
		},
		// 48. Timestamp comparison
		{
			name:     "TimestampComparison",
			query:    "SELECT * FROM %s.filter_schema.data WHERE created_at > TIMESTAMP '2024-01-01 00:00:00'",
			contains: []string{"created_at", ">"},
		},
		// 49. Time comparison
		{
			name:     "TimeComparison",
			query:    "SELECT * FROM %s.filter_schema.data WHERE event_time >= TIME '09:00:00'",
			contains: []string{"event_time", ">="},
		},
		// 50. EXTRACT year (DuckDB converts EXTRACT(YEAR FROM col) to year(col))
		{
			name:     "ExtractYear",
			query:    "SELECT * FROM %s.filter_schema.data WHERE EXTRACT(YEAR FROM birth_date) = 1990",
			contains: []string{"year", "birth_date", "1990"},
		},
		// 51. EXTRACT month (DuckDB converts EXTRACT(MONTH FROM col) to month(col))
		{
			name:     "ExtractMonth",
			query:    "SELECT * FROM %s.filter_schema.data WHERE EXTRACT(MONTH FROM created_at) = 12",
			contains: []string{"month", "created_at", "12"},
		},
		// 52. EXTRACT day (DuckDB converts EXTRACT(DAY FROM col) to day(col))
		{
			name:     "ExtractDay",
			query:    "SELECT * FROM %s.filter_schema.data WHERE EXTRACT(DAY FROM birth_date) = 15",
			contains: []string{"day", "birth_date", "15"},
		},
		// 53. DATE_PART function (DuckDB converts DATE_PART('hour', col) to hour(col))
		{
			name:     "DatePartFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE DATE_PART('hour', created_at) >= 9",
			contains: []string{"hour", "created_at", "9"},
		},
		// 54. DATE_TRUNC function (DuckDB may convert to range comparison)
		{
			name:     "DateTruncFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE DATE_TRUNC('month', created_at) = TIMESTAMP '2024-01-01'",
			contains: []string{"created_at"},
		},
		// 55. AGE function (interval comparison)
		{
			name:     "AgeFunction",
			query:    "SELECT * FROM %s.filter_schema.data WHERE AGE(created_at) < INTERVAL '1 year'",
			contains: []string{"age", "created_at", "<"},
		},
		// 56. Timestamp with interval arithmetic
		{
			name:     "TimestampIntervalArithmetic",
			query:    "SELECT * FROM %s.filter_schema.data WHERE created_at > NOW() - INTERVAL '30 days'",
			contains: []string{"created_at", ">"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q (case-insensitive), got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownCast tests CAST and TRY_CAST expressions.
func TestFilterPushdownCast(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 57. CAST to VARCHAR
		{
			name:     "CastToVarchar",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CAST(id AS VARCHAR) = '42'",
			contains: []string{"CAST", "id", "VARCHAR", "42"},
		},
		// 58. CAST to INTEGER (DuckDB optimizes away redundant casts)
		{
			name:     "CastToInteger",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CAST(rating AS INTEGER) > 3",
			contains: []string{"CAST", "rating", "INTEGER", ">", "3"},
		},
		// 59. CAST to DATE
		{
			name:     "CastToDate",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CAST(created_at AS DATE) = DATE '2024-01-01'",
			contains: []string{"CAST", "created_at", "DATE"},
		},
		// 60. TRY_CAST (returns NULL on failure)
		{
			name:     "TryCast",
			query:    "SELECT * FROM %s.filter_schema.data WHERE TRY_CAST(name AS INTEGER) IS NULL",
			contains: []string{"TRY_CAST", "name", "INTEGER", "IS NULL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownDataTypes tests filters on various data types.
func TestFilterPushdownDataTypes(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 61. BOOLEAN type
		{
			name:     "BooleanType",
			query:    "SELECT * FROM %s.filter_schema.data WHERE is_active = TRUE",
			contains: []string{"is_active", "=", "TRUE"},
		},
		// 62. BOOLEAN false
		{
			name:     "BooleanFalse",
			query:    "SELECT * FROM %s.filter_schema.data WHERE is_active = FALSE",
			contains: []string{"is_active", "=", "FALSE"},
		},
		// 63. String with escape (single quote - SQL escapes as '')
		{
			name:     "StringEscaping",
			query:    "SELECT * FROM %s.filter_schema.data WHERE name = 'O''Brien'",
			contains: []string{"name", "=", "O''Brien"},
		},
		// 64. UUID type comparison (using string comparison since uuid_col is VARCHAR)
		{
			name:     "UUIDType",
			query:    "SELECT * FROM %s.filter_schema.data WHERE uuid_col = '550e8400-e29b-41d4-a716-446655440000'",
			contains: []string{"uuid_col", "=", "550e8400-e29b-41d4-a716-446655440000"},
		},
		// 65. DECIMAL type
		{
			name:     "DecimalType",
			query:    "SELECT * FROM %s.filter_schema.data WHERE decimal_col = 123.45::DECIMAL(10,2)",
			contains: []string{"decimal_col", "="},
		},
		// 66. BIGINT type
		{
			name:     "BigIntType",
			query:    "SELECT * FROM %s.filter_schema.data WHERE id = 9223372036854775807::BIGINT",
			contains: []string{"id", "=", "9223372036854775807"},
		},
		// 67. Double/Float type
		{
			name:     "DoubleType",
			query:    "SELECT * FROM %s.filter_schema.data WHERE price = 99.99",
			contains: []string{"price", "=", "99.99"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownNestedTypes tests filters on struct, list/array, and map types.
func TestFilterPushdownNestedTypes(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 68. Struct field access
		{
			name:     "StructFieldAccess",
			query:    "SELECT * FROM %s.filter_schema.data WHERE metadata.version = 1",
			contains: []string{"metadata", "version", "=", "1"},
		},
		// 69. Nested struct field
		{
			name:     "NestedStructField",
			query:    "SELECT * FROM %s.filter_schema.data WHERE metadata.author.name = 'John'",
			contains: []string{"metadata", "author", "name", "=", "John"},
		},
		// 70. List element access
		{
			name:     "ListElementAccess",
			query:    "SELECT * FROM %s.filter_schema.data WHERE tags[1] = 'important'",
			contains: []string{"tags", "important"},
		},
		// 71. List length
		{
			name:     "ListLength",
			query:    "SELECT * FROM %s.filter_schema.data WHERE len(tags) > 0",
			contains: []string{"len", "tags", ">", "0"},
		},
		// 72. List contains (array_contains)
		{
			name:     "ListContains",
			query:    "SELECT * FROM %s.filter_schema.data WHERE array_contains(tags, 'featured')",
			contains: []string{"array_contains", "tags", "featured"},
		},
		// 73. Map key access
		{
			name:     "MapKeyAccess",
			query:    "SELECT * FROM %s.filter_schema.data WHERE properties['color'] = 'red'",
			contains: []string{"properties", "color", "red"},
		},
		// 74. Map cardinality function (element_at returns array for map type)
		{
			name:     "MapCardinality",
			query:    "SELECT * FROM %s.filter_schema.data WHERE cardinality(properties) > 0",
			contains: []string{"cardinality", "properties", ">", "0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownCaseExpressions tests CASE WHEN expressions.
func TestFilterPushdownCaseExpressions(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 75. Simple CASE with boolean condition
		{
			name:     "SimpleCaseWhen",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN is_active THEN 1 ELSE 0 END = 1",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END"},
		},
		// 76. CASE with multiple WHEN conditions
		{
			name:     "CaseMultipleConditions",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END = 'adult'",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END", "adult"},
		},
		// 77. CASE without ELSE (implicit NULL)
		{
			name:     "CaseWithoutElse",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN age > 21 THEN 'adult' END IS NOT NULL",
			contains: []string{"CASE", "WHEN", "THEN", "END", "IS NOT NULL"},
		},
		// 78. CASE with AND in WHEN condition
		{
			name:     "CaseWithAndCondition",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN age >= 18 AND age < 65 THEN 'working_age' ELSE 'other' END = 'working_age'",
			contains: []string{"CASE", "WHEN", "AND", "THEN", "ELSE", "END"},
		},
		// 79. CASE with OR in WHEN condition
		{
			name:     "CaseWithOrCondition",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN status = 'active' OR status = 'pending' THEN 1 ELSE 0 END = 1",
			contains: []string{"CASE", "WHEN", "OR", "THEN", "ELSE", "END"},
		},
		// 80. CASE with NULL check in WHEN
		{
			name:     "CaseWithNullCheck",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN email IS NULL THEN 'no_email' ELSE 'has_email' END = 'has_email'",
			contains: []string{"CASE", "WHEN", "IS NULL", "THEN", "ELSE", "END"},
		},
		// 81. CASE with arithmetic in THEN
		{
			name:     "CaseWithArithmeticThen",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN is_active THEN price * 0.9 ELSE price END < 100",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END", "price"},
		},
		// 82. CASE with comparison operators in WHEN
		{
			name:     "CaseWithComparisonInWhen",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END IN ('A', 'B')",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END"},
		},
		// 83. CASE with function in WHEN
		{
			name:     "CaseWithFunctionInWhen",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN LENGTH(name) > 10 THEN 'long' ELSE 'short' END = 'long'",
			contains: []string{"CASE", "WHEN", "length", "THEN", "ELSE", "END"},
		},
		// 84. CASE returning numeric values
		{
			name:     "CaseReturningNumeric",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN is_active THEN score ELSE 0 END > 50",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END", "score"},
		},
		// 85. Nested CASE expressions
		{
			name:     "NestedCase",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN age < 18 THEN 'minor' ELSE CASE WHEN age < 65 THEN 'adult' ELSE 'senior' END END = 'adult'",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END", "adult"},
		},
		// 86. CASE with LIKE pattern in WHEN
		{
			name:     "CaseWithLikeInWhen",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN email LIKE '%%@gmail.com' THEN 'gmail' ELSE 'other' END = 'gmail'",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END"},
		},
		// 87. CASE with BETWEEN in WHEN
		{
			name:     "CaseWithBetweenInWhen",
			query:    "SELECT * FROM %s.filter_schema.data WHERE CASE WHEN price BETWEEN 10 AND 100 THEN 'affordable' ELSE 'expensive' END = 'affordable'",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END"},
		},
		// 88. CASE used in conjunction with AND
		{
			name:     "CaseInAndExpression",
			query:    "SELECT * FROM %s.filter_schema.data WHERE is_active = TRUE AND CASE WHEN age >= 18 THEN TRUE ELSE FALSE END",
			contains: []string{"CASE", "WHEN", "THEN", "ELSE", "END", "AND"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownCoalesceNullif tests COALESCE and NULLIF expressions.
func TestFilterPushdownCoalesceNullif(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		// 77. COALESCE
		{
			name:     "Coalesce",
			query:    "SELECT * FROM %s.filter_schema.data WHERE COALESCE(email, 'unknown') <> 'unknown'",
			contains: []string{"COALESCE", "email", "unknown"},
		},
		// 78. NULLIF (DuckDB may convert to CASE WHEN col = val THEN NULL ELSE col END)
		{
			name:     "Nullif",
			query:    "SELECT * FROM %s.filter_schema.data WHERE NULLIF(status, 'deleted') IS NOT NULL",
			contains: []string{"status", "deleted", "IS NOT NULL"}, // DuckDB converts NULLIF to CASE WHEN
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table.Reset()

			query := fmt.Sprintf(tt.query, attachName)
			rows, err := db.Query(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			rows.Close()

			capturedFilter := table.GetCapturedFilter()
			if len(capturedFilter) == 0 {
				t.Fatalf("No filter was captured")
			}

			fp, err := filter.Parse(capturedFilter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := filter.NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			t.Logf("Filter SQL: %s", sql)

			if sql == "" {
				t.Errorf("Expected non-empty SQL from filter")
			}

			for _, expected := range tt.contains {
				if !containsIgnoreCase(sql, expected) {
					t.Errorf("Expected SQL to contain %q, got: %s", expected, sql)
				}
			}
		})
	}
}

// TestFilterPushdownColumnMapping tests column name mapping during encoding.
func TestFilterPushdownColumnMapping(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("ColumnMapping", func(t *testing.T) {
		table.Reset()

		query := fmt.Sprintf("SELECT * FROM %s.filter_schema.data WHERE id = 42", attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		rows.Close()

		capturedFilter := table.GetCapturedFilter()
		if len(capturedFilter) == 0 {
			t.Fatalf("No filter was captured")
		}

		fp, err := filter.Parse(capturedFilter)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Encode with column mapping
		enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
			ColumnMapping: map[string]string{
				"id": "user_id",
			},
		})
		sql := enc.EncodeFilters(fp)

		t.Logf("Filter SQL with mapping: %s", sql)

		// Verify mapped column name is used
		if !strings.Contains(sql, "user_id") {
			t.Errorf("Expected SQL to contain mapped column 'user_id', got: %s", sql)
		}
		if strings.Contains(sql, `"id"`) {
			t.Errorf("Expected SQL NOT to contain original column 'id', got: %s", sql)
		}
	})
}

// TestFilterPushdownExpressionMapping tests column expression replacement.
func TestFilterPushdownExpressionMapping(t *testing.T) {
	table := newFilterTestTable()
	cat := filterTestCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("ExpressionMapping", func(t *testing.T) {
		table.Reset()

		query := fmt.Sprintf("SELECT * FROM %s.filter_schema.data WHERE name = 'John Doe'", attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		rows.Close()

		capturedFilter := table.GetCapturedFilter()
		if len(capturedFilter) == 0 {
			t.Fatalf("No filter was captured")
		}

		fp, err := filter.Parse(capturedFilter)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Encode with expression mapping
		enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
			ColumnExpressions: map[string]string{
				"name": "CONCAT(first_name, ' ', last_name)",
			},
		})
		sql := enc.EncodeFilters(fp)

		t.Logf("Filter SQL with expression: %s", sql)

		// Verify expression replacement
		if !strings.Contains(sql, "CONCAT(first_name, ' ', last_name)") {
			t.Errorf("Expected SQL to contain expression, got: %s", sql)
		}
	})
}

// =============================================================================
// Test Infrastructure
// =============================================================================

// filterTestTable captures filter pushdown JSON for testing.
type filterTestTable struct {
	schema *arrow.Schema

	mu             sync.Mutex
	capturedFilter []byte
	scanCount      int
}

func newFilterTestTable() *filterTestTable {
	// Create a comprehensive schema with various data types for testing
	// Including nested types: struct, list, map

	// Define nested struct type for metadata
	metadataType := arrow.StructOf(
		arrow.Field{Name: "version", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "author", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		), Nullable: true},
	)

	// Define list type for tags
	tagsType := arrow.ListOf(arrow.BinaryTypes.String)

	// Define map type for properties
	propertiesType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)

	schema := arrow.NewSchema([]arrow.Field{
		// Primitive types
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "rating", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},

		// Temporal types
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "deleted_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "birth_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "event_time", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},

		// UUID and decimal
		{Name: "uuid_col", Type: arrow.BinaryTypes.String, Nullable: true}, // UUID as string
		{Name: "decimal_col", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},

		// Nested types
		{Name: "metadata", Type: metadataType, Nullable: true},
		{Name: "tags", Type: tagsType, Nullable: true},
		{Name: "properties", Type: propertiesType, Nullable: true},
	}, nil)

	return &filterTestTable{schema: schema}
}

func (t *filterTestTable) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.capturedFilter = nil
	t.scanCount = 0
}

func (t *filterTestTable) GetCapturedFilter() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.capturedFilter
}

func (t *filterTestTable) GetScanCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.scanCount
}

func (t *filterTestTable) Name() string    { return "data" }
func (t *filterTestTable) Comment() string { return "Test table for filter pushdown" }
func (t *filterTestTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *filterTestTable) Scan(_ context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.Lock()
	t.scanCount++

	// Capture the filter JSON for test verification
	if opts != nil && len(opts.Filter) > 0 {
		t.capturedFilter = make([]byte, len(opts.Filter))
		copy(t.capturedFilter, opts.Filter)
	}
	t.mu.Unlock()

	// Return a record with dummy data
	return t.buildDummyRecord()
}

func (t *filterTestTable) buildDummyRecord() (array.RecordReader, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
	defer builder.Release()

	// Build one row of dummy data for each field
	// Primitive types
	builder.Field(0).(*array.Int64Builder).Append(1)                                           // id
	builder.Field(1).(*array.StringBuilder).Append("Test")                                     // name
	builder.Field(2).(*array.StringBuilder).Append("test@example.com")                         // email
	builder.Field(3).(*array.Int64Builder).Append(25)                                          // age
	builder.Field(4).(*array.Float64Builder).Append(99.99)                                     // price
	builder.Field(5).(*array.Int64Builder).Append(100)                                         // score
	builder.Field(6).(*array.Float64Builder).Append(4.5)                                       // rating
	builder.Field(7).(*array.StringBuilder).Append("active")                                   // status
	builder.Field(8).(*array.BooleanBuilder).Append(true)                                      // is_active
	builder.Field(9).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro())) // created_at
	builder.Field(10).(*array.TimestampBuilder).AppendNull()                                   // deleted_at
	builder.Field(11).(*array.Date32Builder).Append(arrow.Date32(19000))                       // birth_date (days since epoch)
	builder.Field(12).(*array.Time64Builder).Append(arrow.Time64(36000000000))                 // event_time (10:00:00 in microseconds)
	builder.Field(13).(*array.StringBuilder).Append("550e8400-e29b-41d4-a716-446655440000")    // uuid_col
	builder.Field(14).(*array.Decimal128Builder).Append(decimal128.FromI64(12345)) // decimal_col

	// Nested struct: metadata
	metadataBuilder := builder.Field(15).(*array.StructBuilder)
	metadataBuilder.Append(true)
	metadataBuilder.FieldBuilder(0).(*array.Int32Builder).Append(1) // version
	authorBuilder := metadataBuilder.FieldBuilder(1).(*array.StructBuilder)
	authorBuilder.Append(true)
	authorBuilder.FieldBuilder(0).(*array.StringBuilder).Append("John")            // author.name
	authorBuilder.FieldBuilder(1).(*array.StringBuilder).Append("john@example.com") // author.email

	// List: tags
	tagsBuilder := builder.Field(16).(*array.ListBuilder)
	tagsBuilder.Append(true)
	tagsBuilder.ValueBuilder().(*array.StringBuilder).Append("tag1")
	tagsBuilder.ValueBuilder().(*array.StringBuilder).Append("tag2")

	// Map: properties
	propsBuilder := builder.Field(17).(*array.MapBuilder)
	propsBuilder.Append(true)
	propsBuilder.KeyBuilder().(*array.StringBuilder).Append("color")
	propsBuilder.ItemBuilder().(*array.StringBuilder).Append("red")

	record := builder.NewRecordBatch()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

func filterTestCatalog(t *testing.T, table *filterTestTable) catalog.Catalog {
	t.Helper()
	cat, err := airport.NewCatalogBuilder().
		Schema("filter_schema").
		Table(table).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}
	return cat
}

// containsIgnoreCase checks if s contains substr (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
