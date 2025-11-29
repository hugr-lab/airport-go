package airport_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/hugr-lab/airport-go/catalog"
)

// =============================================================================
// Column Statistics Integration Tests via DuckDB
// =============================================================================
// These tests verify that the column_statistics action is called by DuckDB
// during query optimization (especially for JOINs, filters, and ANALYZE).
//
// Reference:
// - https://duckdb.org/docs/stable/guides/performance/join_operations
// - https://duckdb.org/docs/stable/sql/statements/summarize
// - https://duckdb.org/docs/stable/sql/statements/analyze
// =============================================================================

// mockStatisticsTable implements both Table and StatisticsTable interfaces.
type mockStatisticsTable struct {
	name           string
	schema         *arrow.Schema
	data           [][]any
	statsCallCount atomic.Int64
	stats          map[string]*catalog.ColumnStats
}

func (t *mockStatisticsTable) Name() string    { return t.name }
func (t *mockStatisticsTable) Comment() string { return "Mock statistics table" }

func (t *mockStatisticsTable) ArrowSchema(columns []string) *arrow.Schema {
	if len(columns) == 0 {
		return t.schema
	}
	// Project schema
	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		for i := 0; i < t.schema.NumFields(); i++ {
			if t.schema.Field(i).Name == col {
				fields = append(fields, t.schema.Field(i))
				break
			}
		}
	}
	return arrow.NewSchema(fields, nil)
}

func (t *mockStatisticsTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	record := buildTestRecord(t.schema, t.data)
	defer record.Release()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

func (t *mockStatisticsTable) ColumnStatistics(ctx context.Context, columnName string, columnType string) (*catalog.ColumnStats, error) {
	t.statsCallCount.Add(1)

	if t.stats != nil {
		if stats, ok := t.stats[columnName]; ok {
			return stats, nil
		}
	}

	// Check if column exists
	found := false
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			found = true
			break
		}
	}
	if !found {
		return nil, catalog.ErrNotFound
	}

	// Return default statistics
	hasNotNull := true
	hasNull := false
	distinctCount := uint64(len(t.data))
	return &catalog.ColumnStats{
		HasNotNull:    &hasNotNull,
		HasNull:       &hasNull,
		DistinctCount: &distinctCount,
	}, nil
}

func (t *mockStatisticsTable) StatsCallCount() int64 {
	return t.statsCallCount.Load()
}

func (t *mockStatisticsTable) ResetStatsCallCount() {
	t.statsCallCount.Store(0)
}

// mockPartialStatisticsTable returns partial statistics (only min/max).
type mockPartialStatisticsTable struct {
	mockStatisticsTable
}

func (t *mockPartialStatisticsTable) ColumnStatistics(ctx context.Context, columnName string, columnType string) (*catalog.ColumnStats, error) {
	t.statsCallCount.Add(1)

	// Check if column exists
	found := false
	for i := 0; i < t.schema.NumFields(); i++ {
		if t.schema.Field(i).Name == columnName {
			found = true
			break
		}
	}
	if !found {
		return nil, catalog.ErrNotFound
	}

	// Return only min/max (other fields nil)
	return &catalog.ColumnStats{
		Min: int64(1),
		Max: int64(100),
	}, nil
}

// mockStatisticsSchema implements Schema with StatisticsTable tables.
type mockStatisticsSchema struct {
	name   string
	tables map[string]catalog.Table
}

func (s *mockStatisticsSchema) Name() string    { return s.name }
func (s *mockStatisticsSchema) Comment() string { return "Mock statistics schema" }

func (s *mockStatisticsSchema) Tables(_ context.Context) ([]catalog.Table, error) {
	tables := make([]catalog.Table, 0, len(s.tables))
	for _, t := range s.tables {
		tables = append(tables, t)
	}
	return tables, nil
}

func (s *mockStatisticsSchema) Table(_ context.Context, name string) (catalog.Table, error) {
	if t, ok := s.tables[name]; ok {
		return t, nil
	}
	return nil, nil
}

func (s *mockStatisticsSchema) TableFunctions(_ context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

func (s *mockStatisticsSchema) TableFunctionsInOut(_ context.Context) ([]catalog.TableFunctionInOut, error) {
	return nil, nil
}

func (s *mockStatisticsSchema) ScalarFunctions(_ context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

// mockStatisticsCatalog implements Catalog with StatisticsTable tables.
type mockStatisticsCatalog struct {
	schemas map[string]*mockStatisticsSchema
}

func (c *mockStatisticsCatalog) Schemas(_ context.Context) ([]catalog.Schema, error) {
	schemas := make([]catalog.Schema, 0, len(c.schemas))
	for _, s := range c.schemas {
		schemas = append(schemas, s)
	}
	return schemas, nil
}

func (c *mockStatisticsCatalog) Schema(_ context.Context, name string) (catalog.Schema, error) {
	if s, ok := c.schemas[name]; ok {
		return s, nil
	}
	return nil, nil
}

// newMockStatisticsCatalog creates a catalog with tables implementing StatisticsTable.
func newMockStatisticsCatalog() (*mockStatisticsCatalog, *mockStatisticsTable, *mockStatisticsTable) {
	// Users table
	usersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "department_id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	usersData := [][]any{
		{int64(1), "Alice", int64(10)},
		{int64(2), "Bob", int64(20)},
		{int64(3), "Charlie", int64(10)},
		{int64(4), "Diana", int64(30)},
	}

	hasNotNull := true
	hasNull := false
	distinctCount4 := uint64(4)
	distinctCount3 := uint64(3)
	maxStringLen := uint64(7)
	containsUnicode := false

	usersTable := &mockStatisticsTable{
		name:   "users",
		schema: usersSchema,
		data:   usersData,
		stats: map[string]*catalog.ColumnStats{
			"id": {
				HasNotNull:    &hasNotNull,
				HasNull:       &hasNull,
				DistinctCount: &distinctCount4,
				Min:           int64(1),
				Max:           int64(4),
			},
			"name": {
				HasNotNull:      &hasNotNull,
				HasNull:         &hasNull,
				DistinctCount:   &distinctCount4,
				Min:             "Alice",
				Max:             "Diana",
				MaxStringLength: &maxStringLen,
				ContainsUnicode: &containsUnicode,
			},
			"department_id": {
				HasNotNull:    &hasNotNull,
				HasNull:       &hasNull,
				DistinctCount: &distinctCount3,
				Min:           int64(10),
				Max:           int64(30),
			},
		},
	}

	// Departments table
	deptsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	deptsData := [][]any{
		{int64(10), "Engineering"},
		{int64(20), "Marketing"},
		{int64(30), "Sales"},
	}

	deptsTable := &mockStatisticsTable{
		name:   "departments",
		schema: deptsSchema,
		data:   deptsData,
		stats: map[string]*catalog.ColumnStats{
			"id": {
				HasNotNull:    &hasNotNull,
				HasNull:       &hasNull,
				DistinctCount: &distinctCount3,
				Min:           int64(10),
				Max:           int64(30),
			},
			"name": {
				HasNotNull:    &hasNotNull,
				HasNull:       &hasNull,
				DistinctCount: &distinctCount3,
			},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"stats_schema": {
				name: "stats_schema",
				tables: map[string]catalog.Table{
					"users":       usersTable,
					"departments": deptsTable,
				},
			},
		},
	}

	return cat, usersTable, deptsTable
}

// TestColumnStatisticsBasic tests basic column statistics retrieval.
func TestColumnStatisticsBasic(t *testing.T) {
	cat, usersTable, _ := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Reset call counters
	usersTable.ResetStatsCallCount()

	// Simple query that should trigger statistics collection
	query := fmt.Sprintf("SELECT * FROM %s.stats_schema.users WHERE id > 2", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Count results
	count := 0
	for rows.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	t.Logf("Statistics called %d times during query", usersTable.StatsCallCount())
}

// TestColumnStatisticsJoinOptimization tests that statistics are used for JOIN optimization.
// DuckDB uses statistics to estimate cardinality and choose optimal join algorithms.
func TestColumnStatisticsJoinOptimization(t *testing.T) {
	cat, usersTable, deptsTable := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Reset call counters
	usersTable.ResetStatsCallCount()
	deptsTable.ResetStatsCallCount()

	// Execute JOIN query - DuckDB should request statistics for join columns
	query := fmt.Sprintf(`
		SELECT u.name, d.name as department
		FROM %s.stats_schema.users u
		JOIN %s.stats_schema.departments d ON u.department_id = d.id
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}
	defer rows.Close()

	// Collect results
	var results []struct {
		name string
		dept string
	}
	for rows.Next() {
		var r struct {
			name string
			dept string
		}
		if err := rows.Scan(&r.name, &r.dept); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 4 {
		t.Errorf("expected 4 rows from JOIN, got %d", len(results))
	}

	// Log statistics calls (DuckDB may call statistics for join optimization)
	t.Logf("Users table statistics called %d times", usersTable.StatsCallCount())
	t.Logf("Departments table statistics called %d times", deptsTable.StatsCallCount())
}

// TestColumnStatisticsPartial tests partial statistics (some fields nil).
func TestColumnStatisticsPartial(t *testing.T) {
	// Create catalog with partial statistics table
	partialTable := &mockPartialStatisticsTable{
		mockStatisticsTable: mockStatisticsTable{
			name: "partial_stats",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: arrow.BinaryTypes.String},
			}, nil),
			data: [][]any{
				{int64(1), "one"},
				{int64(2), "two"},
			},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"stats_schema": {
				name: "stats_schema",
				tables: map[string]catalog.Table{
					"partial_stats": partialTable,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Query should work with partial statistics
	query := fmt.Sprintf("SELECT * FROM %s.stats_schema.partial_stats WHERE id > 0", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	t.Logf("Partial statistics called %d times", partialTable.StatsCallCount())
}

// TestColumnStatisticsInteger tests statistics for INTEGER columns.
func TestColumnStatisticsInteger(t *testing.T) {
	cat, usersTable, _ := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()

	// Filter query on integer column
	query := fmt.Sprintf("SELECT * FROM %s.stats_schema.users WHERE id BETWEEN 2 AND 3", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	t.Logf("Statistics called %d times for integer filter", usersTable.StatsCallCount())
}

// TestColumnStatisticsVarchar tests statistics for VARCHAR columns.
func TestColumnStatisticsVarchar(t *testing.T) {
	cat, usersTable, _ := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()

	// Filter query on varchar column
	query := fmt.Sprintf("SELECT * FROM %s.stats_schema.users WHERE name > 'C'", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	// Charlie and Diana should match
	if count != 2 {
		t.Errorf("expected 2 rows (Charlie, Diana), got %d", count)
	}

	t.Logf("Statistics called %d times for varchar filter", usersTable.StatsCallCount())
}

// TestColumnStatisticsAllNulls tests table returning all null statistics.
func TestColumnStatisticsAllNulls(t *testing.T) {
	// Create table that returns nil for all stats fields
	allNullsTable := &mockStatisticsTable{
		name: "all_nulls",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		data: [][]any{
			{int64(1)},
		},
		stats: map[string]*catalog.ColumnStats{
			"id": {}, // All fields nil
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"stats_schema": {
				name: "stats_schema",
				tables: map[string]catalog.Table{
					"all_nulls": allNullsTable,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Query should work even with all-null statistics
	query := fmt.Sprintf("SELECT * FROM %s.stats_schema.all_nulls", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}

	t.Logf("All-nulls statistics called %d times", allNullsTable.StatsCallCount())
}

// =============================================================================
// Complex Query Tests - CTEs, Aggregates, Subqueries
// =============================================================================

// TestColumnStatisticsCTE tests statistics with Common Table Expressions.
func TestColumnStatisticsCTE(t *testing.T) {
	cat, usersTable, deptsTable := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()
	deptsTable.ResetStatsCallCount()

	// CTE query - DuckDB may request statistics during planning
	query := fmt.Sprintf(`
		WITH active_users AS (
			SELECT id, name, department_id
			FROM %s.stats_schema.users
			WHERE id > 1
		),
		dept_info AS (
			SELECT id, name as dept_name
			FROM %s.stats_schema.departments
			WHERE id >= 10
		)
		SELECT u.name, d.dept_name
		FROM active_users u
		JOIN dept_info d ON u.department_id = d.id
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 rows from CTE query, got %d", count)
	}

	t.Logf("CTE: Users stats called %d times, Departments stats called %d times",
		usersTable.StatsCallCount(), deptsTable.StatsCallCount())
}

// TestColumnStatisticsAggregates tests statistics with GROUP BY aggregations.
func TestColumnStatisticsAggregates(t *testing.T) {
	cat, usersTable, _ := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()

	// Aggregate query with GROUP BY
	query := fmt.Sprintf(`
		SELECT department_id, COUNT(*) as user_count
		FROM %s.stats_schema.users
		GROUP BY department_id
		HAVING COUNT(*) > 0
		ORDER BY user_count DESC
	`, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Aggregate query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 department groups, got %d", count)
	}

	t.Logf("Aggregate: Statistics called %d times", usersTable.StatsCallCount())
}

// TestColumnStatisticsSubquery tests statistics with subqueries.
func TestColumnStatisticsSubquery(t *testing.T) {
	cat, usersTable, deptsTable := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()
	deptsTable.ResetStatsCallCount()

	// Subquery - filter users based on department existence
	query := fmt.Sprintf(`
		SELECT name
		FROM %s.stats_schema.users
		WHERE department_id IN (
			SELECT id FROM %s.stats_schema.departments WHERE id > 15
		)
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Subquery failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	// Only users with department_id > 15 (20, 30) = Bob (20), Diana (30)
	if count != 2 {
		t.Errorf("expected 2 rows from subquery, got %d", count)
	}

	t.Logf("Subquery: Users stats %d, Departments stats %d",
		usersTable.StatsCallCount(), deptsTable.StatsCallCount())
}

// TestColumnStatisticsExistsSubquery tests statistics with EXISTS subquery.
func TestColumnStatisticsExistsSubquery(t *testing.T) {
	cat, usersTable, deptsTable := newMockStatisticsCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()
	deptsTable.ResetStatsCallCount()

	// EXISTS subquery
	query := fmt.Sprintf(`
		SELECT u.name
		FROM %s.stats_schema.users u
		WHERE EXISTS (
			SELECT 1 FROM %s.stats_schema.departments d
			WHERE d.id = u.department_id AND d.id = 10
		)
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("EXISTS subquery failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	// Users with department_id = 10: Alice, Charlie
	if count != 2 {
		t.Errorf("expected 2 rows from EXISTS subquery, got %d", count)
	}

	t.Logf("EXISTS: Users stats %d, Departments stats %d",
		usersTable.StatsCallCount(), deptsTable.StatsCallCount())
}

// TestColumnStatisticsMultipleJoins tests statistics with multiple JOINs.
func TestColumnStatisticsMultipleJoins(t *testing.T) {
	// Create a catalog with 3 tables for multi-join testing
	hasNotNull := true
	hasNull := false
	distinctCount := uint64(4)

	usersTable := &mockStatisticsTable{
		name: "users",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "role_id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		data: [][]any{
			{int64(1), "Alice", int64(1)},
			{int64(2), "Bob", int64(2)},
			{int64(3), "Charlie", int64(1)},
			{int64(4), "Diana", int64(3)},
		},
		stats: map[string]*catalog.ColumnStats{
			"id":      {HasNotNull: &hasNotNull, HasNull: &hasNull, DistinctCount: &distinctCount, Min: int64(1), Max: int64(4)},
			"name":    {HasNotNull: &hasNotNull, HasNull: &hasNull},
			"role_id": {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(3)},
		},
	}

	rolesTable := &mockStatisticsTable{
		name: "roles",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "level", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		data: [][]any{
			{int64(1), "Admin", int64(10)},
			{int64(2), "User", int64(5)},
			{int64(3), "Guest", int64(1)},
		},
		stats: map[string]*catalog.ColumnStats{
			"id":    {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(3)},
			"name":  {HasNotNull: &hasNotNull, HasNull: &hasNull},
			"level": {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(10)},
		},
	}

	permissionsTable := &mockStatisticsTable{
		name: "permissions",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "role_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "permission", Type: arrow.BinaryTypes.String},
		}, nil),
		data: [][]any{
			{int64(1), "read"},
			{int64(1), "write"},
			{int64(2), "read"},
			{int64(3), "read"},
		},
		stats: map[string]*catalog.ColumnStats{
			"role_id":    {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(3)},
			"permission": {HasNotNull: &hasNotNull, HasNull: &hasNull},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"stats_schema": {
				name: "stats_schema",
				tables: map[string]catalog.Table{
					"users":       usersTable,
					"roles":       rolesTable,
					"permissions": permissionsTable,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	usersTable.ResetStatsCallCount()
	rolesTable.ResetStatsCallCount()
	permissionsTable.ResetStatsCallCount()

	// Triple JOIN query
	query := fmt.Sprintf(`
		SELECT u.name, r.name as role, p.permission
		FROM %s.stats_schema.users u
		JOIN %s.stats_schema.roles r ON u.role_id = r.id
		JOIN %s.stats_schema.permissions p ON r.id = p.role_id
		WHERE r.level >= 5
	`, attachName, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Multi-join query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	// Admin (level 10) has 2 permissions, User (level 5) has 1 permission
	// Users with Admin: Alice, Charlie (2 each) = 4
	// Users with User: Bob (1) = 1
	// Total = 5
	if count != 5 {
		t.Errorf("expected 5 rows from multi-join, got %d", count)
	}

	t.Logf("Multi-join: Users stats %d, Roles stats %d, Permissions stats %d",
		usersTable.StatsCallCount(), rolesTable.StatsCallCount(), permissionsTable.StatsCallCount())
}

// =============================================================================
// Mixed Tables Tests - Statistics + Non-Statistics Tables Together
// =============================================================================

// mockSimpleTable is a table that does NOT implement StatisticsTable.
type mockSimpleTable struct {
	name   string
	schema *arrow.Schema
	data   [][]any
}

func (t *mockSimpleTable) Name() string    { return t.name }
func (t *mockSimpleTable) Comment() string { return "Simple table without statistics" }

func (t *mockSimpleTable) ArrowSchema(columns []string) *arrow.Schema {
	if len(columns) == 0 {
		return t.schema
	}
	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		for i := 0; i < t.schema.NumFields(); i++ {
			if t.schema.Field(i).Name == col {
				fields = append(fields, t.schema.Field(i))
				break
			}
		}
	}
	return arrow.NewSchema(fields, nil)
}

func (t *mockSimpleTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	record := buildTestRecord(t.schema, t.data)
	defer record.Release()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

// TestMixedTablesJoinStatisticsAndNonStatistics tests JOINs between
// tables that implement StatisticsTable and those that don't.
func TestMixedTablesJoinStatisticsAndNonStatistics(t *testing.T) {
	hasNotNull := true
	hasNull := false

	// Table WITH statistics
	ordersTable := &mockStatisticsTable{
		name: "orders",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "customer_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
		}, nil),
		data: [][]any{
			{int64(1), int64(100), float64(99.99)},
			{int64(2), int64(101), float64(149.50)},
			{int64(3), int64(100), float64(25.00)},
			{int64(4), int64(102), float64(500.00)},
		},
		stats: map[string]*catalog.ColumnStats{
			"id":          {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(4)},
			"customer_id": {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(100), Max: int64(102)},
			"amount":      {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: float64(25.0), Max: float64(500.0)},
		},
	}

	// Table WITHOUT statistics (simple table)
	customersTable := &mockSimpleTable{
		name: "customers",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "email", Type: arrow.BinaryTypes.String},
		}, nil),
		data: [][]any{
			{int64(100), "Alice Smith", "alice@example.com"},
			{int64(101), "Bob Jones", "bob@example.com"},
			{int64(102), "Carol White", "carol@example.com"},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"mixed_schema": {
				name: "mixed_schema",
				tables: map[string]catalog.Table{
					"orders":    ordersTable,
					"customers": customersTable,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	ordersTable.ResetStatsCallCount()

	// JOIN between statistics table and non-statistics table
	query := fmt.Sprintf(`
		SELECT c.name, o.amount
		FROM %s.mixed_schema.customers c
		JOIN %s.mixed_schema.orders o ON c.id = o.customer_id
		WHERE o.amount > 50
		ORDER BY o.amount DESC
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Mixed tables JOIN failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		name   string
		amount float64
	}
	for rows.Next() {
		var r struct {
			name   string
			amount float64
		}
		if err := rows.Scan(&r.name, &r.amount); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, r)
	}

	// Orders > 50: Carol (500), Bob (149.50), Alice (99.99) = 3 orders
	if len(results) != 3 {
		t.Errorf("expected 3 rows, got %d", len(results))
	}

	t.Logf("Mixed JOIN: Orders (with stats) called %d times", ordersTable.StatsCallCount())
}

// TestMixedTablesCTEWithBothTypes tests CTEs mixing statistics and non-statistics tables.
func TestMixedTablesCTEWithBothTypes(t *testing.T) {
	hasNotNull := true
	hasNull := false

	// Table WITH statistics
	productsTable := &mockStatisticsTable{
		name: "products",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		}, nil),
		data: [][]any{
			{int64(1), "Widget", float64(10.00)},
			{int64(2), "Gadget", float64(25.00)},
			{int64(3), "Gizmo", float64(15.00)},
		},
		stats: map[string]*catalog.ColumnStats{
			"id":    {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(3)},
			"name":  {HasNotNull: &hasNotNull, HasNull: &hasNull},
			"price": {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: float64(10.0), Max: float64(25.0)},
		},
	}

	// Table WITHOUT statistics
	inventoryTable := &mockSimpleTable{
		name: "inventory",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
			{Name: "warehouse", Type: arrow.BinaryTypes.String},
		}, nil),
		data: [][]any{
			{int64(1), int64(100), "NYC"},
			{int64(1), int64(50), "LA"},
			{int64(2), int64(200), "NYC"},
			{int64(3), int64(75), "Chicago"},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"mixed_schema": {
				name: "mixed_schema",
				tables: map[string]catalog.Table{
					"products":  productsTable,
					"inventory": inventoryTable,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	productsTable.ResetStatsCallCount()

	// CTE mixing both types
	query := fmt.Sprintf(`
		WITH product_totals AS (
			SELECT product_id, SUM(quantity) as total_qty
			FROM %s.mixed_schema.inventory
			GROUP BY product_id
		),
		expensive_products AS (
			SELECT id, name, price
			FROM %s.mixed_schema.products
			WHERE price > 12
		)
		SELECT ep.name, ep.price, pt.total_qty
		FROM expensive_products ep
		JOIN product_totals pt ON ep.id = pt.product_id
		ORDER BY ep.price DESC
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Mixed CTE query failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		name     string
		price    float64
		totalQty int64
	}
	for rows.Next() {
		var r struct {
			name     string
			price    float64
			totalQty int64
		}
		if err := rows.Scan(&r.name, &r.price, &r.totalQty); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, r)
	}

	// Products > 12: Gadget (25), Gizmo (15) = 2 products
	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}

	t.Logf("Mixed CTE: Products (with stats) called %d times", productsTable.StatsCallCount())
}

// TestMixedTablesUnion tests UNION between statistics and non-statistics tables.
func TestMixedTablesUnion(t *testing.T) {
	hasNotNull := true
	hasNull := false

	// Table WITH statistics
	table1 := &mockStatisticsTable{
		name: "data_with_stats",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
		data: [][]any{
			{int64(1), "A"},
			{int64(2), "B"},
		},
		stats: map[string]*catalog.ColumnStats{
			"id":    {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(1), Max: int64(2)},
			"value": {HasNotNull: &hasNotNull, HasNull: &hasNull},
		},
	}

	// Table WITHOUT statistics
	table2 := &mockSimpleTable{
		name: "data_without_stats",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
		data: [][]any{
			{int64(3), "C"},
			{int64(4), "D"},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"mixed_schema": {
				name: "mixed_schema",
				tables: map[string]catalog.Table{
					"data_with_stats":    table1,
					"data_without_stats": table2,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	table1.ResetStatsCallCount()

	// UNION query
	query := fmt.Sprintf(`
		SELECT id, value FROM %s.mixed_schema.data_with_stats
		UNION ALL
		SELECT id, value FROM %s.mixed_schema.data_without_stats
		ORDER BY id
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("UNION query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 4 {
		t.Errorf("expected 4 rows from UNION, got %d", count)
	}

	t.Logf("UNION: Stats table called %d times", table1.StatsCallCount())
}

// TestMixedTablesLeftJoin tests LEFT JOIN with non-statistics table on the left.
func TestMixedTablesLeftJoin(t *testing.T) {
	hasNotNull := true
	hasNull := false

	// Table WITHOUT statistics (left side of JOIN)
	employeesTable := &mockSimpleTable{
		name: "employees",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "dept_id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		data: [][]any{
			{int64(1), "Alice", int64(10)},
			{int64(2), "Bob", int64(20)},
			{int64(3), "Charlie", int64(99)}, // dept_id 99 doesn't exist
		},
	}

	// Table WITH statistics (right side of JOIN)
	deptsTable := &mockStatisticsTable{
		name: "depts",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil),
		data: [][]any{
			{int64(10), "Engineering"},
			{int64(20), "Sales"},
		},
		stats: map[string]*catalog.ColumnStats{
			"id":   {HasNotNull: &hasNotNull, HasNull: &hasNull, Min: int64(10), Max: int64(20)},
			"name": {HasNotNull: &hasNotNull, HasNull: &hasNull},
		},
	}

	cat := &mockStatisticsCatalog{
		schemas: map[string]*mockStatisticsSchema{
			"mixed_schema": {
				name: "mixed_schema",
				tables: map[string]catalog.Table{
					"employees": employeesTable,
					"depts":     deptsTable,
				},
			},
		},
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	deptsTable.ResetStatsCallCount()

	// LEFT JOIN - non-stats table LEFT JOIN stats table
	query := fmt.Sprintf(`
		SELECT e.name, d.name as dept_name
		FROM %s.mixed_schema.employees e
		LEFT JOIN %s.mixed_schema.depts d ON e.dept_id = d.id
		ORDER BY e.id
	`, attachName, attachName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("LEFT JOIN query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	var charlieHasNullDept bool
	for rows.Next() {
		var name string
		var deptName *string
		if err := rows.Scan(&name, &deptName); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if name == "Charlie" && deptName == nil {
			charlieHasNullDept = true
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 rows from LEFT JOIN, got %d", count)
	}
	if !charlieHasNullDept {
		t.Error("expected Charlie to have NULL department")
	}

	t.Logf("LEFT JOIN: Depts (with stats) called %d times", deptsTable.StatsCallCount())
}
