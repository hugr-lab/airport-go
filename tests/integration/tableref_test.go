package airport_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// Integration tests for TableRef support.
// Tests validate:
// - Data query via read_csv function call (SELECT from table ref)
// - Catalog discovery (table visible via duckdb_tables())
// - Column metadata correctness (via duckdb_columns())
// - Coexistence with regular tables
// - Multiple table refs in the same schema

// simpleTableRef implements catalog.TableRef for integration testing.
type simpleTableRef struct {
	name    string
	comment string
	schema  *arrow.Schema
	csvURL  string
}

func (t *simpleTableRef) Name() string               { return t.name }
func (t *simpleTableRef) Comment() string            { return t.comment }
func (t *simpleTableRef) ArrowSchema() *arrow.Schema { return t.schema }

func (t *simpleTableRef) FunctionCalls(ctx context.Context, req *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
	return []catalog.FunctionCall{
		{
			FunctionName: "read_csv",
			Args: []catalog.FunctionCallArg{
				{Value: t.csvURL, Type: arrow.BinaryTypes.String},
				{Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
			},
		},
	}, nil
}

// staticFuncRef implements catalog.TableRef with a generate_series call.
type staticFuncRef struct {
	name    string
	comment string
	schema  *arrow.Schema
}

func (t *staticFuncRef) Name() string               { return t.name }
func (t *staticFuncRef) Comment() string            { return t.comment }
func (t *staticFuncRef) ArrowSchema() *arrow.Schema { return t.schema }

func (t *staticFuncRef) FunctionCalls(ctx context.Context, req *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
	return []catalog.FunctionCall{
		{
			FunctionName: "generate_series",
			Args: []catalog.FunctionCallArg{
				{Value: int64(1), Type: arrow.PrimitiveTypes.Int64},
				{Value: int64(10), Type: arrow.PrimitiveTypes.Int64},
			},
		},
	}, nil
}

// TestTableRefSelectData verifies that SELECT queries against a table ref
// return data from the DuckDB function call (read_csv).
func TestTableRefSelectData(t *testing.T) {
	// Create a temp CSV file with test data
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "data.csv")
	csvContent := "id,name,value\n1,Alice,10.5\n2,Bob,20.3\n3,Charlie,30.1\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	ref := &simpleTableRef{
		name:   "csv_data",
		schema: refSchema,
		csvURL: csvPath,
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("refs").
		TableRef(ref).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	query := fmt.Sprintf("SELECT * FROM %s.refs.csv_data ORDER BY id", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id    int64
		name  string
		value float64
	}{
		{1, "Alice", 10.5},
		{2, "Bob", 20.3},
		{3, "Charlie", 30.1},
	}

	idx := 0
	for rows.Next() {
		var id int64
		var name string
		var value float64
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if idx < len(expected) {
			if id != expected[idx].id {
				t.Errorf("Row %d: expected id %d, got %d", idx, expected[idx].id, id)
			}
			if name != expected[idx].name {
				t.Errorf("Row %d: expected name %q, got %q", idx, expected[idx].name, name)
			}
			if value != expected[idx].value {
				t.Errorf("Row %d: expected value %f, got %f", idx, expected[idx].value, value)
			}
		}
		idx++
	}

	if idx != len(expected) {
		t.Fatalf("Expected %d rows, got %d", len(expected), idx)
	}
}

// TestTableRefDiscovery verifies that a TableRef appears as a normal table
// in DuckDB's catalog with correct metadata.
func TestTableRefDiscovery(t *testing.T) {
	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	ref := &simpleTableRef{
		name:    "csv_data",
		comment: "Remote CSV data",
		schema:  refSchema,
		csvURL:  "https://example.com/data.csv",
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("refs").
		TableRef(ref).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("TableVisible", func(t *testing.T) {
		query := "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'refs'"
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		found := false
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if name == "csv_data" {
				found = true
			}
		}

		if !found {
			t.Error("Table 'csv_data' not found in duckdb_tables()")
		}
	})

	t.Run("DescribeColumns", func(t *testing.T) {
		query := `SELECT column_name, data_type
		          FROM duckdb_columns()
		          WHERE database_name = ?
		            AND schema_name = 'refs'
		            AND table_name = 'csv_data'
		          ORDER BY column_index`
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		expectedColumns := []struct {
			name     string
			dataType string
		}{
			{"id", "BIGINT"},
			{"name", "VARCHAR"},
			{"value", "DOUBLE"},
		}

		idx := 0
		for rows.Next() {
			var colName, colType string
			if err := rows.Scan(&colName, &colType); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if idx < len(expectedColumns) {
				if colName != expectedColumns[idx].name {
					t.Errorf("Column %d: expected name %q, got %q", idx, expectedColumns[idx].name, colName)
				}
				if colType != expectedColumns[idx].dataType {
					t.Errorf("Column %d: expected type %q, got %q", idx, expectedColumns[idx].dataType, colType)
				}
			}
			idx++
		}

		if idx != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), idx)
		}
	})
}

// TestTableRefCoexistsWithRegularTables verifies that table refs and regular
// tables can coexist in the same schema. Regular table queries must work
// even when table refs are present.
func TestTableRefCoexistsWithRegularTables(t *testing.T) {
	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	usersData := [][]any{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
	}

	refSchema := arrow.NewSchema([]arrow.Field{
		{Name: "generate_series", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ref := &staticFuncRef{
		name:   "series_data",
		schema: refSchema,
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("refs").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "User accounts",
			Schema:   tableSchema,
			ScanFunc: makeScanFunc(tableSchema, usersData),
		}).
		TableRef(ref).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Regular table queries must still work alongside table refs
	t.Run("QueryRegularTable", func(t *testing.T) {
		query := fmt.Sprintf("SELECT * FROM %s.refs.users ORDER BY id", attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})

	// Both table and table ref should be visible via duckdb_tables()
	t.Run("BothVisible", func(t *testing.T) {
		query := "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'refs' ORDER BY table_name"
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			tables = append(tables, name)
		}

		if len(tables) != 2 {
			t.Errorf("Expected 2 tables, got %d: %v", len(tables), tables)
		}
	})
}

// TestTableRefMultipleInSchema verifies that multiple table refs can be
// registered in the same schema.
func TestTableRefMultipleInSchema(t *testing.T) {
	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	ref1 := &staticFuncRef{name: "series_a", schema: schema1}
	ref2 := &staticFuncRef{name: "series_b", schema: schema2}

	cat, err := airport.NewCatalogBuilder().
		Schema("refs").
		TableRef(ref1).
		TableRef(ref2).
		Build()
	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	query := "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = 'refs' ORDER BY table_name"
	rows, err := db.Query(query, attachName)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		tables = append(tables, name)
	}

	if len(tables) != 2 {
		t.Fatalf("Expected 2 table refs, got %d: %v", len(tables), tables)
	}

	if tables[0] != "series_a" || tables[1] != "series_b" {
		t.Errorf("Expected [series_a, series_b], got %v", tables)
	}

	// Verify each table ref has correct columns
	for _, tc := range []struct {
		table   string
		colName string
		colType string
	}{
		{"series_a", "id", "BIGINT"},
		{"series_b", "value", "DOUBLE"},
	} {
		q := `SELECT column_name, data_type
		      FROM duckdb_columns()
		      WHERE database_name = ?
		        AND schema_name = 'refs'
		        AND table_name = ?`
		r, err := db.Query(q, attachName, tc.table)
		if err != nil {
			t.Fatalf("Query failed for %s: %v", tc.table, err)
		}

		if !r.Next() {
			r.Close()
			t.Errorf("No columns found for table %s", tc.table)
			continue
		}

		var colName, colType string
		if err := r.Scan(&colName, &colType); err != nil {
			r.Close()
			t.Fatalf("Scan failed: %v", err)
		}
		r.Close()

		if colName != tc.colName {
			t.Errorf("Table %s: expected column %q, got %q", tc.table, tc.colName, colName)
		}
		if colType != tc.colType {
			t.Errorf("Table %s: expected type %q, got %q", tc.table, tc.colType, colType)
		}
	}
}
