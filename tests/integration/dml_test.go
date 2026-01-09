package airport_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	airport "github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// =============================================================================
// DML Integration Tests via DuckDB SQL
// =============================================================================
// These tests use DuckDB as a Flight client to execute SQL DML statements
// (INSERT, UPDATE, DELETE) against the Airport Flight server.
//
// Based on Airport extension protocol:
// - INSERT/UPDATE/DELETE use DoExchange RPC with airport-operation header
// - UPDATE and DELETE require tables to have a rowid pseudocolumn
// - Tables must expose rowid in schema metadata with is_rowid key
// =============================================================================

// TestDMLInsert tests INSERT operations using DuckDB SQL.
func TestDMLInsert(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("InsertSingleRow", func(t *testing.T) {
		table.Clear()

		// Insert via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(
			"INSERT INTO %s.dml_schema.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
			attachName,
		))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		// Verify via SELECT
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 1 {
			t.Errorf("expected 1 row, got %d", count)
		}

		// Verify data was stored in table
		if table.RowCount() != 1 {
			t.Errorf("expected 1 row in memory, got %d", table.RowCount())
		}
	})

	t.Run("InsertMultipleRows", func(t *testing.T) {
		table.Clear()

		// Insert multiple rows
		_, err := db.Exec(fmt.Sprintf(
			"INSERT INTO %s.dml_schema.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com'), (3, 'Charlie', 'charlie@example.com')",
			attachName,
		))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		// Verify count
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}
	})

	t.Run("InsertLargeDataset", func(t *testing.T) {
		table.Clear()

		// Use DuckDB's generate_series to create a large dataset
		// This tests streaming INSERT without loading all data into memory
		const rowCount = 10_000_000 // 10M rows

		// Generate and insert rows using SQL
		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i as id,
				'user_' || i as name,
				'user_' || i || '@example.com' as email
			FROM generate_series(1, %d) as t(i)
		`, attachName, rowCount))
		if err != nil {
			t.Fatalf("INSERT large dataset failed: %v", err)
		}

		// Verify data was stored in table (check directly, not via SELECT which would overflow IPC)
		if int64(table.RowCount()) != rowCount {
			t.Errorf("expected %d rows in memory, got %d", rowCount, table.RowCount())
		}

		t.Logf("Successfully inserted %d rows", rowCount)
	})
}

// TestDMLInsertReturning tests INSERT with RETURNING clause using DuckDB SQL.
// This uses bidirectional streaming pipeline similar to scalar functions.
func TestDMLInsertReturning(t *testing.T) {

	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	table.EnableReturning() // Enable RETURNING support
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("ReturningSingleRow", func(t *testing.T) {
		table.Clear()

		// Insert with RETURNING - return only the id column
		rows, err := db.Query(fmt.Sprintf(
			"INSERT INTO %s.dml_schema.users (id, name, email) VALUES (42, 'Alice', 'alice@example.com') RETURNING id",
			attachName,
		))
		if err != nil {
			t.Fatalf("INSERT RETURNING failed: %v", err)
		}
		defer rows.Close()

		// Read returned values
		var returnedIDs []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedIDs = append(returnedIDs, id)
		}

		if len(returnedIDs) != 1 {
			t.Errorf("expected 1 returned row, got %d", len(returnedIDs))
		}
		if len(returnedIDs) > 0 && returnedIDs[0] != 42 {
			t.Errorf("expected returned id=42, got %d", returnedIDs[0])
		}

		t.Logf("Successfully returned %d row(s) with id=%v", len(returnedIDs), returnedIDs)
	})

	t.Run("ReturningMultipleRows", func(t *testing.T) {
		table.Clear()

		// Insert 10 rows with RETURNING
		rows, err := db.Query(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i as id,
				'user_' || i as name,
				'user_' || i || '@example.com' as email
			FROM generate_series(1, 10) as t(i)
			RETURNING id
		`, attachName))
		if err != nil {
			t.Fatalf("INSERT RETURNING failed: %v", err)
		}
		defer rows.Close()

		// Read returned values
		var returnedIDs []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedIDs = append(returnedIDs, id)
		}

		if len(returnedIDs) != 10 {
			t.Errorf("expected 10 returned rows, got %d", len(returnedIDs))
		}

		t.Logf("Successfully returned %d row(s)", len(returnedIDs))
	})

	t.Run("ReturningLargeDataset", func(t *testing.T) {
		table.Clear()

		const rowCount = 100_000 // 100K rows

		// Insert with RETURNING - return only the id column
		rows, err := db.Query(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i as id,
				'user_' || i as name,
				'user_' || i || '@example.com' as email
			FROM generate_series(1, %d) as t(i)
			RETURNING id
		`, attachName, rowCount))
		if err != nil {
			t.Fatalf("INSERT RETURNING failed: %v", err)
		}
		defer rows.Close()

		// Count returned values
		var returnedCount int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedCount++
		}

		if returnedCount != rowCount {
			t.Errorf("expected %d returned rows, got %d", rowCount, returnedCount)
		}

		t.Logf("Successfully returned %d row(s)", returnedCount)
	})
}

// TestDMLInsertReturningColumns tests that DMLOptions.Returning and ReturningColumns
// are correctly populated when INSERT with RETURNING clause is executed.
// Also verifies that only the requested columns are returned in the result.
func TestDMLInsertReturningColumns(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	table.EnableReturning()
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("ReturningSingleColumn", func(t *testing.T) {
		table.Clear()

		// INSERT with only name and email (NO id), request RETURNING id
		// This simulates auto-generated id - the table should generate id and return it
		rows, err := db.Query(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (name, email)
			SELECT 'user_' || i as name, 'user_' || i || '@example.com' as email
			FROM generate_series(1, 3) as t(i)
			RETURNING id
		`, attachName))
		if err != nil {
			t.Fatalf("INSERT RETURNING failed: %v", err)
		}

		// Read returned values - should only have id column
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}
		t.Logf("Returned columns: %v", cols)

		// Verify only id column is returned
		if len(cols) != 1 {
			t.Errorf("expected 1 column returned, got %d: %v", len(cols), cols)
		}
		if len(cols) > 0 && cols[0] != "id" {
			t.Errorf("expected column 'id', got '%s'", cols[0])
		}

		// Count returned rows and verify auto-generated id values
		var returnedIDs []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedIDs = append(returnedIDs, id)
			t.Logf("Returned auto-generated id: %d", id)
		}
		rows.Close()

		if len(returnedIDs) != 3 {
			t.Errorf("expected 3 returned rows, got %d", len(returnedIDs))
		}

		// Verify auto-generated IDs are sequential (1, 2, 3)
		for i, id := range returnedIDs {
			expected := int64(i + 1)
			if id != expected {
				t.Errorf("expected auto-generated id %d at position %d, got %d", expected, i, id)
			}
		}

		// Verify DMLOptions received by table
		opts := table.GetLastInsertOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if !opts.Returning {
			t.Errorf("expected Returning=true, got false")
		}
		// ReturningColumns should contain all table column names (excluding rowid)
		// because DuckDB Airport extension doesn't communicate specific RETURNING columns.
		// Server populates with all columns; DuckDB filters client-side.
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)

		// Expect all table columns: id, name, email (not rowid)
		expectedCols := []string{"id", "name", "email"}
		if len(opts.ReturningColumns) != len(expectedCols) {
			t.Errorf("expected ReturningColumns=%v, got %v", expectedCols, opts.ReturningColumns)
		}
	})

	t.Run("ReturningAllColumns", func(t *testing.T) {
		table.Clear()

		// INSERT with only name and email (NO id), RETURNING * - should get all columns including auto-generated id
		rows, err := db.Query(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (name, email)
			VALUES ('alice', 'alice@example.com')
			RETURNING *
		`, attachName))
		if err != nil {
			t.Fatalf("INSERT RETURNING * failed: %v", err)
		}

		// Read returned columns - should have all table columns (id, name, email - not rowid)
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}
		t.Logf("Returned columns for RETURNING *: %v", cols)

		// RETURNING * should return all user-facing columns (id, name, email - not rowid)
		if len(cols) < 3 {
			t.Errorf("expected at least 3 columns for RETURNING *, got %d: %v", len(cols), cols)
		}

		// Read the first row and verify auto-generated id is returned
		if !rows.Next() {
			rows.Close()
			t.Fatal("expected at least 1 returned row for RETURNING *")
		}
		// Scan all columns to verify they're present
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("RETURNING * row: %v", vals)
		rows.Close()

		// Verify DMLOptions
		opts := table.GetLastInsertOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if !opts.Returning {
			t.Errorf("expected Returning=true, got false")
		}
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)
	})

	t.Run("ReturningFalse", func(t *testing.T) {
		table.Clear()

		// Execute INSERT without RETURNING - should set Returning=false
		_, err := db.Exec(fmt.Sprintf(
			"INSERT INTO %s.dml_schema.users (name, email) VALUES ('Bob', 'bob@example.com')",
			attachName,
		))
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		// Verify DMLOptions received by table
		opts := table.GetLastInsertOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if opts.Returning {
			t.Errorf("expected Returning=false, got true")
		}
		if len(opts.ReturningColumns) != 0 {
			t.Errorf("expected ReturningColumns to be empty when Returning=false, got %v", opts.ReturningColumns)
		}
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)
	})
}

// TestDMLUpdateReturningColumns tests that DMLOptions.Returning and ReturningColumns
// are correctly populated when UPDATE with RETURNING clause is executed.
// Also verifies that only the requested columns are returned in the result.
func TestDMLUpdateReturningColumns(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	table.EnableReturning()
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("ReturningSingleColumn", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
		})

		// UPDATE and request RETURNING id - should only get id column back
		rows, err := db.Query(fmt.Sprintf(
			"UPDATE %s.dml_schema.users SET name = 'Updated' WHERE id IN (1, 2) RETURNING id",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE RETURNING failed: %v", err)
		}

		// Read returned columns - should only have id column
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}
		t.Logf("Returned columns: %v", cols)

		// Verify only id column is returned
		if len(cols) != 1 {
			t.Errorf("expected 1 column returned, got %d: %v", len(cols), cols)
		}
		if len(cols) > 0 && cols[0] != "id" {
			t.Errorf("expected column 'id', got '%s'", cols[0])
		}

		// Count returned rows
		var returnedCount int
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedCount++
		}
		rows.Close()

		if returnedCount != 2 {
			t.Errorf("expected 2 returned rows, got %d", returnedCount)
		}

		// Verify DMLOptions received by table
		opts := table.GetLastUpdateOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if !opts.Returning {
			t.Errorf("expected Returning=true, got false")
		}
		// ReturningColumns should contain all table column names (excluding rowid)
		// because DuckDB Airport extension doesn't communicate specific RETURNING columns.
		// Server populates with all columns; DuckDB filters client-side.
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)

		// Expect all table columns: id, name, email (not rowid)
		expectedCols := []string{"id", "name", "email"}
		if len(opts.ReturningColumns) != len(expectedCols) {
			t.Errorf("expected ReturningColumns=%v, got %v", expectedCols, opts.ReturningColumns)
		}
	})

	t.Run("ReturningAllColumns", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
		})

		// UPDATE and RETURNING * - should get all columns back
		rows, err := db.Query(fmt.Sprintf(
			"UPDATE %s.dml_schema.users SET name = 'Alice Updated' WHERE id = 1 RETURNING *",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE RETURNING * failed: %v", err)
		}

		// Read returned columns - should have all table columns
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}
		t.Logf("Returned columns for RETURNING *: %v", cols)

		// RETURNING * should return all table columns
		if len(cols) < 3 {
			t.Errorf("expected at least 3 columns for RETURNING *, got %d: %v", len(cols), cols)
		}

		// Read at least one row
		var hasRow bool
		for rows.Next() {
			hasRow = true
			break
		}
		rows.Close()

		if !hasRow {
			t.Errorf("expected at least 1 returned row for RETURNING *")
		}

		// Verify DMLOptions
		opts := table.GetLastUpdateOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if !opts.Returning {
			t.Errorf("expected Returning=true, got false")
		}
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)
	})

	t.Run("ReturningFalse", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
		})

		// Execute UPDATE without RETURNING
		_, err := db.Exec(fmt.Sprintf(
			"UPDATE %s.dml_schema.users SET name = 'Alice Updated' WHERE id = 1",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}

		// Verify DMLOptions received by table
		opts := table.GetLastUpdateOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if opts.Returning {
			t.Errorf("expected Returning=false, got true")
		}
		if len(opts.ReturningColumns) != 0 {
			t.Errorf("expected ReturningColumns to be empty when Returning=false, got %v", opts.ReturningColumns)
		}
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)
	})
}

// TestDMLDeleteReturningColumns tests that DMLOptions.Returning and ReturningColumns
// are correctly populated when DELETE with RETURNING clause is executed.
// Also verifies that only the requested columns are returned in the result.
func TestDMLDeleteReturningColumns(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	table.EnableReturning()
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("ReturningSingleColumn", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// DELETE and request RETURNING id - should only get id column back
		rows, err := db.Query(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id IN (1, 2) RETURNING id",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE RETURNING failed: %v", err)
		}

		// Read returned columns - should only have id column
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}
		t.Logf("Returned columns: %v", cols)

		// Verify only id column is returned
		if len(cols) != 1 {
			t.Errorf("expected 1 column returned, got %d: %v", len(cols), cols)
		}
		if len(cols) > 0 && cols[0] != "id" {
			t.Errorf("expected column 'id', got '%s'", cols[0])
		}

		// Count returned rows
		var returnedCount int
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			returnedCount++
		}
		rows.Close()

		if returnedCount != 2 {
			t.Errorf("expected 2 returned rows, got %d", returnedCount)
		}

		// Verify DMLOptions received by table
		opts := table.GetLastDeleteOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if !opts.Returning {
			t.Errorf("expected Returning=true, got false")
		}
		// ReturningColumns should contain all table column names (excluding rowid)
		// because DuckDB Airport extension doesn't communicate specific RETURNING columns.
		// Server populates with all columns; DuckDB filters client-side.
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)

		// Expect all table columns: id, name, email (not rowid)
		expectedCols := []string{"id", "name", "email"}
		if len(opts.ReturningColumns) != len(expectedCols) {
			t.Errorf("expected ReturningColumns=%v, got %v", expectedCols, opts.ReturningColumns)
		}
	})

	t.Run("ReturningAllColumns", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
		})

		// DELETE and RETURNING * - should get all columns back
		rows, err := db.Query(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id = 1 RETURNING *",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE RETURNING * failed: %v", err)
		}

		// Read returned columns - should have all table columns
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}
		t.Logf("Returned columns for RETURNING *: %v", cols)

		// RETURNING * should return all table columns
		if len(cols) < 3 {
			t.Errorf("expected at least 3 columns for RETURNING *, got %d: %v", len(cols), cols)
		}

		// Read at least one row
		var hasRow bool
		for rows.Next() {
			hasRow = true
			break
		}
		rows.Close()

		if !hasRow {
			t.Errorf("expected at least 1 returned row for RETURNING *")
		}

		// Verify DMLOptions
		opts := table.GetLastDeleteOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if !opts.Returning {
			t.Errorf("expected Returning=true, got false")
		}
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)
	})

	t.Run("ReturningFalse", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
		})

		// Execute DELETE without RETURNING
		_, err := db.Exec(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id = 1",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify DMLOptions received by table
		opts := table.GetLastDeleteOpts()
		if opts == nil {
			t.Fatal("expected DMLOptions to be non-nil")
		}
		if opts.Returning {
			t.Errorf("expected Returning=false, got true")
		}
		if len(opts.ReturningColumns) != 0 {
			t.Errorf("expected ReturningColumns to be empty when Returning=false, got %v", opts.ReturningColumns)
		}
		t.Logf("DMLOptions: Returning=%v, ReturningColumns=%v", opts.Returning, opts.ReturningColumns)
	})
}

// TestDMLUpdate tests UPDATE operations using DuckDB SQL.
func TestDMLUpdate(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("UpdateSingleRow", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
		})

		// Update via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(
			"UPDATE %s.dml_schema.users SET name = 'Alice Updated' WHERE id = 1",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}

		// Verify via SELECT
		var name string
		err = db.QueryRow(fmt.Sprintf("SELECT name FROM %s.dml_schema.users WHERE id = 1", attachName)).Scan(&name)
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}

		if name != "Alice Updated" {
			t.Errorf("expected 'Alice Updated', got '%s'", name)
		}
	})

	t.Run("UpdateMultipleRows", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// Update multiple rows
		_, err := db.Exec(fmt.Sprintf(
			"UPDATE %s.dml_schema.users SET email = 'updated@example.com' WHERE id IN (1, 2)",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}

		// Verify updates
		var count int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE email = 'updated@example.com'",
			attachName,
		)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 2 {
			t.Errorf("expected 2 updated rows, got %d", count)
		}
	})

	t.Run("UpdateLargeDataset", func(t *testing.T) {
		table.Clear()

		const rowCount = 100_000 // 100K rows

		// First, insert test data using SQL
		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i as id,
				'user_' || i as name,
				'user_' || i || '@example.com' as email
			FROM generate_series(1, %d) as t(i)
		`, attachName, rowCount))
		if err != nil {
			t.Fatalf("INSERT for UPDATE test failed: %v", err)
		}

		if table.RowCount() != rowCount {
			t.Fatalf("expected %d rows after INSERT, got %d", rowCount, table.RowCount())
		}

		// Update all rows - change email domain
		_, err = db.Exec(fmt.Sprintf(
			"UPDATE %s.dml_schema.users SET email = 'user_' || id || '@updated.com' WHERE id > 0",
			attachName,
		))
		if err != nil {
			t.Fatalf("UPDATE large dataset failed: %v", err)
		}

		// Verify all rows were updated
		var count int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE email LIKE '%%@updated.com'",
			attachName,
		)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != rowCount {
			t.Errorf("expected %d updated rows, got %d", rowCount, count)
		}

		t.Logf("Successfully updated %d rows", count)
	})
}

// TestDMLDelete tests DELETE operations using DuckDB SQL.
func TestDMLDelete(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("DeleteSingleRow", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// Delete via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id = 2",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify count
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 2 {
			t.Errorf("expected 2 rows remaining, got %d", count)
		}
	})

	t.Run("DeleteMultipleRows", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
			{int64(3), int64(3), "Charlie", "charlie@example.com"},
		})

		// Delete multiple rows
		_, err := db.Exec(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id IN (1, 3)",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify only Bob remains
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 1 {
			t.Errorf("expected 1 row remaining, got %d", count)
		}
	})

	t.Run("DeleteAllRows", func(t *testing.T) {
		table.Clear()
		table.SeedData([][]any{
			{int64(1), int64(1), "Alice", "alice@example.com"},
			{int64(2), int64(2), "Bob", "bob@example.com"},
		})

		// Delete all rows
		_, err := db.Exec(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id > 0",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify empty
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}

		if count != 0 {
			t.Errorf("expected 0 rows remaining, got %d", count)
		}
	})

	t.Run("DeleteLargeDataset", func(t *testing.T) {
		table.Clear()

		const rowCount = 100_000 // 100K rows

		// First, insert test data using SQL
		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i as id,
				'user_' || i as name,
				'user_' || i || '@example.com' as email
			FROM generate_series(1, %d) as t(i)
		`, attachName, rowCount))
		if err != nil {
			t.Fatalf("INSERT for DELETE test failed: %v", err)
		}

		if table.RowCount() != rowCount {
			t.Fatalf("expected %d rows after INSERT, got %d", rowCount, table.RowCount())
		}

		// Delete all rows
		_, err = db.Exec(fmt.Sprintf(
			"DELETE FROM %s.dml_schema.users WHERE id > 0",
			attachName,
		))
		if err != nil {
			t.Fatalf("DELETE large dataset failed: %v", err)
		}

		// Verify all rows were deleted
		if table.RowCount() != 0 {
			t.Errorf("expected 0 rows after DELETE, got %d", table.RowCount())
		}

		t.Logf("Successfully deleted %d rows", rowCount)
	})
}

// TestDMLFullCRUDCycle tests a complete CRUD cycle using DuckDB SQL.
func TestDMLFullCRUDCycle(t *testing.T) {
	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// 1. INSERT - Create initial data
	_, err := db.Exec(fmt.Sprintf(
		"INSERT INTO %s.dml_schema.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
		attachName,
	))
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// 2. Verify INSERT with SELECT
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT failed: %v", err)
	}
	if count != 2 {
		t.Errorf("after INSERT: expected 2 rows, got %d", count)
	}

	// 3. UPDATE - Modify data
	_, err = db.Exec(fmt.Sprintf(
		"UPDATE %s.dml_schema.users SET name = 'Alice Updated', email = 'alice.new@example.com' WHERE id = 1",
		attachName,
	))
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// 4. Verify UPDATE with SELECT
	var name, email string
	err = db.QueryRow(fmt.Sprintf("SELECT name, email FROM %s.dml_schema.users WHERE id = 1", attachName)).Scan(&name, &email)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "Alice Updated" {
		t.Errorf("after UPDATE: expected name='Alice Updated', got '%s'", name)
	}
	if email != "alice.new@example.com" {
		t.Errorf("after UPDATE: expected email='alice.new@example.com', got '%s'", email)
	}

	// 5. DELETE - Remove data
	_, err = db.Exec(fmt.Sprintf(
		"DELETE FROM %s.dml_schema.users WHERE id = 2",
		attachName,
	))
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// 6. Verify DELETE with SELECT
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT failed: %v", err)
	}
	if count != 1 {
		t.Errorf("after DELETE: expected 1 row, got %d", count)
	}

	// 7. Verify only Alice remains
	err = db.QueryRow(fmt.Sprintf("SELECT name FROM %s.dml_schema.users", attachName)).Scan(&name)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "Alice Updated" {
		t.Errorf("expected 'Alice Updated' to remain, got '%s'", name)
	}
}

// TestDMLMergeInsertUpdate tests MERGE statements that INSERT new rows and UPDATE existing rows.
// MERGE is a powerful SQL statement that combines INSERT, UPDATE, and DELETE based on conditions.
//
// NOTE: DuckDB's Airport extension does not currently support MERGE INTO or ON CONFLICT statements.
// These tests are skipped until the Airport extension adds MERGE support.
// See: https://airport.query.farm/ for extension capabilities.
func TestDMLMergeInsertUpdate(t *testing.T) {
	t.Skip("MERGE INTO not supported by DuckDB Airport extension")

	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("MergeInsertUpdate10Rows", func(t *testing.T) {
		table.Clear()

		// Seed with 5 existing rows (IDs 1-5)
		table.SeedData([][]any{
			{int64(1), int64(1), "user_1", "user_1@old.com"},
			{int64(2), int64(2), "user_2", "user_2@old.com"},
			{int64(3), int64(3), "user_3", "user_3@old.com"},
			{int64(4), int64(4), "user_4", "user_4@old.com"},
			{int64(5), int64(5), "user_5", "user_5@old.com"},
		})

		// MERGE: update existing rows (1-5) and insert new rows (6-10)
		// Source data: IDs 1-10, which will UPDATE 5 rows and INSERT 5 rows
		_, err := db.Exec(fmt.Sprintf(`
			MERGE INTO %s.dml_schema.users AS target
			USING (
				SELECT i AS id, 'user_' || i AS name, 'user_' || i || '@new.com' AS email
				FROM generate_series(1, 10) AS t(i)
			) AS source
			ON target.id = source.id
			WHEN MATCHED THEN
				UPDATE SET name = source.name, email = source.email
			WHEN NOT MATCHED THEN
				INSERT (id, name, email) VALUES (source.id, source.name, source.email)
		`, attachName))
		if err != nil {
			t.Fatalf("MERGE failed: %v", err)
		}

		// Verify total row count is 10
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if count != 10 {
			t.Errorf("expected 10 rows after MERGE, got %d", count)
		}

		// Verify all rows have new email domain
		var newEmailCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE email LIKE '%%@new.com'",
			attachName,
		)).Scan(&newEmailCount)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if newEmailCount != 10 {
			t.Errorf("expected 10 rows with @new.com email, got %d", newEmailCount)
		}

		t.Logf("Successfully merged 10 rows (5 updated, 5 inserted)")
	})

	t.Run("MergeInsertUpdate100kRows", func(t *testing.T) {
		table.Clear()

		const existingRows = 50_000
		const sourceRows = 100_000

		// Seed with 50k existing rows (IDs 1-50000)
		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i AS id,
				'user_' || i AS name,
				'user_' || i || '@old.com' AS email
			FROM generate_series(1, %d) AS t(i)
		`, attachName, existingRows))
		if err != nil {
			t.Fatalf("INSERT for MERGE test failed: %v", err)
		}

		// MERGE: update 50k existing rows and insert 50k new rows
		_, err = db.Exec(fmt.Sprintf(`
			MERGE INTO %s.dml_schema.users AS target
			USING (
				SELECT i AS id, 'user_' || i AS name, 'user_' || i || '@new.com' AS email
				FROM generate_series(1, %d) AS t(i)
			) AS source
			ON target.id = source.id
			WHEN MATCHED THEN
				UPDATE SET name = source.name, email = source.email
			WHEN NOT MATCHED THEN
				INSERT (id, name, email) VALUES (source.id, source.name, source.email)
		`, attachName, sourceRows))
		if err != nil {
			t.Fatalf("MERGE failed: %v", err)
		}

		// Verify total row count is 100k
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if count != sourceRows {
			t.Errorf("expected %d rows after MERGE, got %d", sourceRows, count)
		}

		// Verify all rows have new email domain
		var newEmailCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE email LIKE '%%@new.com'",
			attachName,
		)).Scan(&newEmailCount)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if newEmailCount != sourceRows {
			t.Errorf("expected %d rows with @new.com email, got %d", sourceRows, newEmailCount)
		}

		t.Logf("Successfully merged %d rows (%d updated, %d inserted)", sourceRows, existingRows, sourceRows-existingRows)
	})
}

// TestDMLMergeInsertDelete tests MERGE statements that INSERT new rows and DELETE existing rows.
//
// NOTE: DuckDB's Airport extension does not currently support MERGE INTO or ON CONFLICT statements.
// These tests are skipped until the Airport extension adds MERGE support.
// See: https://airport.query.farm/ for extension capabilities.
func TestDMLMergeInsertDelete(t *testing.T) {
	t.Skip("MERGE INTO not supported by DuckDB Airport extension")

	table := newDuckDBDMLTable(dmlSchemaWithRowID())
	cat := duckDBDMLCatalog(t, table)
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("MergeInsertDelete10Rows", func(t *testing.T) {
		table.Clear()

		// Seed with 10 existing rows (IDs 1-10)
		table.SeedData([][]any{
			{int64(1), int64(1), "user_1", "user_1@example.com"},
			{int64(2), int64(2), "user_2", "user_2@example.com"},
			{int64(3), int64(3), "user_3", "user_3@example.com"},
			{int64(4), int64(4), "user_4", "user_4@example.com"},
			{int64(5), int64(5), "user_5", "user_5@example.com"},
			{int64(6), int64(6), "user_6", "user_6@example.com"},
			{int64(7), int64(7), "user_7", "user_7@example.com"},
			{int64(8), int64(8), "user_8", "user_8@example.com"},
			{int64(9), int64(9), "user_9", "user_9@example.com"},
			{int64(10), int64(10), "user_10", "user_10@example.com"},
		})

		// MERGE: delete rows with even IDs (matched), insert new rows 11-15 (not matched)
		// Source: IDs 2,4,6,8,10 (to delete) and 11,12,13,14,15 (to insert)
		_, err := db.Exec(fmt.Sprintf(`
			MERGE INTO %s.dml_schema.users AS target
			USING (
				SELECT * FROM (
					SELECT i AS id, 'user_' || i AS name, 'user_' || i || '@new.com' AS email, 1 AS to_delete
					FROM generate_series(2, 10, 2) AS t(i)
					UNION ALL
					SELECT i AS id, 'user_' || i AS name, 'user_' || i || '@new.com' AS email, 0 AS to_delete
					FROM generate_series(11, 15) AS t(i)
				)
			) AS source
			ON target.id = source.id
			WHEN MATCHED AND source.to_delete = 1 THEN
				DELETE
			WHEN NOT MATCHED THEN
				INSERT (id, name, email) VALUES (source.id, source.name, source.email)
		`, attachName))
		if err != nil {
			t.Fatalf("MERGE failed: %v", err)
		}

		// Verify: started with 10, deleted 5 (even IDs), inserted 5 = 10 rows
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if count != 10 {
			t.Errorf("expected 10 rows after MERGE, got %d", count)
		}

		// Verify no even IDs (2,4,6,8,10) remain
		var evenCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE id IN (2, 4, 6, 8, 10)",
			attachName,
		)).Scan(&evenCount)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if evenCount != 0 {
			t.Errorf("expected 0 even ID rows after MERGE delete, got %d", evenCount)
		}

		// Verify new rows (11-15) were inserted
		var newRowCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE id >= 11 AND id <= 15",
			attachName,
		)).Scan(&newRowCount)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if newRowCount != 5 {
			t.Errorf("expected 5 new rows (11-15) after MERGE insert, got %d", newRowCount)
		}

		t.Logf("Successfully merged 10 rows (5 deleted, 5 inserted)")
	})

	t.Run("MergeInsertDelete100kRows", func(t *testing.T) {
		table.Clear()

		const existingRows = 100_000
		const rowsToDelete = 50_000 // Delete even IDs
		const rowsToInsert = 50_000 // Insert IDs 100001-150000

		// Seed with 100k existing rows (IDs 1-100000)
		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO %s.dml_schema.users (id, name, email)
			SELECT
				i AS id,
				'user_' || i AS name,
				'user_' || i || '@example.com' AS email
			FROM generate_series(1, %d) AS t(i)
		`, attachName, existingRows))
		if err != nil {
			t.Fatalf("INSERT for MERGE test failed: %v", err)
		}

		// MERGE: delete rows with even IDs (50k), insert new rows 100001-150000 (50k)
		_, err = db.Exec(fmt.Sprintf(`
			MERGE INTO %s.dml_schema.users AS target
			USING (
				SELECT * FROM (
					-- Rows to delete: even IDs from 2 to 100000
					SELECT i AS id, 'user_' || i AS name, 'user_' || i || '@example.com' AS email, 1 AS to_delete
					FROM generate_series(2, %d, 2) AS t(i)
					UNION ALL
					-- Rows to insert: IDs 100001 to 150000
					SELECT i AS id, 'user_' || i AS name, 'user_' || i || '@new.com' AS email, 0 AS to_delete
					FROM generate_series(%d, %d) AS t(i)
				)
			) AS source
			ON target.id = source.id
			WHEN MATCHED AND source.to_delete = 1 THEN
				DELETE
			WHEN NOT MATCHED THEN
				INSERT (id, name, email) VALUES (source.id, source.name, source.email)
		`, attachName, existingRows, existingRows+1, existingRows+rowsToInsert))
		if err != nil {
			t.Fatalf("MERGE failed: %v", err)
		}

		// Verify: started with 100k, deleted 50k (even IDs), inserted 50k = 100k rows
		var count int64
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.dml_schema.users", attachName)).Scan(&count)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		expectedCount := existingRows - rowsToDelete + rowsToInsert
		if count != int64(expectedCount) {
			t.Errorf("expected %d rows after MERGE, got %d", expectedCount, count)
		}

		// Verify no even IDs in original range remain
		var evenCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE id <= %d AND id %% 2 = 0",
			attachName, existingRows,
		)).Scan(&evenCount)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if evenCount != 0 {
			t.Errorf("expected 0 even ID rows after MERGE delete, got %d", evenCount)
		}

		// Verify new rows were inserted
		var newRowCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.dml_schema.users WHERE id > %d",
			attachName, existingRows,
		)).Scan(&newRowCount)
		if err != nil {
			t.Fatalf("SELECT COUNT failed: %v", err)
		}
		if newRowCount != int64(rowsToInsert) {
			t.Errorf("expected %d new rows after MERGE insert, got %d", rowsToInsert, newRowCount)
		}

		t.Logf("Successfully merged %d rows (%d deleted, %d inserted)", rowsToDelete+rowsToInsert, rowsToDelete, rowsToInsert)
	})
}

// =============================================================================
// Test Infrastructure for DuckDB-based DML Testing
// =============================================================================

// dmlSchemaWithRowID creates a schema with rowid pseudocolumn for DML operations.
// The rowid column has is_rowid metadata which enables UPDATE/DELETE via DuckDB.
// Note: All fields are marked as nullable to match DuckDB's input schema for RETURNING.
// DuckDB sends all columns as nullable when executing DML operations with RETURNING.
func dmlSchemaWithRowID() *arrow.Schema {
	rowidMeta := arrow.NewMetadata([]string{"is_rowid"}, []string{"true"})
	return arrow.NewSchema([]arrow.Field{
		{Name: "rowid", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: rowidMeta},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// duckDBDMLTable is an in-memory table that supports DML via DuckDB.
// It exposes a rowid pseudocolumn which is required for UPDATE/DELETE.
type duckDBDMLTable struct {
	tableName       string
	schema          *arrow.Schema
	alloc           memory.Allocator
	mu              sync.RWMutex
	data            [][]any // Each row: [rowid, id, name, email]
	nextRowID       int64
	enableReturning bool // When true, DML operations return affected rows

	// Track last received DMLOptions for testing
	lastInsertOpts *catalog.DMLOptions
	lastUpdateOpts *catalog.DMLOptions
	lastDeleteOpts *catalog.DMLOptions
}

func newDuckDBDMLTable(schema *arrow.Schema) *duckDBDMLTable {
	return &duckDBDMLTable{
		tableName: "users",
		schema:    schema,
		alloc:     memory.DefaultAllocator,
		data:      make([][]any, 0),
		nextRowID: 1,
	}
}

// EnableReturning enables RETURNING clause support for this table.
func (t *duckDBDMLTable) EnableReturning() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enableReturning = true
}

func (t *duckDBDMLTable) Name() string    { return t.tableName }
func (t *duckDBDMLTable) Comment() string { return "In-memory DML table with rowid" }
func (t *duckDBDMLTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *duckDBDMLTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.data) == 0 {
		return array.NewRecordReader(t.schema, nil)
	}

	record := buildTestRecord(t.schema, t.convertData())
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

func (t *duckDBDMLTable) convertData() [][]any {
	result := make([][]any, len(t.data))
	for i, row := range t.data {
		result[i] = make([]any, len(row))
		copy(result[i], row)
	}
	return result
}

func (t *duckDBDMLTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	start := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	// Track received DMLOptions for testing
	t.lastInsertOpts = opts

	inputSchema := rows.Schema()
	var insertedRows [][]any // For RETURNING support
	var totalRows int64
	var batchCount int

	for rows.Next() {
		batch := rows.RecordBatch()
		batchCount++
		for rowIdx := int64(0); rowIdx < batch.NumRows(); rowIdx++ {
			// Assign rowid and extract other values
			// Row format: [rowid, col1, col2, ...] where cols come from input in order
			row := make([]any, batch.NumCols()+1)
			row[0] = t.nextRowID // rowid

			for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
				col := batch.Column(colIdx)
				val := extractValue(col, int(rowIdx))
				// If column is 'id' and value is nil, auto-generate
				if val == nil && inputSchema.Field(colIdx).Name == "id" {
					row[colIdx+1] = t.nextRowID // auto-generate id
				} else {
					row[colIdx+1] = val
				}
			}
			t.data = append(t.data, row)

			// Track inserted rows for RETURNING
			if t.enableReturning {
				insertedRows = append(insertedRows, row)
			}

			t.nextRowID++
			totalRows++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	elapsed := time.Since(start)
	fmt.Printf("[DEBUG] INSERT completed: %d rows in %d batches, duration=%v (%.0f rows/sec)\n",
		totalRows, batchCount, elapsed, float64(totalRows)/elapsed.Seconds())

	result := &catalog.DMLResult{AffectedRows: totalRows}

	// Build RETURNING data if enabled and opts.Returning is true
	// Use opts.ReturningColumns to determine which columns to return
	if t.enableReturning && opts != nil && opts.Returning && len(insertedRows) > 0 {
		// Project schema to ReturningColumns (for now, same as all columns)
		returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
		returningReader, err := t.buildReturningReader(returningSchema, insertedRows)
		if err != nil {
			return nil, fmt.Errorf("failed to build RETURNING data: %w", err)
		}
		result.ReturningData = returningReader
	}

	return result, nil
}

func (t *duckDBDMLTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	start := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	// Track received DMLOptions for testing
	t.lastUpdateOpts = opts

	// Get the input schema to map column names to table positions
	// DuckDB sends only the columns being updated, not all columns
	inputSchema := rows.Schema()

	// Build mapping from input column index to table column index
	// Table schema: [rowid, id, name, email]
	// Input might be: [name] (only the column being updated)
	colMapping := make([]int, inputSchema.NumFields())
	for i := 0; i < inputSchema.NumFields(); i++ {
		inputColName := inputSchema.Field(i).Name
		// Find this column in the table schema (skip rowid at index 0)
		found := false
		for j := 1; j < t.schema.NumFields(); j++ {
			if t.schema.Field(j).Name == inputColName {
				colMapping[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column %s not found in table schema", inputColName)
		}
	}

	// Build map of rowID to column updates (column index -> value)
	type colUpdate struct {
		colIdx int
		value  any
	}
	updates := make(map[int64][]colUpdate)
	rowIdx := 0
	var batchCount int
	for rows.Next() {
		batch := rows.RecordBatch()
		batchCount++
		for batchRowIdx := int64(0); batchRowIdx < batch.NumRows(); batchRowIdx++ {
			if rowIdx >= len(rowIDs) {
				break
			}
			// Extract column updates
			var colUpdates []colUpdate
			for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
				col := batch.Column(colIdx)
				colUpdates = append(colUpdates, colUpdate{
					colIdx: colMapping[colIdx],
					value:  extractValue(col, int(batchRowIdx)),
				})
			}
			updates[rowIDs[rowIdx]] = colUpdates
			rowIdx++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Apply updates and collect affected rows for RETURNING
	affected := int64(0)
	var affectedRows [][]any
	for i := range t.data {
		rowID := t.data[i][0].(int64)
		if colUpdates, ok := updates[rowID]; ok {
			// Apply each column update at the correct position
			for _, cu := range colUpdates {
				t.data[i][cu.colIdx] = cu.value
			}
			// Store affected row for RETURNING
			if t.enableReturning && opts != nil && opts.Returning {
				rowCopy := make([]any, len(t.data[i]))
				copy(rowCopy, t.data[i])
				affectedRows = append(affectedRows, rowCopy)
			}
			affected++
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("[DEBUG] UPDATE completed: %d rows affected in %d batches, duration=%v\n",
		affected, batchCount, elapsed)

	result := &catalog.DMLResult{AffectedRows: affected}

	// Build RETURNING data if enabled and opts.Returning is true
	// Use opts.ReturningColumns to determine which columns to return
	if t.enableReturning && opts != nil && opts.Returning && len(affectedRows) > 0 {
		fmt.Printf("[DEBUG] Building UPDATE RETURNING data for %d rows\n", len(affectedRows))
		// Project schema to ReturningColumns (for now, same as all columns)
		returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
		fmt.Printf("[DEBUG] RETURNING schema: %v\n", returningSchema.String())
		returningReader, err := t.buildReturningReader(returningSchema, affectedRows)
		if err != nil {
			return nil, fmt.Errorf("failed to build RETURNING data: %w", err)
		}
		fmt.Printf("[DEBUG] UPDATE RETURNING reader built successfully\n")
		result.ReturningData = returningReader
	}

	return result, nil
}

func (t *duckDBDMLTable) Delete(ctx context.Context, rowIDs []int64, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	start := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	// Track received DMLOptions for testing
	t.lastDeleteOpts = opts

	// Build set of rowIDs to delete
	deleteSet := make(map[int64]bool)
	for _, id := range rowIDs {
		deleteSet[id] = true
	}

	// Filter out deleted rows and collect them for RETURNING
	newData := make([][]any, 0, len(t.data))
	var deletedRows [][]any
	deleted := int64(0)
	for _, row := range t.data {
		rowID := row[0].(int64)
		if !deleteSet[rowID] {
			newData = append(newData, row)
		} else {
			// Store deleted row for RETURNING
			if t.enableReturning && opts != nil && opts.Returning {
				rowCopy := make([]any, len(row))
				copy(rowCopy, row)
				deletedRows = append(deletedRows, rowCopy)
			}
			deleted++
		}
	}
	t.data = newData

	elapsed := time.Since(start)
	fmt.Printf("[DEBUG] DELETE completed: %d rows deleted, duration=%v\n",
		deleted, elapsed)

	result := &catalog.DMLResult{AffectedRows: deleted}

	// Build RETURNING data if enabled and opts.Returning is true
	// Use opts.ReturningColumns to determine which columns to return
	if t.enableReturning && opts != nil && opts.Returning && len(deletedRows) > 0 {
		// Project schema to ReturningColumns (for now, same as all columns)
		returningSchema := catalog.ProjectSchema(t.schema, opts.ReturningColumns)
		returningReader, err := t.buildReturningReader(returningSchema, deletedRows)
		if err != nil {
			return nil, fmt.Errorf("failed to build RETURNING data: %w", err)
		}
		result.ReturningData = returningReader
	}

	return result, nil
}

func (t *duckDBDMLTable) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = make([][]any, 0)
	t.nextRowID = 1
}

func (t *duckDBDMLTable) RowCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.data)
}

func (t *duckDBDMLTable) SeedData(rows [][]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// rows should include rowid as first column
	t.data = append(t.data, rows...)
	// Update nextRowID to be max(existing rowids) + 1
	maxRowID := int64(0)
	for _, row := range t.data {
		if rowID := row[0].(int64); rowID > maxRowID {
			maxRowID = rowID
		}
	}
	t.nextRowID = maxRowID + 1
}

func (t *duckDBDMLTable) GetData() [][]any {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([][]any, len(t.data))
	copy(result, t.data)
	return result
}

// GetLastInsertOpts returns the last DMLOptions received by Insert.
func (t *duckDBDMLTable) GetLastInsertOpts() *catalog.DMLOptions {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastInsertOpts
}

// GetLastUpdateOpts returns the last DMLOptions received by Update.
func (t *duckDBDMLTable) GetLastUpdateOpts() *catalog.DMLOptions {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastUpdateOpts
}

// GetLastDeleteOpts returns the last DMLOptions received by Delete.
func (t *duckDBDMLTable) GetLastDeleteOpts() *catalog.DMLOptions {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastDeleteOpts
}

// buildReturningReader builds a RecordReader from affected rows for RETURNING clause.
// The inputSchema is the schema of the columns sent by the client (what they want returned).
// The rows contain full table data: [rowid, id, name, email].
func (t *duckDBDMLTable) buildReturningReader(inputSchema *arrow.Schema, rows [][]any) (array.RecordReader, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Build column data for the input schema columns
	// Input schema columns map to table columns (offset by 1 for rowid)
	numCols := inputSchema.NumFields()
	builders := make([]array.Builder, numCols)
	for i := 0; i < numCols; i++ {
		field := inputSchema.Field(i)
		builders[i] = array.NewBuilder(t.alloc, field.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	// Map input column names to table column indices
	colMapping := make([]int, numCols)
	for i := 0; i < numCols; i++ {
		colName := inputSchema.Field(i).Name
		// Find this column in the table schema
		found := false
		for j := 0; j < t.schema.NumFields(); j++ {
			if t.schema.Field(j).Name == colName {
				colMapping[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column %s not found in table schema", colName)
		}
	}

	// Append row values
	for _, row := range rows {
		for i, builder := range builders {
			tableColIdx := colMapping[i]
			value := row[tableColIdx]

			switch b := builder.(type) {
			case *array.Int64Builder:
				if value == nil {
					b.AppendNull()
				} else {
					b.Append(value.(int64))
				}
			case *array.StringBuilder:
				if value == nil {
					b.AppendNull()
				} else {
					b.Append(value.(string))
				}
			default:
				return nil, fmt.Errorf("unsupported builder type for column %s", inputSchema.Field(i).Name)
			}
		}
	}

	// Build arrays and create record
	arrays := make([]arrow.Array, numCols)
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	record := array.NewRecordBatch(inputSchema, arrays, int64(len(rows)))
	for _, arr := range arrays {
		arr.Release()
	}

	return array.NewRecordReader(inputSchema, []arrow.RecordBatch{record})
}

// duckDBDMLCatalog creates a catalog with a DML-capable table for DuckDB testing.
func duckDBDMLCatalog(t *testing.T, table catalog.Table) catalog.Catalog {
	t.Helper()

	cat, err := airport.NewCatalogBuilder().
		Schema("dml_schema").
		Comment("Schema for DML testing via DuckDB").
		Table(table).
		Build()

	if err != nil {
		t.Fatalf("failed to build DML catalog: %v", err)
	}
	return cat
}

// extractValue extracts a Go value from an Arrow array at the given index.
func extractValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch typedArr := arr.(type) {
	case *array.Int64:
		return typedArr.Value(idx)
	case *array.Int32:
		return int64(typedArr.Value(idx))
	case *array.Int16:
		return int64(typedArr.Value(idx))
	case *array.Int8:
		return int64(typedArr.Value(idx))
	case *array.Uint64:
		return int64(typedArr.Value(idx))
	case *array.Uint32:
		return int64(typedArr.Value(idx))
	case *array.Uint16:
		return int64(typedArr.Value(idx))
	case *array.Uint8:
		return int64(typedArr.Value(idx))
	case *array.Float64:
		return typedArr.Value(idx)
	case *array.Float32:
		return float64(typedArr.Value(idx))
	case *array.String:
		return typedArr.Value(idx)
	case *array.Boolean:
		return typedArr.Value(idx)
	case *array.Binary:
		return typedArr.Value(idx)
	default:
		return nil
	}
}
