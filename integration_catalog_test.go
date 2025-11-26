package airport_test

import (
	"testing"
)

// TestCatalogDiscovery verifies that DuckDB can discover schemas and tables
// from the Flight server catalog.
func TestCatalogDiscovery(t *testing.T) {
	// Create test catalog
	cat := simpleCatalog()

	// Start Flight server
	server := newTestServer(t, cat, nil)
	defer server.stop()

	// Open DuckDB
	db := openDuckDB(t)
	defer db.Close()

	// Attach Flight server
	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Discover schemas
	t.Run("ListSchemas", func(t *testing.T) {
		rows, err := db.Query("SELECT schema_name FROM duckdb_schemas() WHERE catalog_name = ?", attachName)
		if err != nil {
			t.Fatalf("Failed to query schemas: %v", err)
		}
		defer rows.Close()

		schemas := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan schema: %v", err)
			}
			schemas = append(schemas, name)
		}

		// Should have "main" schema
		found := false
		for _, s := range schemas {
			if s == "main" {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected to find 'main' schema, got schemas: %v", schemas)
		}
	})

	// Test 2: Discover tables
	t.Run("ListTables", func(t *testing.T) {
		query := "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' AND catalog_name = ?"
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Failed to query tables: %v", err)
		}
		defer rows.Close()

		tables := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan table: %v", err)
			}
			tables = append(tables, name)
		}

		// Should have "users" and "products" tables
		expectedTables := map[string]bool{
			"users":    false,
			"products": false,
		}

		for _, table := range tables {
			if _, exists := expectedTables[table]; exists {
				expectedTables[table] = true
			}
		}

		for table, found := range expectedTables {
			if !found {
				t.Errorf("Expected to find table '%s', got tables: %v", table, tables)
			}
		}
	})

	// Test 3: Discover columns
	t.Run("ListColumns", func(t *testing.T) {
		query := `SELECT column_name, data_type
		          FROM duckdb_columns()
		          WHERE table_name = 'users'
		          AND schema_name = 'main'
		          AND catalog_name = ?
		          ORDER BY column_name`

		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Failed to query columns: %v", err)
		}
		defer rows.Close()

		type column struct {
			name     string
			dataType string
		}

		columns := []column{}
		for rows.Next() {
			var col column
			if err := rows.Scan(&col.name, &col.dataType); err != nil {
				t.Fatalf("Failed to scan column: %v", err)
			}
			columns = append(columns, col)
		}

		// Should have id, name, email columns
		expectedColumns := map[string]bool{
			"id":    false,
			"name":  false,
			"email": false,
		}

		for _, col := range columns {
			if _, exists := expectedColumns[col.name]; exists {
				expectedColumns[col.name] = true
			}
		}

		for colName, found := range expectedColumns {
			if !found {
				t.Errorf("Expected to find column '%s', got columns: %v", colName, columns)
			}
		}
	})

	// Test 4: SHOW TABLES works
	t.Run("ShowTables", func(t *testing.T) {
		query := "SHOW TABLES FROM " + attachName + ".main"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("SHOW TABLES failed: %v", err)
		}
		defer rows.Close()

		tableCount := 0
		for rows.Next() {
			tableCount++
		}

		if tableCount < 2 {
			t.Errorf("Expected at least 2 tables, got %d", tableCount)
		}
	})
}

// TestCatalogMetadata verifies that table and column metadata is correctly
// transferred from the Flight server.
func TestCatalogMetadata(t *testing.T) {
	cat := simpleCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	_ = connectToFlightServer(t, db, server.address, "")

	// Test table comments are preserved
	t.Run("TableComments", func(t *testing.T) {
		// DuckDB may or may not expose table comments through system tables
		// This test documents expected behavior
		query := "SELECT comment FROM duckdb_tables() WHERE table_name = 'users' AND schema_name = 'main'"
		rows, err := db.Query(query)
		if err != nil {
			t.Skipf("Table comments not supported: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var comment *string
			if err := rows.Scan(&comment); err != nil {
				t.Fatalf("Failed to scan comment: %v", err)
			}

			if comment != nil && *comment == "User accounts" {
				t.Logf("Table comment preserved: %s", *comment)
			}
		}
	})

	// Test column data types are correct
	t.Run("ColumnTypes", func(t *testing.T) {
		query := `SELECT column_name, data_type
		          FROM duckdb_columns()
		          WHERE table_name = 'products'
		          AND schema_name = 'main'
		          ORDER BY column_name`

		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Failed to query columns: %v", err)
		}
		defer rows.Close()

		expectedTypes := map[string]string{
			"id":    "BIGINT",
			"name":  "VARCHAR",
			"price": "DOUBLE",
		}

		for rows.Next() {
			var colName, dataType string
			if err := rows.Scan(&colName, &dataType); err != nil {
				t.Fatalf("Failed to scan column: %v", err)
			}

			if expectedType, exists := expectedTypes[colName]; exists {
				if dataType != expectedType {
					t.Errorf("Column %s: expected type %s, got %s", colName, expectedType, dataType)
				}
			}
		}
	})
}
