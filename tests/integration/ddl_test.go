package airport_test

import (
	"fmt"
	"testing"
)

// =============================================================================
// DDL Integration Tests via DuckDB SQL
// =============================================================================
// These tests use DuckDB as a Flight client to execute SQL DDL statements
// (CREATE SCHEMA, DROP SCHEMA, CREATE TABLE, etc.) against the Airport server.
//
// Based on Airport extension protocol:
// - DDL operations use DoAction RPC with snake_case action names
// - create_schema, drop_schema, create_table, drop_table, add_column, remove_column
// - Requests use MessagePack encoding per protocol spec
// =============================================================================

// TestDDLCreateSchema tests CREATE SCHEMA operation via DuckDB SQL.
func TestDDLCreateSchema(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("CreateNewSchema", func(t *testing.T) {
		// Create schema via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.new_schema", attachName))
		if err != nil {
			t.Fatalf("CREATE SCHEMA failed: %v", err)
		}

		// Verify schema was created in mock catalog
		if !mockCat.HasSchema("new_schema") {
			t.Error("schema was not created in catalog")
		}

		// Verify DuckDB sees the new schema via duckdb_schemas() metadata function
		var schemaName string
		err = db.QueryRow(fmt.Sprintf(
			"SELECT schema_name FROM duckdb_schemas() WHERE database_name = '%s' AND schema_name = 'new_schema'",
			attachName,
		)).Scan(&schemaName)
		if err != nil {
			t.Errorf("DuckDB cannot see new schema in duckdb_schemas(): %v", err)
		}
		if schemaName != "new_schema" {
			t.Errorf("expected schema_name 'new_schema', got %q", schemaName)
		}
	})
}

// TestDDLCreateSchemaAlreadyExists tests CREATE SCHEMA when schema already exists.
func TestDDLCreateSchemaAlreadyExists(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	// Pre-create the schema
	mockCat.schemas["existing_schema"] = &mockDynamicSchema{
		name:   "existing_schema",
		tables: make(map[string]*mockDynamicTable),
	}
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Try to create existing schema - should fail
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.existing_schema", attachName))
	if err == nil {
		t.Error("expected error when creating existing schema, got nil")
	}

	// Note: CREATE SCHEMA IF NOT EXISTS is not supported by Airport protocol
	// The protocol doesn't have an on_conflict parameter for create_schema
	// DuckDB sends the same create_schema action regardless of IF NOT EXISTS
}

// TestDDLCreateSchemaOnStaticCatalog tests CREATE SCHEMA on non-dynamic catalog.
func TestDDLCreateSchemaOnStaticCatalog(t *testing.T) {
	// Use simpleCatalog which is NOT a DynamicCatalog
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Try to create schema on static catalog - should fail with Unimplemented
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.new_schema", attachName))
	if err == nil {
		t.Error("expected error when creating schema on static catalog, got nil")
	}
}

// TestDDLDropSchema tests DROP SCHEMA operation via DuckDB SQL.
func TestDDLDropSchema(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	// Create a schema to drop
	mockCat.schemas["schema_to_drop"] = &mockDynamicSchema{
		name:   "schema_to_drop",
		tables: make(map[string]*mockDynamicTable),
	}
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("DropExistingSchema", func(t *testing.T) {
		// Drop schema via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf("DROP SCHEMA %s.schema_to_drop", attachName))
		if err != nil {
			t.Fatalf("DROP SCHEMA failed: %v", err)
		}

		// Verify schema was dropped in mock catalog
		if mockCat.HasSchema("schema_to_drop") {
			t.Error("schema was not dropped from catalog")
		}

		// Verify DuckDB no longer sees the schema via duckdb_schemas() metadata function
		var count int
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = '%s' AND schema_name = 'schema_to_drop'",
			attachName,
		)).Scan(&count)
		if err != nil {
			t.Errorf("DuckDB query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("DuckDB still sees dropped schema in duckdb_schemas(), count = %d", count)
		}
	})
}

// TestDDLDropSchemaNotFound tests DROP SCHEMA when schema doesn't exist.
func TestDDLDropSchemaNotFound(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Drop non-existent schema - should fail
	_, err := db.Exec(fmt.Sprintf("DROP SCHEMA %s.nonexistent", attachName))
	if err == nil {
		t.Error("expected error when dropping non-existent schema, got nil")
	}

	// DROP SCHEMA IF EXISTS should succeed
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s.nonexistent", attachName))
	if err != nil {
		t.Errorf("DROP SCHEMA IF EXISTS failed: %v", err)
	}
}

// TestDDLDropSchemaWithTables tests DROP SCHEMA on schema containing tables.
func TestDDLDropSchemaWithTables(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	// Create schema with a table
	schema := &mockDynamicSchema{
		name:   "schema_with_tables",
		tables: make(map[string]*mockDynamicTable),
	}
	schema.tables["some_table"] = &mockDynamicTable{
		name: "some_table",
	}
	mockCat.schemas["schema_with_tables"] = schema
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Drop schema with tables - should fail (FR-016)
	_, err := db.Exec(fmt.Sprintf("DROP SCHEMA %s.schema_with_tables", attachName))
	if err == nil {
		t.Error("expected error when dropping schema with tables, got nil")
	}

	// Schema should still exist
	if !mockCat.HasSchema("schema_with_tables") {
		t.Error("schema should not have been dropped")
	}
}

// TestDDLCreateTable tests CREATE TABLE operation via DuckDB SQL.
// Note: DuckDB validates schema existence locally before sending DDL.
// To test CREATE TABLE, we first need to create the schema via DDL.
func TestDDLCreateTable(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("CreateSimpleTable", func(t *testing.T) {
		// First create the schema
		_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
		if err != nil {
			t.Fatalf("CREATE SCHEMA failed: %v", err)
		}

		// Create table via DuckDB SQL
		_, err = db.Exec(fmt.Sprintf(`
			CREATE TABLE %s.test_schema.users (
				id INTEGER,
				name VARCHAR,
				email VARCHAR
			)
		`, attachName))
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}

		// Verify table was created in mock catalog
		schema := mockCat.GetSchema("test_schema")
		if schema == nil {
			t.Fatal("test_schema not found in catalog")
		}
		if !schema.HasTable("users") {
			t.Error("table was not created in catalog")
		}

		// Verify schema has correct columns in mock catalog
		table := schema.GetTable("users")
		if table == nil {
			t.Fatal("users table not found in catalog")
		}
		if table.ColumnCount() != 3 {
			t.Errorf("expected 3 columns in catalog, got %d", table.ColumnCount())
		}

		// Verify DuckDB sees the new table via duckdb_tables() metadata function
		var tableName string
		var columnCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT table_name, column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'users'",
			attachName,
		)).Scan(&tableName, &columnCount)
		if err != nil {
			t.Errorf("DuckDB cannot see new table in duckdb_tables(): %v", err)
		}
		if tableName != "users" {
			t.Errorf("expected table_name 'users', got %q", tableName)
		}
		if columnCount != 3 {
			t.Errorf("DuckDB shows %d columns in duckdb_tables(), expected 3", columnCount)
		}
	})
}

// TestDDLCreateTableOnConflictIgnore tests CREATE TABLE IF NOT EXISTS.
func TestDDLCreateTableOnConflictIgnore(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create the schema
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Create table first time
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.test_table (id INTEGER)
	`, attachName))
	if err != nil {
		t.Fatalf("First CREATE TABLE failed: %v", err)
	}

	// Create same table without IF NOT EXISTS - should fail
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.test_table (id INTEGER)
	`, attachName))
	if err == nil {
		t.Error("expected error when creating duplicate table, got nil")
	}

	// Note: CREATE TABLE IF NOT EXISTS maps to on_conflict=ignore per protocol
	// but behavior depends on DuckDB's local catalog validation
}

// TestDDLCreateTableOnConflictReplace tests CREATE OR REPLACE TABLE.
// Note: DuckDB's Airport extension does not currently support CREATE OR REPLACE TABLE.
// The protocol defines on_conflict=replace, but DuckDB returns "Not implemented Error".
// This test documents the server-side support for when DuckDB adds this feature.
func TestDDLCreateTableOnConflictReplace(t *testing.T) {
	t.Skip("DuckDB Airport extension does not support CREATE OR REPLACE TABLE (returns 'Not implemented Error: REPLACE ON CONFLICT in CreateTable')")

	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create the schema
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Create table first time with 2 columns
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.replace_test (id INTEGER, name VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("First CREATE TABLE failed: %v", err)
	}

	// Verify 2 columns
	schema := mockCat.GetSchema("test_schema")
	table := schema.GetTable("replace_test")
	if table.ColumnCount() != 2 {
		t.Errorf("expected 2 columns initially, got %d", table.ColumnCount())
	}

	// Create OR REPLACE with 3 columns
	_, err = db.Exec(fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.test_schema.replace_test (id INTEGER, name VARCHAR, email VARCHAR)
	`, attachName))
	if err != nil {
		t.Errorf("CREATE OR REPLACE TABLE failed: %v", err)
	}

	// Verify 3 columns after replace
	table = schema.GetTable("replace_test")
	if table.ColumnCount() != 3 {
		t.Errorf("expected 3 columns after replace, got %d", table.ColumnCount())
	}
}

// TestDDLDropTable tests DROP TABLE operation via DuckDB SQL.
func TestDDLDropTable(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("DropExistingTable", func(t *testing.T) {
		// First create a schema and table
		_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
		if err != nil {
			t.Fatalf("CREATE SCHEMA failed: %v", err)
		}

		_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s.test_schema.table_to_drop (id INTEGER)", attachName))
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}

		schema := mockCat.GetSchema("test_schema")
		if !schema.HasTable("table_to_drop") {
			t.Fatal("table was not created in catalog")
		}

		// Drop table via DuckDB SQL
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s.test_schema.table_to_drop", attachName))
		if err != nil {
			t.Fatalf("DROP TABLE failed: %v", err)
		}

		// Verify table was dropped in mock catalog
		if schema.HasTable("table_to_drop") {
			t.Error("table was not dropped from catalog")
		}

		// Verify DuckDB no longer sees the table via duckdb_tables() metadata function
		var count int
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'table_to_drop'",
			attachName,
		)).Scan(&count)
		if err != nil {
			t.Errorf("DuckDB query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("DuckDB still sees dropped table in duckdb_tables(), count = %d", count)
		}
	})
}

// TestDDLDropTableNotFound tests DROP TABLE when table doesn't exist.
func TestDDLDropTableNotFound(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create the schema
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Drop non-existent table - should fail
	// Note: DuckDB validates table existence locally, so it may fail before sending
	// the drop_table action to the server
	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s.test_schema.nonexistent", attachName))
	if err == nil {
		t.Error("expected error when dropping non-existent table, got nil")
	}

	// DROP TABLE IF EXISTS should succeed
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s.test_schema.nonexistent", attachName))
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS failed: %v", err)
	}
}

// TestDDLAddColumn tests ALTER TABLE ADD COLUMN via DuckDB SQL.
func TestDDLAddColumn(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and table
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.alter_test (id INTEGER, name VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("AddNewColumn", func(t *testing.T) {
		// Add column via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(`
			ALTER TABLE %s.test_schema.alter_test ADD COLUMN email VARCHAR
		`, attachName))
		if err != nil {
			t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
		}

		// Verify column was added in mock catalog
		schema := mockCat.GetSchema("test_schema")
		table := schema.GetTable("alter_test")
		if !table.HasColumn("email") {
			t.Error("column was not added to catalog")
		}
		if table.ColumnCount() != 3 {
			t.Errorf("expected 3 columns in catalog, got %d", table.ColumnCount())
		}

		// Verify DuckDB sees the new column via duckdb_columns() metadata function
		var columnName string
		err = db.QueryRow(fmt.Sprintf(
			"SELECT column_name FROM duckdb_columns() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'alter_test' AND column_name = 'email'",
			attachName,
		)).Scan(&columnName)
		if err != nil {
			t.Errorf("DuckDB cannot see new column 'email' in duckdb_columns(): %v", err)
		}
		if columnName != "email" {
			t.Errorf("expected column_name 'email', got %q", columnName)
		}

		// Verify DuckDB sees 3 columns total via duckdb_tables()
		var columnCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'alter_test'",
			attachName,
		)).Scan(&columnCount)
		if err != nil {
			t.Errorf("DuckDB duckdb_tables() query failed: %v", err)
		}
		if columnCount != 3 {
			t.Errorf("DuckDB shows %d columns in duckdb_tables(), expected 3", columnCount)
		}
	})
}

// TestDDLAddColumnAlreadyExists tests ADD COLUMN when column already exists.
func TestDDLAddColumnAlreadyExists(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and table with 'name' column
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.dup_col_test (id INTEGER, name VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Add duplicate column - should fail
	_, err = db.Exec(fmt.Sprintf(`
		ALTER TABLE %s.test_schema.dup_col_test ADD COLUMN name VARCHAR
	`, attachName))
	if err == nil {
		t.Error("expected error when adding duplicate column, got nil")
	}

	// Note: ADD COLUMN IF NOT EXISTS behavior depends on DuckDB's local validation
}

// TestDDLRemoveColumn tests ALTER TABLE DROP COLUMN via DuckDB SQL.
func TestDDLRemoveColumn(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and table
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.drop_col_test (id INTEGER, name VARCHAR, email VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("RemoveExistingColumn", func(t *testing.T) {
		// Drop column via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(`
			ALTER TABLE %s.test_schema.drop_col_test DROP COLUMN email
		`, attachName))
		if err != nil {
			t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
		}

		// Verify column was removed in mock catalog
		schema := mockCat.GetSchema("test_schema")
		table := schema.GetTable("drop_col_test")
		if table.HasColumn("email") {
			t.Error("column was not removed from catalog")
		}
		if table.ColumnCount() != 2 {
			t.Errorf("expected 2 columns in catalog, got %d", table.ColumnCount())
		}

		// Verify DuckDB no longer sees the column via duckdb_columns() metadata function
		var count int
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM duckdb_columns() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'drop_col_test' AND column_name = 'email'",
			attachName,
		)).Scan(&count)
		if err != nil {
			t.Errorf("DuckDB query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("DuckDB still sees dropped column 'email' in duckdb_columns(), count = %d", count)
		}

		// Verify DuckDB sees 2 columns total via duckdb_tables()
		var columnCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'drop_col_test'",
			attachName,
		)).Scan(&columnCount)
		if err != nil {
			t.Errorf("DuckDB duckdb_tables() query failed: %v", err)
		}
		if columnCount != 2 {
			t.Errorf("DuckDB shows %d columns in duckdb_tables(), expected 2", columnCount)
		}
	})
}

// TestDDLRemoveColumnNotFound tests DROP COLUMN when column doesn't exist.
func TestDDLRemoveColumnNotFound(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and table
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.no_col_test (id INTEGER)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Drop non-existent column - should fail
	_, err = db.Exec(fmt.Sprintf(`
		ALTER TABLE %s.test_schema.no_col_test DROP COLUMN nonexistent
	`, attachName))
	if err == nil {
		t.Error("expected error when dropping non-existent column, got nil")
	}

	// Note: DROP COLUMN IF EXISTS behavior depends on DuckDB's local validation
}

// TestDDLFullLifecycle tests complete DDL lifecycle operations.
func TestDDLFullLifecycle(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Step 1: Create schema
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.lifecycle_test", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}
	if !mockCat.HasSchema("lifecycle_test") {
		t.Fatal("schema was not created in catalog")
	}

	// DuckDB check: schema should be visible via duckdb_schemas()
	var schemaCount int
	_ = db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = '%s' AND schema_name = 'lifecycle_test'",
		attachName,
	)).Scan(&schemaCount)
	if schemaCount != 1 {
		t.Fatalf("DuckDB cannot see created schema in duckdb_schemas()")
	}

	// Step 2: Create table
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.lifecycle_test.users (id INTEGER, name VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	schema := mockCat.GetSchema("lifecycle_test")
	if !schema.HasTable("users") {
		t.Fatal("table was not created in catalog")
	}

	// DuckDB check: table should be visible via duckdb_tables() with 2 columns
	var columnCount int64
	_ = db.QueryRow(fmt.Sprintf(
		"SELECT column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'lifecycle_test' AND table_name = 'users'",
		attachName,
	)).Scan(&columnCount)
	if columnCount != 2 {
		t.Fatalf("DuckDB shows %d columns in duckdb_tables(), expected 2", columnCount)
	}

	// Step 3: Add column
	_, err = db.Exec(fmt.Sprintf(`
		ALTER TABLE %s.lifecycle_test.users ADD COLUMN email VARCHAR
	`, attachName))
	if err != nil {
		t.Fatalf("ADD COLUMN failed: %v", err)
	}
	table := schema.GetTable("users")
	if !table.HasColumn("email") {
		t.Fatal("column was not added to catalog")
	}

	// DuckDB check: table should now have 3 columns via duckdb_tables()
	_ = db.QueryRow(fmt.Sprintf(
		"SELECT column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'lifecycle_test' AND table_name = 'users'",
		attachName,
	)).Scan(&columnCount)
	if columnCount != 3 {
		t.Fatalf("DuckDB shows %d columns after add in duckdb_tables(), expected 3", columnCount)
	}

	// Step 4: Remove column
	_, err = db.Exec(fmt.Sprintf(`
		ALTER TABLE %s.lifecycle_test.users DROP COLUMN email
	`, attachName))
	if err != nil {
		t.Fatalf("DROP COLUMN failed: %v", err)
	}
	if table.HasColumn("email") {
		t.Fatal("column was not removed from catalog")
	}

	// DuckDB check: table should now have 2 columns via duckdb_tables()
	_ = db.QueryRow(fmt.Sprintf(
		"SELECT column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'lifecycle_test' AND table_name = 'users'",
		attachName,
	)).Scan(&columnCount)
	if columnCount != 2 {
		t.Fatalf("DuckDB shows %d columns after drop in duckdb_tables(), expected 2", columnCount)
	}

	// Step 5: Drop table
	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s.lifecycle_test.users", attachName))
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
	if schema.HasTable("users") {
		t.Fatal("table was not dropped from catalog")
	}

	// DuckDB check: table should no longer be visible via duckdb_tables()
	var tableCount int
	_ = db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'lifecycle_test' AND table_name = 'users'",
		attachName,
	)).Scan(&tableCount)
	if tableCount != 0 {
		t.Fatal("DuckDB still sees dropped table in duckdb_tables()")
	}

	// Step 6: Drop schema
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA %s.lifecycle_test", attachName))
	if err != nil {
		t.Fatalf("DROP SCHEMA failed: %v", err)
	}
	if mockCat.HasSchema("lifecycle_test") {
		t.Fatal("schema was not dropped from catalog")
	}

	// DuckDB check: schema should no longer be visible via duckdb_schemas()
	_ = db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = '%s' AND schema_name = 'lifecycle_test'",
		attachName,
	)).Scan(&schemaCount)
	if schemaCount != 0 {
		t.Fatal("DuckDB still sees dropped schema in duckdb_schemas()")
	}

	t.Log("Full DDL lifecycle completed successfully with DuckDB metadata verification")
}

// =============================================================================
// Additional DDL Tests for Extended Operations
// =============================================================================

// TestDDLRenameColumn tests ALTER TABLE RENAME COLUMN operation via DuckDB SQL.
func TestDDLRenameColumn(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and table
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.rename_col_test (id INTEGER, old_name VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("RenameExistingColumn", func(t *testing.T) {
		// Rename column via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(`
			ALTER TABLE %s.test_schema.rename_col_test RENAME COLUMN old_name TO new_name
		`, attachName))
		if err != nil {
			t.Fatalf("ALTER TABLE RENAME COLUMN failed: %v", err)
		}

		// Verify column was renamed in mock catalog
		schema := mockCat.GetSchema("test_schema")
		table := schema.GetTable("rename_col_test")
		if table.HasColumn("old_name") {
			t.Error("old column name still exists in catalog")
		}
		if !table.HasColumn("new_name") {
			t.Error("new column name does not exist in catalog")
		}

		// Verify DuckDB no longer sees the old column name via duckdb_columns()
		var oldCount int
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM duckdb_columns() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'rename_col_test' AND column_name = 'old_name'",
			attachName,
		)).Scan(&oldCount)
		if err != nil {
			t.Errorf("DuckDB query for old column failed: %v", err)
		}
		if oldCount != 0 {
			t.Errorf("DuckDB still sees old column name 'old_name' in duckdb_columns(), count = %d", oldCount)
		}

		// Verify DuckDB sees the new column name via duckdb_columns()
		var newColumnName string
		err = db.QueryRow(fmt.Sprintf(
			"SELECT column_name FROM duckdb_columns() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'rename_col_test' AND column_name = 'new_name'",
			attachName,
		)).Scan(&newColumnName)
		if err != nil {
			t.Errorf("DuckDB cannot see renamed column 'new_name' in duckdb_columns(): %v", err)
		}
		if newColumnName != "new_name" {
			t.Errorf("expected column_name 'new_name', got %q", newColumnName)
		}
	})
}

// TestDDLRenameTable tests ALTER TABLE RENAME TO operation via DuckDB SQL.
func TestDDLRenameTable(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and table
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.old_table (id INTEGER)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("RenameExistingTable", func(t *testing.T) {
		// Rename table via DuckDB SQL
		_, err := db.Exec(fmt.Sprintf(`
			ALTER TABLE %s.test_schema.old_table RENAME TO new_table
		`, attachName))
		if err != nil {
			t.Fatalf("ALTER TABLE RENAME TO failed: %v", err)
		}

		// Verify table was renamed in mock catalog
		schema := mockCat.GetSchema("test_schema")
		if schema.HasTable("old_table") {
			t.Error("old table name still exists in catalog")
		}
		if !schema.HasTable("new_table") {
			t.Error("new table name does not exist in catalog")
		}

		// Verify DuckDB no longer sees the old table name via duckdb_tables()
		var oldCount int
		err = db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'old_table'",
			attachName,
		)).Scan(&oldCount)
		if err != nil {
			t.Errorf("DuckDB query for old table failed: %v", err)
		}
		if oldCount != 0 {
			t.Errorf("DuckDB still sees old table name 'old_table' in duckdb_tables(), count = %d", oldCount)
		}

		// Verify DuckDB sees the new table name via duckdb_tables()
		var newTableName string
		err = db.QueryRow(fmt.Sprintf(
			"SELECT table_name FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'new_table'",
			attachName,
		)).Scan(&newTableName)
		if err != nil {
			t.Errorf("DuckDB cannot see renamed table 'new_table' in duckdb_tables(): %v", err)
		}
		if newTableName != "new_table" {
			t.Errorf("expected table_name 'new_table', got %q", newTableName)
		}
	})
}

// TestDDLCreateTableAsSelect tests CREATE TABLE AS SELECT operation.
// This triggers create_table followed by insert.
func TestDDLCreateTableAsSelect(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// First create schema and source table
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.test_schema", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.test_schema.source_table (id INTEGER, name VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE source TABLE failed: %v", err)
	}

	t.Run("CreateTableAsSelectFromLocal", func(t *testing.T) {
		// Create a local temp table to select from
		_, err := db.Exec("CREATE TEMP TABLE local_data AS SELECT 1 as id, 'test' as name")
		if err != nil {
			t.Fatalf("CREATE local TEMP TABLE failed: %v", err)
		}

		// CREATE TABLE AS SELECT from local temp table into remote
		_, err = db.Exec(fmt.Sprintf(`
			CREATE TABLE %s.test_schema.ctas_test AS SELECT * FROM local_data
		`, attachName))
		if err != nil {
			t.Fatalf("CREATE TABLE AS SELECT failed: %v", err)
		}

		// Verify table was created via mock catalog
		schema := mockCat.GetSchema("test_schema")
		if !schema.HasTable("ctas_test") {
			t.Error("table was not created via CREATE TABLE AS SELECT")
		}

		// Verify DuckDB sees the table via duckdb_tables() metadata function
		var tableName string
		var columnCount int64
		err = db.QueryRow(fmt.Sprintf(
			"SELECT table_name, column_count FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = 'test_schema' AND table_name = 'ctas_test'",
			attachName,
		)).Scan(&tableName, &columnCount)
		if err != nil {
			t.Fatalf("DuckDB verification failed: %v", err)
		}
		if tableName != "ctas_test" {
			t.Errorf("DuckDB table name = %q, want %q", tableName, "ctas_test")
		}
		if columnCount != 2 {
			t.Errorf("DuckDB column_count = %d, want 2", columnCount)
		}

		// Verify the inserted data can be read back
		var id int
		var name string
		err = db.QueryRow(fmt.Sprintf(
			"SELECT id, name FROM %s.test_schema.ctas_test",
			attachName,
		)).Scan(&id, &name)
		if err != nil {
			t.Fatalf("SELECT from CTAS table failed: %v", err)
		}
		if id != 1 {
			t.Errorf("id = %d, want 1", id)
		}
		if name != "test" {
			t.Errorf("name = %q, want %q", name, "test")
		}
	})
}

// TestDDLExtendedLifecycle tests extended DDL operations including rename.
func TestDDLExtendedLifecycle(t *testing.T) {
	mockCat := newMockDynamicCatalog()
	server := newTestServer(t, mockCat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Step 1: Create schema
	_, err := db.Exec(fmt.Sprintf("CREATE SCHEMA %s.extended_test", attachName))
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Step 2: Create table
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s.extended_test.test_table (id INTEGER, old_col VARCHAR)
	`, attachName))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Step 3: Rename column
	_, err = db.Exec(fmt.Sprintf(`
		ALTER TABLE %s.extended_test.test_table RENAME COLUMN old_col TO new_col
	`, attachName))
	if err != nil {
		t.Fatalf("RENAME COLUMN failed: %v", err)
	}

	// Verify column renamed
	schema := mockCat.GetSchema("extended_test")
	table := schema.GetTable("test_table")
	if table.HasColumn("old_col") {
		t.Fatal("old column name still exists")
	}
	if !table.HasColumn("new_col") {
		t.Fatal("new column name does not exist")
	}

	// Step 4: Rename table
	_, err = db.Exec(fmt.Sprintf(`
		ALTER TABLE %s.extended_test.test_table RENAME TO renamed_table
	`, attachName))
	if err != nil {
		t.Fatalf("RENAME TABLE failed: %v", err)
	}

	// Verify table renamed
	if schema.HasTable("test_table") {
		t.Fatal("old table name still exists")
	}
	if !schema.HasTable("renamed_table") {
		t.Fatal("new table name does not exist")
	}

	// Step 5: Drop renamed table
	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s.extended_test.renamed_table", attachName))
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	// Step 6: Drop schema
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA %s.extended_test", attachName))
	if err != nil {
		t.Fatalf("DROP SCHEMA failed: %v", err)
	}

	t.Log("Extended DDL lifecycle completed successfully")
}
