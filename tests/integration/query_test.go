package airport_test

import (
	"testing"
)

// TestQueryExecution verifies that DuckDB can execute queries against
// Flight server tables and retrieve correct results.
func TestQueryExecution(t *testing.T) {
	cat := simpleCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Simple SELECT query
	t.Run("SimpleSelect", func(t *testing.T) {
		query := "SELECT * FROM " + attachName + ".some_schema.users ORDER BY id"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		// Verify we get 3 rows
		rowCount := 0
		for rows.Next() {
			var id int64
			var name, email string
			if err := rows.Scan(&id, &name, &email); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			rowCount++

			// Verify first row
			if rowCount == 1 {
				if id != 1 || name != "Alice" || email != "alice@example.com" {
					t.Errorf("Row 1: expected (1, Alice, alice@example.com), got (%d, %s, %s)", id, name, email)
				}
			}
		}

		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
		}
	})

	// Test 2: Filtered query
	t.Run("FilteredSelect", func(t *testing.T) {
		query := "SELECT name FROM " + attachName + ".some_schema.users WHERE id = 2"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected 1 row, got 0")
		}

		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		if name != "Bob" {
			t.Errorf("Expected name 'Bob', got '%s'", name)
		}
	})

	// Test 3: Aggregation query
	t.Run("Aggregation", func(t *testing.T) {
		query := "SELECT COUNT(*) FROM " + attachName + ".some_schema.products"
		var count int64
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
	})

	// Test 4: JOIN between tables
	t.Run("Join", func(t *testing.T) {
		// Create a cross join (Cartesian product)
		query := `
			SELECT u.name, p.name
			FROM ` + attachName + `.some_schema.users u,
			     ` + attachName + `.some_schema.products p
			WHERE u.id = 1 AND p.id = 101
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected 1 row, got 0")
		}

		var userName, productName string
		if err := rows.Scan(&userName, &productName); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		if userName != "Alice" || productName != "Widget" {
			t.Errorf("Expected (Alice, Widget), got (%s, %s)", userName, productName)
		}
	})

	// Test 5: ORDER BY
	t.Run("OrderBy", func(t *testing.T) {
		query := "SELECT name FROM " + attachName + ".some_schema.users ORDER BY name DESC"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		expectedNames := []string{"Charlie", "Bob", "Alice"}
		i := 0
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}

			if i >= len(expectedNames) {
				t.Errorf("Got more rows than expected")
				break
			}

			if name != expectedNames[i] {
				t.Errorf("Row %d: expected '%s', got '%s'", i, expectedNames[i], name)
			}
			i++
		}

		if i != len(expectedNames) {
			t.Errorf("Expected %d rows, got %d", len(expectedNames), i)
		}
	})

	// Test 6: LIMIT and OFFSET
	t.Run("LimitOffset", func(t *testing.T) {
		query := "SELECT name FROM " + attachName + ".some_schema.users ORDER BY id LIMIT 1 OFFSET 1"
		var name string
		if err := db.QueryRow(query).Scan(&name); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should get second user (Bob)
		if name != "Bob" {
			t.Errorf("Expected 'Bob', got '%s'", name)
		}
	})
}

// TestComplexQueries tests more advanced SQL features.
func TestComplexQueries(t *testing.T) {
	cat := simpleCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test subquery
	t.Run("Subquery", func(t *testing.T) {
		query := `
			SELECT name
			FROM ` + attachName + `.some_schema.users
			WHERE id IN (SELECT id FROM ` + attachName + `.some_schema.users WHERE id > 1)
			ORDER BY name
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		names := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			names = append(names, name)
		}

		// Should get Bob and Charlie
		if len(names) != 2 {
			t.Errorf("Expected 2 names, got %d", len(names))
		}
	})

	// Test CASE expression
	t.Run("CaseExpression", func(t *testing.T) {
		query := `
			SELECT
				name,
				CASE
					WHEN price < 15 THEN 'Cheap'
					WHEN price < 25 THEN 'Medium'
					ELSE 'Expensive'
				END as price_category
			FROM ` + attachName + `.some_schema.products
			ORDER BY name
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		type result struct {
			name     string
			category string
		}

		results := []result{}
		for rows.Next() {
			var r result
			if err := rows.Scan(&r.name, &r.category); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			results = append(results, r)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
	})

	// Test aggregate functions
	t.Run("AggregateFunctions", func(t *testing.T) {
		query := `
			SELECT
				MIN(price) as min_price,
				MAX(price) as max_price,
				AVG(price) as avg_price
			FROM ` + attachName + `.some_schema.products
		`
		var minPrice, maxPrice, avgPrice float64
		if err := db.QueryRow(query).Scan(&minPrice, &maxPrice, &avgPrice); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if minPrice != 9.99 {
			t.Errorf("Expected min_price 9.99, got %f", minPrice)
		}
		if maxPrice != 29.99 {
			t.Errorf("Expected max_price 29.99, got %f", maxPrice)
		}
		// Average should be around 19.99
		if avgPrice < 19.0 || avgPrice > 21.0 {
			t.Errorf("Expected avg_price around 19.99, got %f", avgPrice)
		}
	})
}

// TestDataTypes verifies that different Arrow data types are correctly
// transferred and usable in DuckDB queries.
func TestDataTypes(t *testing.T) {
	cat := simpleCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test integer type
	t.Run("IntegerType", func(t *testing.T) {
		query := "SELECT id FROM " + attachName + ".some_schema.users WHERE id = 1"
		var id int64
		if err := db.QueryRow(query).Scan(&id); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if id != 1 {
			t.Errorf("Expected id 1, got %d", id)
		}
	})

	// Test string type
	t.Run("StringType", func(t *testing.T) {
		query := "SELECT name FROM " + attachName + ".some_schema.users WHERE name LIKE 'Al%'"
		var name string
		if err := db.QueryRow(query).Scan(&name); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if name != "Alice" {
			t.Errorf("Expected name 'Alice', got '%s'", name)
		}
	})

	// Test float type
	t.Run("FloatType", func(t *testing.T) {
		query := "SELECT price FROM " + attachName + ".some_schema.products WHERE id = 101"
		var price float64
		if err := db.QueryRow(query).Scan(&price); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if price != 9.99 {
			t.Errorf("Expected price 9.99, got %f", price)
		}
	})

	// Test string operations
	t.Run("StringOperations", func(t *testing.T) {
		query := "SELECT UPPER(name) FROM " + attachName + ".some_schema.users WHERE id = 1"
		var upperName string
		if err := db.QueryRow(query).Scan(&upperName); err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if upperName != "ALICE" {
			t.Errorf("Expected 'ALICE', got '%s'", upperName)
		}
	})
}
