package airport_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
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

// timeTravelCatalog creates a catalog with versioned data for time travel testing.
// The catalog has data at two different time points with schema evolution.
func timeTravelCatalog() catalog.Catalog {
	// Define two time points
	// Time point 1: 2024-01-01 00:00:00 UTC (1704067200 seconds)
	// Time point 2: 2024-06-01 00:00:00 UTC (1717200000 seconds) - 5 months later
	time2 := time.Unix(1717200000, 0).UTC()

	// Schema at time1: id, name, email
	schemaV1 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
	}, nil)

	// Schema at time2: id, name, email, phone (added column)
	schemaV2 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
		{Name: "phone", Type: arrow.BinaryTypes.String},
	}, nil)

	// Data at time1 (3 users, no phone column)
	dataV1 := [][]interface{}{
		{int64(1), "Alice", "alice@example.com"},
		{int64(2), "Bob", "bob@example.com"},
		{int64(3), "Charlie", "charlie@example.com"},
	}

	// Data at time2 (4 users, with phone column)
	dataV2 := [][]interface{}{
		{int64(1), "Alice", "alice@example.com", "555-0001"},
		{int64(2), "Bob", "bob@updated.com", "555-0002"}, // Email updated
		{int64(3), "Charlie", "charlie@example.com", "555-0003"},
		{int64(4), "David", "david@example.com", "555-0004"}, // New user
	}

	// Create scan function that respects time travel
	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		// Determine which version to return based on timestamp
		var schema *arrow.Schema
		var data [][]interface{}

		if opts.TimePoint != nil {
			// Parse the timestamp from TimePoint
			// TimePoint.Value is a string representation of Unix timestamp
			var requestTime time.Time

			switch opts.TimePoint.Unit {
			case "timestamp":
				// Parse seconds since epoch
				var ts int64
				_, err := fmt.Sscanf(opts.TimePoint.Value, "%d", &ts)
				if err == nil {
					requestTime = time.Unix(ts, 0).UTC()
				}
			case "timestamp_ns":
				// Parse nanoseconds since epoch
				var tsNs int64
				_, err := fmt.Sscanf(opts.TimePoint.Value, "%d", &tsNs)
				if err == nil {
					requestTime = time.Unix(0, tsNs).UTC()
				}
			}

			if !requestTime.IsZero() && requestTime.Before(time2) {
				// Return v1 data (schema without phone)
				schema = schemaV1
				data = dataV1
			} else {
				// Return v2 data (schema with phone)
				schema = schemaV2
				data = dataV2
			}
		} else {
			// No time specified - return current/latest data
			schema = schemaV2
			data = dataV2
		}

		record := buildTestRecord(schema, data)
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	}

	// Build catalog with time-aware table
	cat, err := airport.NewCatalogBuilder().
		Schema("versioned_schema").
		Comment("Schema with time travel support").
		SimpleTable(airport.SimpleTableDef{
			Name:     "versioned_users",
			Comment:  "User table with schema evolution (phone added in v2)",
			Schema:   schemaV2, // Current schema
			ScanFunc: scanFunc,
		}).
		Build()

	if err != nil {
		panic("Failed to build time travel catalog: " + err.Error())
	}

	return cat
}

// TestTimeTravelQueries tests time travel functionality with AT syntax.
func TestTimeTravelQueries(t *testing.T) {
	//t.Skip("DuckDB Airport extension does not yet support SQL-level time travel syntax (AT TIMESTAMP)")

	cat := timeTravelCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Query at time point 1 (old schema without phone, 3 users)
	t.Run("QueryAtTime1_OldSchema", func(t *testing.T) {
		// 2024-01-01 00:00:00 UTC = 1704067200 seconds
		// Note: DuckDB Airport extension handles time travel at protocol level,
		// not via SQL syntax yet
		query := "SELECT * FROM " + attachName + ".versioned_schema.versioned_users AT (TIMESTAMP => TIMESTAMP '2024-01-01 00:00:00') ORDER BY id"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		// Should get 3 columns (id, name, email - no phone)
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Failed to get columns: %v", err)
		}

		if len(cols) != 3 {
			t.Errorf("Expected 3 columns at time1, got %d: %v", len(cols), cols)
		}

		// Verify column names
		expectedCols := []string{"id", "name", "email"}
		for i, col := range cols {
			if col != expectedCols[i] {
				t.Errorf("Column %d: expected '%s', got '%s'", i, expectedCols[i], col)
			}
		}

		// Count rows
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
			t.Errorf("Expected 3 rows at time1, got %d", rowCount)
		}
	})

	// Test 2: Query at time point 2 (new schema with phone, 4 users)
	t.Run("QueryAtTime2_NewSchema", func(t *testing.T) {
		// 2024-06-01 00:00:00 UTC = 1717200000 seconds
		query := "SELECT * FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-06-01 00:00:00' ORDER BY id"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		// Should get 4 columns (id, name, email, phone)
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Failed to get columns: %v", err)
		}

		if len(cols) != 4 {
			t.Errorf("Expected 4 columns at time2, got %d: %v", len(cols), cols)
		}

		// Verify column names
		expectedCols := []string{"id", "name", "email", "phone"}
		for i, col := range cols {
			if col != expectedCols[i] {
				t.Errorf("Column %d: expected '%s', got '%s'", i, expectedCols[i], col)
			}
		}

		// Count rows and verify data
		rowCount := 0
		for rows.Next() {
			var id int64
			var name, email, phone string
			if err := rows.Scan(&id, &name, &email, &phone); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			rowCount++

			// Verify specific changes
			if rowCount == 2 {
				// Bob's email should be updated
				if id != 2 || name != "Bob" || email != "bob@updated.com" || phone != "555-0002" {
					t.Errorf("Row 2: expected (2, Bob, bob@updated.com, 555-0002), got (%d, %s, %s, %s)", id, name, email, phone)
				}
			}
			if rowCount == 4 {
				// David should exist (new user)
				if id != 4 || name != "David" || email != "david@example.com" || phone != "555-0004" {
					t.Errorf("Row 4: expected (4, David, david@example.com, 555-0004), got (%d, %s, %s, %s)", id, name, email, phone)
				}
			}
		}

		if rowCount != 4 {
			t.Errorf("Expected 4 rows at time2, got %d", rowCount)
		}
	})

	// Test 3: Query without time (should return current/latest data)
	t.Run("QueryCurrentData", func(t *testing.T) {
		query := "SELECT * FROM " + attachName + ".versioned_schema.versioned_users ORDER BY id"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		// Should get current schema (4 columns, 4 rows)
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Failed to get columns: %v", err)
		}

		if len(cols) != 4 {
			t.Errorf("Expected 4 columns for current data, got %d", len(cols))
		}

		rowCount := 0
		for rows.Next() {
			var id int64
			var name, email, phone string
			if err := rows.Scan(&id, &name, &email, &phone); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			rowCount++
		}

		if rowCount != 4 {
			t.Errorf("Expected 4 rows for current data, got %d", rowCount)
		}
	})

	// Test 4: Verify schema evolution - compare data across time points
	t.Run("CompareDataAcrossTimePoints", func(t *testing.T) {
		// Query Bob's email at both time points
		query1 := "SELECT email FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-01-01 00:00:00' WHERE id = 2"
		var email1 string
		if err := db.QueryRow(query1).Scan(&email1); err != nil {
			t.Fatalf("Query1 failed: %v", err)
		}

		query2 := "SELECT email FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-06-01 00:00:00' WHERE id = 2"
		var email2 string
		if err := db.QueryRow(query2).Scan(&email2); err != nil {
			t.Fatalf("Query2 failed: %v", err)
		}

		// Bob's email should have changed
		if email1 != "bob@example.com" {
			t.Errorf("Expected Bob's old email to be 'bob@example.com', got '%s'", email1)
		}
		if email2 != "bob@updated.com" {
			t.Errorf("Expected Bob's new email to be 'bob@updated.com', got '%s'", email2)
		}
	})

	// Test 5: Query phone column only available in newer schema
	t.Run("QueryNewColumnAtDifferentTimes", func(t *testing.T) {
		// At time1, phone column doesn't exist - query should fail or return error
		query1 := "SELECT phone FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-01-01 00:00:00' WHERE id = 1"
		var phone1 string
		err := db.QueryRow(query1).Scan(&phone1)
		if err == nil {
			t.Error("Expected error when querying phone column at time1 (column didn't exist yet)")
		}

		// At time2, phone column exists
		query2 := "SELECT phone FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-06-01 00:00:00' WHERE id = 1"
		var phone2 string
		if err := db.QueryRow(query2).Scan(&phone2); err != nil {
			t.Fatalf("Query2 failed: %v", err)
		}

		if phone2 != "555-0001" {
			t.Errorf("Expected phone '555-0001', got '%s'", phone2)
		}
	})

	// Test 6: Count users at different time points
	t.Run("CountUsersAcrossTime", func(t *testing.T) {
		// Count at time1 (should be 3)
		query1 := "SELECT COUNT(*) FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-01-01 00:00:00'"
		var count1 int64
		if err := db.QueryRow(query1).Scan(&count1); err != nil {
			t.Fatalf("Query1 failed: %v", err)
		}

		// Count at time2 (should be 4)
		query2 := "SELECT COUNT(*) FROM " + attachName + ".versioned_schema.versioned_users AT TIMESTAMP '2024-06-01 00:00:00'"
		var count2 int64
		if err := db.QueryRow(query2).Scan(&count2); err != nil {
			t.Fatalf("Query2 failed: %v", err)
		}

		if count1 != 3 {
			t.Errorf("Expected 3 users at time1, got %d", count1)
		}
		if count2 != 4 {
			t.Errorf("Expected 4 users at time2, got %d", count2)
		}
	})
}
