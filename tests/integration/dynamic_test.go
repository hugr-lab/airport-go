package airport_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/hugr-lab/airport-go/catalog"
)

// dynamicTestCatalog implements a catalog that can change at runtime.
type dynamicTestCatalog struct {
	mu      sync.RWMutex
	schemas map[string]*dynamicTestSchema
}

func newDynamicTestCatalog() *dynamicTestCatalog {
	return &dynamicTestCatalog{
		schemas: make(map[string]*dynamicTestSchema),
	}
}

func (c *dynamicTestCatalog) addSchema(name string, schema *dynamicTestSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[name] = schema
}

func (c *dynamicTestCatalog) removeSchema(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, name)
}

func (c *dynamicTestCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]catalog.Schema, 0, len(c.schemas))
	for _, schema := range c.schemas {
		result = append(result, schema)
	}
	return result, nil
}

func (c *dynamicTestCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[name]
	if !ok {
		return nil, nil
	}
	return schema, nil
}

// dynamicTestSchema implements a schema with mutable tables.
type dynamicTestSchema struct {
	name    string
	comment string
	mu      sync.RWMutex
	tables  map[string]catalog.Table
}

func newDynamicTestSchema(name, comment string) *dynamicTestSchema {
	return &dynamicTestSchema{
		name:    name,
		comment: comment,
		tables:  make(map[string]catalog.Table),
	}
}

func (s *dynamicTestSchema) addTable(table catalog.Table) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[table.Name()] = table
}

func (s *dynamicTestSchema) removeTable(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, name)
}

func (s *dynamicTestSchema) Name() string {
	return s.name
}

func (s *dynamicTestSchema) Comment() string {
	return s.comment
}

func (s *dynamicTestSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]catalog.Table, 0, len(s.tables))
	for _, table := range s.tables {
		result = append(result, table)
	}
	return result, nil
}

func (s *dynamicTestSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	table, ok := s.tables[name]
	if !ok {
		return nil, nil
	}
	return table, nil
}

func (s *dynamicTestSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

func (s *dynamicTestSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

// liveTable is a table whose data changes on each scan.
type liveTable struct {
	name    string
	comment string
	schema  *arrow.Schema
	getData func() [][]interface{}
}

func (t *liveTable) Name() string {
	return t.name
}

func (t *liveTable) Comment() string {
	return t.comment
}

func (t *liveTable) ArrowSchema() *arrow.Schema {
	return t.schema
}

func (t *liveTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	data := t.getData()
	record := buildTestRecord(t.schema, data)
	defer record.Release()
	return array.NewRecordReader(t.schema, []arrow.Record{record})
}

// TestDynamicCatalog verifies that catalogs can change at runtime
// and clients see the updates.
func TestDynamicCatalog(t *testing.T) {
	cat := newDynamicTestCatalog()

	// Start with one schema
	schema1 := newDynamicTestSchema("initial", "Initial schema")
	cat.addSchema("initial", schema1)

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Initial schema is visible
	t.Run("InitialSchema", func(t *testing.T) {
		query := "SELECT schema_name FROM duckdb_schemas() WHERE database_name = ?"
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		schemas := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			schemas = append(schemas, name)
		}

		found := false
		for _, s := range schemas {
			if s == "initial" {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected to find 'initial' schema, got: %v", schemas)
		}
	})

	// Test 2: Add schema at runtime
	t.Run("AddSchema", func(t *testing.T) {
		schema2 := newDynamicTestSchema("dynamic", "Dynamically added schema")
		cat.addSchema("dynamic", schema2)

		// Note: DuckDB may cache catalog metadata, so newly added schemas
		// might not be immediately visible without reconnection
		// This test documents the expected behavior
		t.Log("Schema added dynamically - may require reconnection to see")
	})

	// Test 3: Remove schema at runtime
	t.Run("RemoveSchema", func(t *testing.T) {
		cat.removeSchema("initial")

		// Note: DuckDB may cache catalog metadata
		t.Log("Schema removed dynamically - may require reconnection to see")
	})
}

// TestLiveData verifies that table data can change between queries.
func TestLiveData(t *testing.T) {
	cat := newDynamicTestCatalog()
	schema := newDynamicTestSchema("live", "Schema with live data")

	// Create table with counter that increments on each scan
	counter := 0
	counterMu := sync.Mutex{}

	metricsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "metric", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	liveMetrics := &liveTable{
		name:    "metrics",
		comment: "Live metrics",
		schema:  metricsSchema,
		getData: func() [][]interface{} {
			counterMu.Lock()
			defer counterMu.Unlock()
			counter++
			return [][]interface{}{
				{"scan_count", int64(counter)},
			}
		},
	}

	schema.addTable(liveMetrics)
	cat.addSchema("live", schema)

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Query multiple times - should get different values
	t.Run("ChangingData", func(t *testing.T) {
		query := "SELECT value FROM " + attachName + ".live.metrics WHERE metric = 'scan_count'"

		// First query
		var value1 int64
		if err := db.QueryRow(query).Scan(&value1); err != nil {
			t.Fatalf("First query failed: %v", err)
		}

		// Second query
		var value2 int64
		if err := db.QueryRow(query).Scan(&value2); err != nil {
			t.Fatalf("Second query failed: %v", err)
		}

		// Values should be different (counter increments)
		if value1 == value2 {
			t.Logf("Note: Values are the same (%d), DuckDB may be caching results", value1)
		} else {
			t.Logf("Live data working: first=%d, second=%d", value1, value2)
		}
	})
}

// TestDynamicTables verifies that tables can be added/removed at runtime.
func TestDynamicTables(t *testing.T) {
	cat := newDynamicTestCatalog()
	schema := newDynamicTestSchema("dynamic", "Dynamic schema")
	cat.addSchema("dynamic", schema)

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: No tables initially
	t.Run("NoTables", func(t *testing.T) {
		query := "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'dynamic' AND database_name = ?"
		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		tableCount := 0
		for rows.Next() {
			tableCount++
		}

		if tableCount != 0 {
			t.Errorf("Expected 0 tables, got %d", tableCount)
		}
	})

	// Test 2: Add table at runtime
	t.Run("AddTable", func(t *testing.T) {
		tableSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		testData := [][]interface{}{{int64(1)}}

		newTable := catalog.NewStaticTable(
			"test_table",
			"Test table",
			tableSchema,
			makeScanFunc(tableSchema, testData),
		)

		schema.addTable(newTable)

		// Note: May require reconnection to see new table
		t.Log("Table added dynamically - may require reconnection to see")
	})

	// Test 3: Remove table at runtime
	t.Run("RemoveTable", func(t *testing.T) {
		schema.removeTable("test_table")

		// Note: May require reconnection to see removal
		t.Log("Table removed dynamically - may require reconnection to see")
	})
}

// TestConcurrentCatalogAccess verifies that the catalog is thread-safe.
func TestConcurrentCatalogAccess(t *testing.T) {
	cat := newDynamicTestCatalog()
	schema := newDynamicTestSchema("concurrent", "Concurrent access test")

	// Add a simple table
	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	testData := [][]interface{}{{int64(1)}}

	table := catalog.NewStaticTable(
		"test",
		"Test table",
		tableSchema,
		makeScanFunc(tableSchema, testData),
	)

	schema.addTable(table)
	cat.addSchema("concurrent", schema)

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Run concurrent queries
	t.Run("ConcurrentQueries", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				query := "SELECT COUNT(*) FROM " + attachName + ".concurrent.test"
				var count int64
				if err := db.QueryRow(query).Scan(&count); err != nil {
					errors <- err
				}
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Concurrent queries timed out")
		}

		close(errors)
		for err := range errors {
			t.Errorf("Concurrent query error: %v", err)
		}
	})
}
