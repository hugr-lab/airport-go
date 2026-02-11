package airport_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// namedCatalog wraps a catalog.Catalog with a name for multi-catalog testing.
type namedCatalog struct {
	catalog.Catalog
	name string
}

func (c *namedCatalog) Name() string {
	return c.name
}

// multiCatalogTestServer wraps a multi-catalog Flight server for integration testing.
type multiCatalogTestServer struct {
	grpcServer *grpc.Server
	mcs        *airport.MultiCatalogServer
	listener   net.Listener
	address    string
}

// newMultiCatalogTestServer creates and starts a test multi-catalog Flight server.
func newMultiCatalogTestServer(t *testing.T, catalogs []catalog.Catalog) *multiCatalogTestServer {
	t.Helper()

	// Create listener on random port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	// Configure server with debug logging for tests
	debugLevel := slog.LevelDebug
	config := airport.MultiCatalogServerConfig{
		Catalogs: catalogs,
		Address:  lis.Addr().String(),
		LogLevel: &debugLevel,
	}

	opts := airport.MultiCatalogServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	mcs, err := airport.NewMultiCatalogServer(grpcServer, config)
	if err != nil {
		t.Fatalf("Failed to register multi-catalog server: %v", err)
	}

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	return &multiCatalogTestServer{
		grpcServer: grpcServer,
		mcs:        mcs,
		listener:   lis,
		address:    lis.Addr().String(),
	}
}

// stop gracefully stops the test server.
func (s *multiCatalogTestServer) stop() {
	s.grpcServer.GracefulStop()
	s.listener.Close()
}

// createSalesNamedCatalog creates the "sales" catalog with an orders table.
func createSalesNamedCatalog() catalog.Catalog {
	ordersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "customer", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	scanOrders := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, ordersSchema)
		defer builder.Release()

		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1001, 1002, 1003}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Acme Corp", "Widgets Inc", "TechStart"}, nil)
		builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1500.00, 2300.50, 890.75}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(ordersSchema, []arrow.RecordBatch{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("sales_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "orders",
			Comment:  "Sales orders",
			Schema:   ordersSchema,
			ScanFunc: scanOrders,
		}).
		Build()
	if err != nil {
		panic("Failed to build sales catalog: " + err.Error())
	}

	return &namedCatalog{Catalog: cat, name: "sales"}
}

// createAnalyticsNamedCatalog creates the "analytics" catalog with a metrics table.
func createAnalyticsNamedCatalog() catalog.Catalog {
	metricsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "metric_name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	scanMetrics := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, metricsSchema)
		defer builder.Release()

		builder.Field(0).(*array.StringBuilder).AppendValues([]string{"page_views", "conversions"}, nil)
		builder.Field(1).(*array.Float64Builder).AppendValues([]float64{15234, 342}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(metricsSchema, []arrow.RecordBatch{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("analytics_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "metrics",
			Comment:  "Analytics metrics",
			Schema:   metricsSchema,
			ScanFunc: scanMetrics,
		}).
		Build()
	if err != nil {
		panic("Failed to build analytics catalog: " + err.Error())
	}

	return &namedCatalog{Catalog: cat, name: "analytics"}
}

// createDefaultCatalog creates a default catalog (empty name) with a users table.
func createDefaultCatalog() catalog.Catalog {
	usersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	scanUsers := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, usersSchema)
		defer builder.Release()

		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob"}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(usersSchema, []arrow.RecordBatch{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("default_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "Default users table",
			Schema:   usersSchema,
			ScanFunc: scanUsers,
		}).
		Build()
	if err != nil {
		panic("Failed to build default catalog: " + err.Error())
	}

	// Default catalog has empty name
	return &namedCatalog{Catalog: cat, name: ""}
}

// TestMultiCatalogDynamicAddRemove tests adding and removing catalogs at runtime.
func TestMultiCatalogDynamicAddRemove(t *testing.T) {
	salesCatalog := createSalesNamedCatalog()

	server := newMultiCatalogTestServer(t, []catalog.Catalog{salesCatalog})
	defer server.stop()

	// Add analytics catalog dynamically
	analyticsCatalog := createAnalyticsNamedCatalog()
	err := server.mcs.AddCatalog(analyticsCatalog)
	if err != nil {
		t.Fatalf("Failed to add analytics catalog: %v", err)
	}
	t.Log("Successfully added analytics catalog")

	// Try to add duplicate - should fail
	duplicateCatalog := createAnalyticsNamedCatalog()
	err = server.mcs.AddCatalog(duplicateCatalog)
	if err == nil {
		t.Fatal("Expected error when adding duplicate catalog")
	}
	t.Logf("Correctly rejected duplicate catalog: %v", err)

	// Remove analytics catalog
	err = server.mcs.RemoveCatalog("analytics")
	if err != nil {
		t.Fatalf("Failed to remove analytics catalog: %v", err)
	}
	t.Log("Successfully removed analytics catalog")

	// Try to remove non-existent catalog - should fail
	err = server.mcs.RemoveCatalog("nonexistent")
	if err == nil {
		t.Fatal("Expected error when removing non-existent catalog")
	}
	t.Logf("Correctly rejected removal of non-existent catalog: %v", err)
}

// TestMultiCatalogDefaultRouting tests that requests without a catalog header
// route to the default catalog (empty name).
func TestMultiCatalogDefaultRouting(t *testing.T) {
	// Create a default catalog (empty name) and named catalogs
	defaultCatalog := createDefaultCatalog()
	salesCatalog := createSalesNamedCatalog()

	server := newMultiCatalogTestServer(t, []catalog.Catalog{defaultCatalog, salesCatalog})
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	// Connect without catalog header - should route to default catalog
	attachName := connectToFlightServer(t, db, server.address, "")

	// Query the default catalog's users table
	query := fmt.Sprintf("SELECT * FROM %s.default_schema.users ORDER BY id", attachName)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Verify we get 2 users from the default catalog
	rowCount := 0
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		rowCount++

		// Verify first row
		if rowCount == 1 {
			if id != 1 || name != "Alice" {
				t.Errorf("Row 1: expected (1, Alice), got (%d, %s)", id, name)
			}
		}
	}

	if rowCount != 2 {
		t.Errorf("Expected 2 rows from default catalog, got %d", rowCount)
	}
}

// TestMultiCatalogCatalogDiscovery tests that catalog discovery works with the default catalog.
func TestMultiCatalogCatalogDiscovery(t *testing.T) {
	defaultCatalog := createDefaultCatalog()
	salesCatalog := createSalesNamedCatalog()

	server := newMultiCatalogTestServer(t, []catalog.Catalog{defaultCatalog, salesCatalog})
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test: Discover schemas from default catalog
	t.Run("ListSchemas", func(t *testing.T) {
		rows, err := db.Query("SELECT schema_name FROM duckdb_schemas() WHERE database_name = ?", attachName)
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

		// Should have "default_schema" schema from default catalog
		found := false
		for _, s := range schemas {
			if s == "default_schema" {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected to find 'default_schema' schema, got schemas: %v", schemas)
		}
	})

	// Test: Discover tables from default catalog
	t.Run("ListTables", func(t *testing.T) {
		query := "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'default_schema' AND database_name = ?"
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

		// Should have "users" table from default catalog
		found := false
		for _, table := range tables {
			if table == "users" {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected to find 'users' table from default catalog, got tables: %v", tables)
		}
	})
}

// TestMultiCatalogHeaderRouting tests that requests are routed to the correct catalog
// based on the airport-catalog header.
//
// SKIP: This test requires the airport-catalog header to be implemented in the
// DuckDB Airport extension. The feature is pending merge.
func TestMultiCatalogHeaderRouting(t *testing.T) {
	t.Skip("Skipping: airport-catalog header not yet implemented in DuckDB Airport extension")

	salesCatalog := createSalesNamedCatalog()
	analyticsCatalog := createAnalyticsNamedCatalog()

	server := newMultiCatalogTestServer(t, []catalog.Catalog{salesCatalog, analyticsCatalog})
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	// TODO: Once airport-catalog header is supported in DuckDB extension:
	// 1. Attach with catalog header specifying "sales"
	// 2. Query should return orders table data
	// 3. Attach with catalog header specifying "analytics"
	// 4. Query should return metrics table data

	_ = server.address
}

// TestMultiCatalogHeaderDiscovery tests that catalog discovery works with specific catalog headers.
//
// SKIP: This test requires the airport-catalog header to be implemented in the
// DuckDB Airport extension. The feature is pending merge.
func TestMultiCatalogHeaderDiscovery(t *testing.T) {
	t.Skip("Skipping: airport-catalog header not yet implemented in DuckDB Airport extension")

	salesCatalog := createSalesNamedCatalog()
	analyticsCatalog := createAnalyticsNamedCatalog()

	server := newMultiCatalogTestServer(t, []catalog.Catalog{salesCatalog, analyticsCatalog})
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	// TODO: Once airport-catalog header is supported in DuckDB extension:
	// 1. Attach to sales catalog with header
	// 2. Verify only sales tables (orders) are visible
	// 3. Attach to analytics catalog with header
	// 4. Verify only analytics tables (metrics) are visible

	_ = server.address
}
