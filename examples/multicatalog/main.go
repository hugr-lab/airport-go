// Package main demonstrates a multi-catalog Airport Flight server.
// This example creates a server that serves two catalogs: "sales" and "analytics".
// Clients specify the target catalog via the "airport-catalog" gRPC metadata header.
//
// The example also demonstrates dynamic catalog management - adding and removing
// catalogs at runtime using the MultiCatalogServer methods.
//
// Note: The airport-catalog header support in DuckDB Airport extension is pending.
// This example demonstrates the server-side implementation.
package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// namedCatalog wraps a catalog.Catalog with a name.
type namedCatalog struct {
	catalog.Catalog
	name string
}

func (c *namedCatalog) Name() string {
	return c.name
}

func main() {
	// Create initial catalogs
	salesCatalog := createSalesCatalog()
	analyticsCatalog := createAnalyticsCatalog()

	// Create server configuration with multiple catalogs
	debugLevel := slog.LevelDebug
	config := airport.MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
		LogLevel: &debugLevel,
	}

	// Create gRPC server with options (includes interceptors for metadata extraction)
	opts := airport.MultiCatalogServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	// Register multi-catalog Airport service
	// The returned MultiCatalogServer can be used to add/remove catalogs at runtime
	mcs, err := airport.NewMultiCatalogServer(grpcServer, config)
	if err != nil {
		log.Fatalf("Failed to register multi-catalog server: %v", err)
	}

	// Demonstrate dynamic catalog management in a goroutine
	go demonstrateDynamicCatalogs(mcs)

	// Start serving
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Multi-Catalog Airport server listening on :50051")
	log.Println("")
	log.Println("Initial catalogs:")
	log.Println("  - sales (sales_data.orders table)")
	log.Println("  - analytics (analytics_data.metrics table)")
	log.Println("")
	log.Println("Dynamic catalog 'inventory' will be added after 5 seconds")
	log.Println("")
	log.Println("Usage with DuckDB (once airport-catalog header is supported):")
	log.Println("  ATTACH 'grpc://localhost:50051' AS db (TYPE AIRPORT);")
	log.Println("  SELECT * FROM db.sales_data.orders;  -- routes to 'sales' catalog")

	// Handle graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// demonstrateDynamicCatalogs shows how to add and remove catalogs at runtime.
func demonstrateDynamicCatalogs(mcs *airport.MultiCatalogServer) {
	// Wait a bit before adding a new catalog
	time.Sleep(5 * time.Second)

	// Add a new catalog dynamically
	inventoryCatalog := createInventoryCatalog()
	if err := mcs.AddCatalog(inventoryCatalog); err != nil {
		log.Printf("Failed to add inventory catalog: %v", err)
	} else {
		log.Println("Added 'inventory' catalog dynamically")
	}

	// Wait and then remove it to demonstrate removal
	time.Sleep(10 * time.Second)

	if err := mcs.RemoveCatalog("inventory"); err != nil {
		log.Printf("Failed to remove inventory catalog: %v", err)
	} else {
		log.Println("Removed 'inventory' catalog")
	}
}

// createSalesCatalog creates the "sales" catalog with an orders table.
func createSalesCatalog() catalog.Catalog {
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
		Schema("sales_data").
		SimpleTable(airport.SimpleTableDef{
			Name:     "orders",
			Comment:  "Sales orders",
			Schema:   ordersSchema,
			ScanFunc: scanOrders,
		}).
		Build()
	if err != nil {
		log.Fatalf("Failed to build sales catalog: %v", err)
	}

	return &namedCatalog{Catalog: cat, name: "sales"}
}

// createAnalyticsCatalog creates the "analytics" catalog with a metrics table.
func createAnalyticsCatalog() catalog.Catalog {
	metricsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "metric_name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "timestamp", Type: arrow.BinaryTypes.String},
	}, nil)

	scanMetrics := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, metricsSchema)
		defer builder.Release()

		builder.Field(0).(*array.StringBuilder).AppendValues([]string{"page_views", "conversions", "revenue"}, nil)
		builder.Field(1).(*array.Float64Builder).AppendValues([]float64{15234, 342, 45678.90}, nil)
		builder.Field(2).(*array.StringBuilder).AppendValues([]string{
			"2024-01-15T10:00:00Z",
			"2024-01-15T10:00:00Z",
			"2024-01-15T10:00:00Z",
		}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(metricsSchema, []arrow.RecordBatch{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("analytics_data").
		SimpleTable(airport.SimpleTableDef{
			Name:     "metrics",
			Comment:  "Analytics metrics",
			Schema:   metricsSchema,
			ScanFunc: scanMetrics,
		}).
		Build()
	if err != nil {
		log.Fatalf("Failed to build analytics catalog: %v", err)
	}

	return &namedCatalog{Catalog: cat, name: "analytics"}
}

// createInventoryCatalog creates the "inventory" catalog (added dynamically).
func createInventoryCatalog() catalog.Catalog {
	productsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "sku", Type: arrow.BinaryTypes.String},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	scanProducts := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, productsSchema)
		defer builder.Release()

		builder.Field(0).(*array.StringBuilder).AppendValues([]string{"SKU-001", "SKU-002", "SKU-003"}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Widget A", "Widget B", "Gadget X"}, nil)
		builder.Field(2).(*array.Int64Builder).AppendValues([]int64{150, 75, 200}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(productsSchema, []arrow.RecordBatch{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("inventory_data").
		SimpleTable(airport.SimpleTableDef{
			Name:     "products",
			Comment:  "Product inventory",
			Schema:   productsSchema,
			ScanFunc: scanProducts,
		}).
		Build()
	if err != nil {
		log.Fatalf("Failed to build inventory catalog: %v", err)
	}

	return &namedCatalog{Catalog: cat, name: "inventory"}
}
