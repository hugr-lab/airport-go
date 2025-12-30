// Package main demonstrates an Airport Flight server with filter pushdown support.
// This example shows how to parse and use the filter JSON that DuckDB sends
// when executing queries with WHERE clauses.
//
// To test with DuckDB CLI:
//
//	duckdb
//	INSTALL airport FROM community;
//	LOAD airport;
//	ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
//
//	-- Filter pushdown examples:
//	SELECT * FROM demo.products.items WHERE price > 100;
//	SELECT * FROM demo.products.items WHERE category = 'electronics' AND price <= 500;
//	SELECT * FROM demo.products.items WHERE name LIKE 'Pro%';
//	SELECT * FROM demo.products.items WHERE id IN (1, 3, 5);
//	SELECT * FROM demo.products.items WHERE price BETWEEN 50 AND 200;
//
// The server will log the parsed filter and generated SQL for each query.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/filter"
)

func main() {
	// Create items table with filter pushdown support
	table := NewItemsTable()

	// Build catalog
	cat, err := airport.NewCatalogBuilder().
		Schema("products").
		Table(table).
		Build()
	if err != nil {
		log.Fatalf("Failed to build catalog: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register Airport handlers
	debugLevel := slog.LevelDebug
	err = airport.NewServer(grpcServer, airport.ServerConfig{
		Catalog:  cat,
		LogLevel: &debugLevel,
	})
	if err != nil {
		log.Fatalf("Failed to register Airport server: %v", err)
	}

	// Start serving
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Airport Filter Pushdown server listening on :50051")
	log.Println("")
	log.Println("Example catalog contains:")
	log.Println("  - Schema: products")
	log.Println("    - Table: items (with filter pushdown)")
	log.Println("")
	log.Println("Test with DuckDB CLI:")
	log.Println("  ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');")
	log.Println("")
	log.Println("  -- Simple comparisons:")
	log.Println("  SELECT * FROM demo.products.items WHERE price > 100;")
	log.Println("  SELECT * FROM demo.products.items WHERE category = 'electronics';")
	log.Println("")
	log.Println("  -- Conjunctions:")
	log.Println("  SELECT * FROM demo.products.items WHERE category = 'electronics' AND price <= 500;")
	log.Println("  SELECT * FROM demo.products.items WHERE price < 50 OR price > 200;")
	log.Println("")
	log.Println("  -- Range queries:")
	log.Println("  SELECT * FROM demo.products.items WHERE price BETWEEN 50 AND 200;")
	log.Println("  SELECT * FROM demo.products.items WHERE id IN (1, 3, 5);")
	log.Println("")
	log.Println("The server logs will show the parsed filter and generated SQL.")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// =============================================================================
// ItemsTable Implementation with Filter Pushdown
// =============================================================================

// ItemsTable is an in-memory table that demonstrates filter pushdown parsing.
// In a real implementation, you would use the parsed filter to optimize
// your backend query (e.g., add WHERE clause to database query).
type ItemsTable struct {
	schema *arrow.Schema
	alloc  memory.Allocator
	mu     sync.RWMutex
	data   []Item
}

// Item represents a product item.
type Item struct {
	ID       int64
	Name     string
	Category string
	Price    float64
	InStock  bool
}

// NewItemsTable creates a new items table with sample data.
func NewItemsTable() *ItemsTable {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "in_stock", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)

	return &ItemsTable{
		schema: schema,
		alloc:  memory.DefaultAllocator,
		data: []Item{
			{ID: 1, Name: "Laptop Pro", Category: "electronics", Price: 1299.99, InStock: true},
			{ID: 2, Name: "Wireless Mouse", Category: "electronics", Price: 29.99, InStock: true},
			{ID: 3, Name: "USB Cable", Category: "electronics", Price: 9.99, InStock: true},
			{ID: 4, Name: "Desk Chair", Category: "furniture", Price: 249.99, InStock: true},
			{ID: 5, Name: "Standing Desk", Category: "furniture", Price: 599.99, InStock: false},
			{ID: 6, Name: "Monitor 27\"", Category: "electronics", Price: 399.99, InStock: true},
			{ID: 7, Name: "Keyboard Mechanical", Category: "electronics", Price: 149.99, InStock: true},
			{ID: 8, Name: "Bookshelf", Category: "furniture", Price: 89.99, InStock: true},
			{ID: 9, Name: "Headphones Pro", Category: "electronics", Price: 299.99, InStock: false},
			{ID: 10, Name: "Coffee Table", Category: "furniture", Price: 179.99, InStock: true},
		},
	}
}

// Table interface implementation

func (t *ItemsTable) Name() string    { return "items" }
func (t *ItemsTable) Comment() string { return "Product items table with filter pushdown support" }
func (t *ItemsTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

// Scan implements catalog.Table. It demonstrates filter pushdown by parsing
// the filter JSON and logging the SQL representation.
func (t *ItemsTable) Scan(_ context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Process filter pushdown if provided
	if opts != nil && len(opts.Filter) > 0 {
		t.processFilter(opts.Filter)
	} else {
		fmt.Println("[ItemsTable] Scan called without filter (full table scan)")
	}

	// Build result - in a real implementation, you would apply the filter
	// to your backend storage here
	record := t.buildRecord()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

// processFilter parses the filter JSON and logs the SQL representation.
// This demonstrates how to use the filter package.
func (t *ItemsTable) processFilter(filterJSON []byte) {
	fmt.Println("")
	fmt.Println("========== Filter Pushdown Received ==========")

	// Parse the filter JSON
	fp, err := filter.Parse(filterJSON)
	if err != nil {
		fmt.Printf("[ItemsTable] Failed to parse filter: %v\n", err)
		fmt.Printf("[ItemsTable] Raw JSON: %s\n", string(filterJSON))
		return
	}

	fmt.Printf("[ItemsTable] Parsed %d filter expression(s)\n", len(fp.Filters))
	fmt.Printf("[ItemsTable] Column bindings: %v\n", fp.ColumnBindings)

	// Create encoder and generate SQL
	enc := filter.NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	fmt.Printf("[ItemsTable] Generated SQL WHERE clause: %s\n", sql)

	// Demonstrate expression inspection
	fmt.Println("")
	fmt.Println("Expression tree:")
	for i, expr := range fp.Filters {
		fmt.Printf("  Filter %d: %s\n", i+1, expr.Class())
		inspectExpression(expr, "    ")
	}
	fmt.Println("===============================================")
	fmt.Println("")

	// In a real implementation, you could:
	// 1. Use the SQL directly if your backend is SQL-based
	// 2. Walk the expression tree to build a native query
	// 3. Use column mapping to translate column names:
	//
	//    enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
	//        ColumnMapping: map[string]string{
	//            "name": "product_name",
	//            "category": "product_category",
	//        },
	//    })
	//
	// 4. Replace columns with expressions for computed columns:
	//
	//    enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
	//        ColumnExpressions: map[string]string{
	//            "full_name": "CONCAT(first_name, ' ', last_name)",
	//        },
	//    })
}

// inspectExpression recursively prints expression structure for debugging.
func inspectExpression(expr filter.Expression, indent string) {
	switch e := expr.(type) {
	case *filter.ComparisonExpression:
		fmt.Printf("%s%s\n", indent, e.Type())
		fmt.Printf("%s  left:\n", indent)
		inspectExpression(e.Left, indent+"    ")
		fmt.Printf("%s  right:\n", indent)
		inspectExpression(e.Right, indent+"    ")

	case *filter.ConjunctionExpression:
		fmt.Printf("%s%s (%d children)\n", indent, e.Type(), len(e.Children))
		for i, child := range e.Children {
			fmt.Printf("%s  child %d:\n", indent, i+1)
			inspectExpression(child, indent+"    ")
		}

	case *filter.ConstantExpression:
		fmt.Printf("%sConstant: %v (type: %s)\n", indent, e.Value.Data, e.Value.Type.ID)

	case *filter.ColumnRefExpression:
		fmt.Printf("%sColumn: index=%d (type: %s)\n", indent, e.Binding.ColumnIndex, e.ReturnType.ID)

	case *filter.FunctionExpression:
		fmt.Printf("%sFunction: %s (%d args)\n", indent, e.Name, len(e.Children))
		for i, child := range e.Children {
			fmt.Printf("%s  arg %d:\n", indent, i+1)
			inspectExpression(child, indent+"    ")
		}

	case *filter.CastExpression:
		fmt.Printf("%sCast to %s\n", indent, e.ReturnType.ID)
		inspectExpression(e.Child, indent+"  ")

	case *filter.BetweenExpression:
		fmt.Printf("%sBETWEEN\n", indent)

	case *filter.OperatorExpression:
		fmt.Printf("%sOperator: %s (%d children)\n", indent, e.Type(), len(e.Children))

	default:
		fmt.Printf("%s%T\n", indent, expr)
	}
}

// buildRecord creates an Arrow record from in-memory data.
func (t *ItemsTable) buildRecord() arrow.RecordBatch {
	builder := array.NewRecordBuilder(t.alloc, t.schema)
	defer builder.Release()

	for _, item := range t.data {
		builder.Field(0).(*array.Int64Builder).Append(item.ID)
		builder.Field(1).(*array.StringBuilder).Append(item.Name)
		builder.Field(2).(*array.StringBuilder).Append(item.Category)
		builder.Field(3).(*array.Float64Builder).Append(item.Price)
		builder.Field(4).(*array.BooleanBuilder).Append(item.InStock)
	}

	return builder.NewRecordBatch()
}
