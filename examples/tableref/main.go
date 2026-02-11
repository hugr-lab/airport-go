// Package main demonstrates a table reference Airport Flight server.
// Table references appear as normal tables in DuckDB but delegate data reading
// to DuckDB function calls (e.g., read_csv, read_parquet) via data:// URIs.
// This allows the server to point DuckDB at external data sources without
// proxying the data through the Flight server.
package main

import (
	"context"
	"log"
	"log/slog"
	"net"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// csvTableRef implements catalog.TableRef to point DuckDB at a CSV file.
// When DuckDB queries this table, it receives a data:// URI containing
// a read_csv function call that DuckDB executes locally.
type csvTableRef struct {
	name    string
	comment string
	schema  *arrow.Schema
	url     string
}

func (t *csvTableRef) Name() string              { return t.name }
func (t *csvTableRef) Comment() string            { return t.comment }
func (t *csvTableRef) ArrowSchema() *arrow.Schema { return t.schema }

func (t *csvTableRef) FunctionCalls(ctx context.Context, req *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
	return []catalog.FunctionCall{
		{
			FunctionName: "read_csv",
			Args: []catalog.FunctionCallArg{
				// Positional arg: file path or URL
				{Value: t.url, Type: arrow.BinaryTypes.String},
				// Named arg: treat first row as header
				{Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
			},
		},
	}, nil
}

// generateSeriesRef implements catalog.TableRef using DuckDB's generate_series.
// This demonstrates how table references can use any DuckDB function.
type generateSeriesRef struct {
	name  string
	start int64
	stop  int64
}

func (t *generateSeriesRef) Name() string    { return t.name }
func (t *generateSeriesRef) Comment() string { return "Generated integer series" }
func (t *generateSeriesRef) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "generate_series", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

func (t *generateSeriesRef) FunctionCalls(ctx context.Context, req *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
	return []catalog.FunctionCall{
		{
			FunctionName: "generate_series",
			Args: []catalog.FunctionCallArg{
				{Value: t.start, Type: arrow.PrimitiveTypes.Int64},
				{Value: t.stop, Type: arrow.PrimitiveTypes.Int64},
			},
		},
	}, nil
}

func main() {
	// Define the schema for the CSV data
	orderSchema := arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "customer", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// Create table references
	ordersRef := &csvTableRef{
		name:    "orders",
		comment: "Order data from CSV file",
		schema:  orderSchema,
		url:     "https://example.com/orders.csv",
	}

	sequenceRef := &generateSeriesRef{
		name:  "sequence",
		start: 1,
		stop:  100,
	}

	// Build catalog with table references
	cat, err := airport.NewCatalogBuilder().
		Schema("data").
		TableRef(ordersRef).
		TableRef(sequenceRef).
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

	log.Println("Airport server listening on :50051")
	log.Println("Table references:")
	log.Println("  - data.orders    (CSV via read_csv)")
	log.Println("  - data.sequence  (generate_series 1..100)")
	log.Println()
	log.Println("Connect from DuckDB:")
	log.Println("  INSTALL airport FROM community;")
	log.Println("  LOAD airport;")
	log.Println("  ATTACH '' AS demo (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');")
	log.Println("  SELECT * FROM demo.data.sequence;")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
