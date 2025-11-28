// Package main demonstrates a basic Airport Flight server.
// This example creates a server with a single "users" table containing in-memory data.
package main

import (
	"context"
	"log"
	"log/slog"
	"net"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Define schema for users table
	userSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	// Define scan function that returns in-memory data
	scanUsers := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		// Create in-memory data
		builder := array.NewRecordBuilder(memory.DefaultAllocator, userSchema)
		defer builder.Release()

		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(userSchema, []arrow.RecordBatch{record})
	}

	// Build catalog with a single schema and table
	cat, err := airport.NewCatalogBuilder().
		Schema("main").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "User accounts",
			Schema:   userSchema,
			ScanFunc: scanUsers,
		}).
		Build()
	if err != nil {
		log.Fatalf("Failed to build catalog: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register Airport handlers with debug logging
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
	log.Println("Example catalog contains:")
	log.Println("  - Schema: main")
	log.Println("    - Table: users (3 rows)")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
