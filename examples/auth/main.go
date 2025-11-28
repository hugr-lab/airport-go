// Package main demonstrates Airport Flight server with bearer token authentication.
// This example shows how to configure authentication and require valid tokens for requests.
package main

import (
	"context"
	"fmt"
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

// Simulated user database for demo purposes
var validTokens = map[string]string{
	"secret-admin-token": "admin",
	"secret-user1-token": "user1",
	"secret-user2-token": "user2",
	"secret-guest-token": "guest",
}

// validateToken checks if a bearer token is valid and returns user identity.
// In production, this would call your authentication backend.
func validateToken(token string) (string, error) {
	identity, ok := validTokens[token]
	if !ok {
		return "", airport.ErrUnauthorized
	}
	return identity, nil
}

func main() {
	// Define schema for users table
	userSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
	}, nil)

	// Define scan function that returns in-memory data
	scanUsers := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		// Create in-memory data
		builder := array.NewRecordBuilder(memory.DefaultAllocator, userSchema)
		defer builder.Release()

		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
		builder.Field(2).(*array.StringBuilder).AppendValues([]string{"alice@example.com", "bob@example.com", "charlie@example.com"}, nil)

		record := builder.NewRecordBatch()
		defer record.Release()

		return array.NewRecordReader(userSchema, []arrow.RecordBatch{record})
	}

	// Build catalog with authentication-aware table
	cat, err := airport.NewCatalogBuilder().
		Schema("main").
		Comment("Main application schema - requires authentication").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "User accounts - authenticated access only",
			Schema:   userSchema,
			ScanFunc: scanUsers,
		}).
		Build()
	if err != nil {
		log.Fatalf("Failed to build catalog: %v", err)
	}

	// Create server configuration with bearer token authentication and debug logging
	debugLevel := slog.LevelDebug
	config := airport.ServerConfig{
		Catalog:  cat,
		Auth:     airport.BearerAuth(validateToken),
		LogLevel: &debugLevel,
	}

	// Create gRPC server with authentication interceptors
	// IMPORTANT: ServerOptions must be used when creating the gRPC server
	opts := airport.ServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	// Register Airport handlers
	err = airport.NewServer(grpcServer, config)
	if err != nil {
		log.Fatalf("Failed to register Airport server: %v", err)
	}

	// Start serving
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Authenticated Airport server listening on :50052")
	log.Println("Example catalog contains:")
	log.Println("  - Schema: main (requires authentication)")
	log.Println("    - Table: users (3 rows)")
	log.Println("")
	log.Println("Valid bearer tokens for testing:")
	for token, identity := range validTokens {
		fmt.Printf("  - %s (identity: %s)\n", token, identity)
	}
	log.Println("")
	log.Println("Test with:")
	log.Println("  grpcurl -H 'authorization: Bearer secret-admin-token' \\")
	log.Println("    -plaintext localhost:50052 \\")
	log.Println("    arrow.flight.protocol.FlightService/ListFlights")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
