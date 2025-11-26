// Package airport provides a high-level API for building Apache Arrow Flight servers
// compatible with the DuckDB Airport Extension.
//
// The airport package simplifies building Flight servers by:
//   - Registering Flight service handlers on an existing grpc.Server
//   - Providing a fluent catalog builder API for defining schemas and tables
//   - Supporting dynamic catalog implementations via interfaces
//   - Handling authentication with bearer tokens
//   - Streaming Arrow data efficiently without rebatching
//
// # Quick Start
//
// Build a basic Flight server in under 30 lines:
//
//	package main
//
//	import (
//	    "context"
//	    "log"
//	    "net"
//
//	    "github.com/apache/arrow/go/v18/arrow"
//	    "github.com/apache/arrow/go/v18/arrow/array"
//	    "github.com/apache/arrow/go/v18/arrow/memory"
//	    "google.golang.org/grpc"
//
//	    "github.com/hugr-lab/airport-go"
//	    "github.com/hugr-lab/airport-go/catalog"
//	)
//
//	func main() {
//	    // Define schema and scan function
//	    userSchema := arrow.NewSchema([]arrow.Field{
//	        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
//	        {Name: "name", Type: arrow.BinaryTypes.String},
//	    }, nil)
//
//	    scanUsers := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
//	        builder := array.NewRecordBuilder(memory.DefaultAllocator, userSchema)
//	        defer builder.Release()
//	        builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
//	        builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
//	        record := builder.NewRecord()
//	        defer record.Release()
//	        return array.NewRecordReader(userSchema, []arrow.Record{record})
//	    }
//
//	    // Build catalog
//	    cat, _ := airport.NewCatalogBuilder().
//	        Schema("main").
//	            SimpleTable(airport.SimpleTableDef{
//	                Name:     "users",
//	                Schema:   userSchema,
//	                ScanFunc: scanUsers,
//	            }).
//	        Build()
//
//	    // Register handlers and start server
//	    grpcServer := grpc.NewServer()
//	    airport.NewServer(grpcServer, airport.ServerConfig{Catalog: cat})
//	    lis, _ := net.Listen("tcp", ":50051")
//	    log.Println("Airport server listening on :50051")
//	    grpcServer.Serve(lis)
//	}
//
// # Architecture
//
// The package follows an interface-based design:
//
//   - Catalog: Top-level interface for querying schemas
//   - Schema: Interface for querying tables and functions
//   - Table: Interface providing Arrow schema and scan function
//   - ScalarFunction: Interface for custom scalar functions
//
// Users can either:
//   - Use the CatalogBuilder fluent API for static catalogs
//   - Implement the Catalog interface for dynamic catalogs
//
// # Server Lifecycle
//
// The package registers Flight service handlers on a user-provided grpc.Server
// but does NOT manage server lifecycle (start/stop/listen). This gives users
// full control over:
//   - TLS configuration via grpc.Creds()
//   - Server options and interceptors
//   - Graceful shutdown via grpcServer.GracefulStop()
//
// # Authentication
//
// Bearer token authentication is supported via the BearerAuth helper:
//
//	auth := airport.BearerAuth(func(token string) (string, error) {
//	    if token == "secret-api-key" {
//	        return "user1", nil
//	    }
//	    return "", airport.ErrUnauthorized
//	})
//
//	airport.NewServer(grpcServer, airport.ServerConfig{
//	    Catalog: cat,
//	    Auth:    auth,
//	})
//
// # Logging
//
// The package uses log/slog.Default() for all internal logging. Users can
// configure logging by calling slog.SetDefault() before package initialization.
//
// # Context Cancellation
//
// All long-running operations (scan functions, catalog queries, streaming)
// support context-based cancellation. The package respects ctx.Done() and
// stops work immediately when clients disconnect.
//
// # Memory Management
//
// Arrow uses manual reference counting. Callers MUST call Release() on:
//   - RecordReaders returned by scan functions
//   - Records and Arrays created during processing
//
// Use defer record.Release() immediately after creation to ensure cleanup.
package airport
