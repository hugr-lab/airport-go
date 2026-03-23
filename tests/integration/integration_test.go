// Package airport provides integration tests for the Flight server using DuckDB.
// These tests verify the server works correctly with the DuckDB Airport extension.
package airport_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"

	_ "github.com/duckdb/duckdb-go/v2"
)

// testServer wraps a Flight server for integration testing.
type testServer struct {
	grpcServer *grpc.Server
	listener   net.Listener
	address    string
}

// newTestServer creates and starts a test Flight server.
func newTestServer(t *testing.T, cat catalog.Catalog, auth airport.Authenticator) *testServer {
	t.Helper()

	// Create listener on random port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	// Configure server with debug logging for tests
	debugLevel := slog.LevelDebug
	config := airport.ServerConfig{
		Catalog:  cat,
		Auth:     auth,
		Address:  lis.Addr().String(), // Pass server address for FlightEndpoint locations
		LogLevel: &debugLevel,
	}

	opts := airport.ServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	if err := airport.NewServer(grpcServer, config); err != nil {
		t.Fatalf("Failed to register server: %v", err)
	}

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	return &testServer{
		grpcServer: grpcServer,
		listener:   lis,
		address:    lis.Addr().String(),
	}
}

// stop gracefully stops the test server.
func (s *testServer) stop() {
	s.grpcServer.GracefulStop()
	s.listener.Close()
}

// openDuckDB opens a DuckDB connection with the Airport extension loaded.
// Note: This requires DuckDB to be installed with the Airport extension.
func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()

	// Open in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("DuckDB not available: %v", err)
	}

	// Try to install and load Airport extension
	// This will fail if DuckDB or Airport extension is not installed
	_, err = db.Exec("INSTALL airport FROM community")
	if err != nil {
		t.Fatalf("Airport extension not available: %v", err)
	}

	_, err = db.Exec("LOAD airport")
	if err != nil {
		t.Fatalf("Failed to load Airport extension: %v", err)
	}

	return db
}

// connectToFlightServer attaches a Flight server to DuckDB.
// Returns the attachment name for use in queries.
//
//nolint:unparam
func connectToFlightServer(t *testing.T, db *sql.DB, address string, token string) string {
	t.Helper()

	attachName := "test_flight"

	// Create secret if token provided
	if token == "" {
		// Attach without auth
		query := fmt.Sprintf("ATTACH '%s' AS %s (TYPE airport, LOCATION 'grpc://%s')", "", attachName, address)
		_, err := db.Exec(query)
		if err != nil {
			t.Fatalf("Failed to attach Flight server: %v", err)
		}
		return attachName
	}
	secretQuery := fmt.Sprintf("CREATE OR REPLACE SECRET airport_test_secret (TYPE AIRPORT, auth_token '%s', scope 'grpc://%s')", token, address)
	_, err := db.Exec(secretQuery)
	if err != nil {
		t.Fatalf("Failed to create secret: %v", err)
	}

	// Attach with secret
	query := fmt.Sprintf("ATTACH '%s' AS %s (TYPE airport, SECRET airport_test_secret, LOCATION 'grpc://%s')", "", attachName, address)
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("Failed to attach Flight server: %v", err)
	}

	return attachName
}

// simpleCatalog creates a basic catalog for testing.
func simpleCatalog() catalog.Catalog {
	// Add users table
	usersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
	}, nil)

	usersData := [][]any{
		{int64(1), "Alice", "alice@example.com"},
		{int64(2), "Bob", "bob@example.com"},
		{int64(3), "Charlie", "charlie@example.com"},
	}

	// Add products table
	productsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	productsData := [][]any{
		{int64(101), "Widget", 9.99},
		{int64(102), "Gadget", 19.99},
		{int64(103), "Doohickey", 29.99},
	}

	// Build catalog
	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("Test schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "User accounts",
			Schema:   usersSchema,
			ScanFunc: makeScanFunc(usersSchema, usersData),
		}).
		SimpleTable(airport.SimpleTableDef{
			Name:     "products",
			Comment:  "Product catalog",
			Schema:   productsSchema,
			ScanFunc: makeScanFunc(productsSchema, productsData),
		}).
		Build()

	if err != nil {
		panic(fmt.Sprintf("Failed to build catalog: %v", err))
	}

	return cat
}

// authenticatedCatalog creates a catalog with authentication required.
func authenticatedCatalog() catalog.Catalog {
	// Add sensitive data table
	secretsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	secretsData := [][]any{
		{"api_key", "secret123"},
		{"db_password", "pass456"},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("secure").
		Comment("Secure schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "secrets",
			Comment:  "Sensitive configuration",
			Schema:   secretsSchema,
			ScanFunc: makeScanFunc(secretsSchema, secretsData),
		}).
		Build()

	if err != nil {
		panic(fmt.Sprintf("Failed to build catalog: %v", err))
	}

	return cat
}

// testAuthHandler creates a simple bearer token auth handler for testing.
func testAuthHandler() airport.Authenticator {
	return airport.BearerAuth(func(token string) (string, error) {
		validTokens := map[string]string{
			"valid-token": "test-user",
			"admin-token": "admin",
		}

		if identity, ok := validTokens[token]; ok {
			return identity, nil
		}

		return "", airport.ErrUnauthorized
	})
}

// buildTestRecord creates an Arrow record from test data.
func buildTestRecord(schema *arrow.Schema, data [][]any) arrow.RecordBatch {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, row := range data {
		for i, val := range row {
			field := schema.Field(i)
			switch field.Type.ID() {
			case arrow.BOOL:
				builder.Field(i).(*array.BooleanBuilder).Append(val.(bool))
			case arrow.INT8:
				builder.Field(i).(*array.Int8Builder).Append(val.(int8))
			case arrow.INT16:
				builder.Field(i).(*array.Int16Builder).Append(val.(int16))
			case arrow.INT32:
				builder.Field(i).(*array.Int32Builder).Append(val.(int32))
			case arrow.INT64:
				builder.Field(i).(*array.Int64Builder).Append(val.(int64))
			case arrow.UINT8:
				builder.Field(i).(*array.Uint8Builder).Append(val.(uint8))
			case arrow.UINT16:
				builder.Field(i).(*array.Uint16Builder).Append(val.(uint16))
			case arrow.UINT32:
				builder.Field(i).(*array.Uint32Builder).Append(val.(uint32))
			case arrow.UINT64:
				builder.Field(i).(*array.Uint64Builder).Append(val.(uint64))
			case arrow.FLOAT32:
				builder.Field(i).(*array.Float32Builder).Append(val.(float32))
			case arrow.FLOAT64:
				builder.Field(i).(*array.Float64Builder).Append(val.(float64))
			case arrow.STRING:
				builder.Field(i).(*array.StringBuilder).Append(val.(string))
			case arrow.BINARY:
				builder.Field(i).(*array.BinaryBuilder).Append(val.([]byte))
			case arrow.FIXED_SIZE_BINARY:
				builder.Field(i).(*array.FixedSizeBinaryBuilder).Append(val.([]byte))
			case arrow.DATE32:
				builder.Field(i).(*array.Date32Builder).Append(arrow.Date32(val.(int32)))
			case arrow.TIMESTAMP:
				builder.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(val.(int64)))
			case arrow.EXTENSION:
				switch b := builder.Field(i).(type) {
				case *catalog.GeometryBuilder:
					b.Append(val.(orb.Geometry))
				default:
					// For unsupported extension types, skip or panic
					panic("unsupported extension type in buildTestRecord: " + field.Type.String())
				}
			default:
				// For unsupported types, skip or panic
				panic("unsupported Arrow type in buildTestRecord: " + field.Type.String())
			}
		}
	}

	return builder.NewRecordBatch()
}

// makeScanFunc creates a ScanFunc from in-memory data.
func makeScanFunc(schema *arrow.Schema, data [][]any) catalog.ScanFunc {
	return func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		record := buildTestRecord(schema, data)
		defer record.Release()
		return array.NewRecordReader(schema, []arrow.RecordBatch{record})
	}
}
