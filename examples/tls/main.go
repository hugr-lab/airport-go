// Package main demonstrates how to configure TLS for secure Flight connections.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Create a simple catalog with sample data
	cat, err := createSampleCatalog()
	if err != nil {
		log.Fatalf("Failed to create catalog: %v", err)
	}

	// Load TLS credentials
	creds, err := loadTLSCredentials()
	if err != nil {
		log.Fatalf("Failed to load TLS credentials: %v", err)
	}

	// Configure server with TLS
	config := airport.ServerConfig{
		Catalog: cat,
	}

	// Create gRPC server with TLS
	opts := append(
		airport.ServerOptions(config),
		grpc.Creds(creds),
	)
	grpcServer := grpc.NewServer(opts...)

	// Register Airport Flight service
	if err := airport.NewServer(grpcServer, config); err != nil {
		log.Fatalf("Failed to register server: %v", err)
	}

	// Start listening
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Airport Flight server (TLS) listening on %s", lis.Addr())
	log.Printf("Connect with: ATTACH 'tls://localhost:50051' AS my_data (TYPE airport)")

	// Serve (blocks until shutdown)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

// loadTLSCredentials loads TLS credentials from files.
// In production, use proper certificate management.
func loadTLSCredentials() (credentials.TransportCredentials, error) {
	// Load server certificate and key
	serverCert, err := tls.LoadX509KeyPair("server-cert.pem", "server-key.pem")
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert: %w", err)
	}

	// Load CA certificate for mutual TLS (optional)
	certPool := x509.NewCertPool()
	if caCert, err := os.ReadFile("ca-cert.pem"); err == nil {
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to add CA cert to pool")
		}
	}

	// Configure TLS
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.NoClientCert, // Change to tls.RequireAndVerifyClientCert for mTLS
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS12,
	}

	return credentials.NewTLS(tlsConfig), nil
}

// createSampleCatalog creates a catalog with sample data.
func createSampleCatalog() (catalog.Catalog, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "message", Type: arrow.BinaryTypes.String},
	}, nil)

	data := [][]interface{}{
		{int64(1), "Secure connection established"},
		{int64(2), "TLS encryption active"},
	}

	return airport.NewCatalogBuilder().
		Schema("secure").
		Comment("Secure schema with TLS").
		SimpleTable(airport.SimpleTableDef{
			Name:    "messages",
			Comment: "Secure messages table",
			Schema:  schema,
			ScanFunc: func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
				record := buildRecord(schema, data)
				return array.NewRecordReader(schema, []arrow.Record{record})
			},
		}).
		Build()
}

// buildRecord creates an Arrow record from test data.
func buildRecord(schema *arrow.Schema, data [][]interface{}) arrow.Record {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, row := range data {
		builder.Field(0).(*array.Int64Builder).Append(row[0].(int64))
		builder.Field(1).(*array.StringBuilder).Append(row[1].(string))
	}

	return builder.NewRecord()
}
