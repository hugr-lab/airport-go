// Package main demonstrates a dynamic Airport Flight catalog.
// Unlike the static catalog builder, this example shows how to implement
// the Catalog interface with custom logic that can change at runtime.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// DynamicCatalog implements a catalog that can change at runtime.
// This demonstrates permission-based filtering and live schema updates.
type DynamicCatalog struct {
	mu      sync.RWMutex
	schemas map[string]*DynamicSchema
}

func NewDynamicCatalog() *DynamicCatalog {
	return &DynamicCatalog{
		schemas: make(map[string]*DynamicSchema),
	}
}

// AddSchema adds a schema to the dynamic catalog (safe for concurrent use).
func (c *DynamicCatalog) AddSchema(name string, schema *DynamicSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[name] = schema
}

// Schemas implements catalog.Catalog.
func (c *DynamicCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get authenticated user from context
	identity := airport.IdentityFromContext(ctx)

	var result []catalog.Schema
	for _, schema := range c.schemas {
		// Filter based on permissions
		if schema.canAccess(identity) {
			result = append(result, schema)
		}
	}

	return result, nil
}

// Schema implements catalog.Catalog.
func (c *DynamicCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, ok := c.schemas[name]
	if !ok {
		return nil, nil
	}

	// Check permissions
	identity := airport.IdentityFromContext(ctx)
	if !schema.canAccess(identity) {
		return nil, nil // Act as if schema doesn't exist
	}

	return schema, nil
}

// DynamicSchema implements a schema with permission checks.
type DynamicSchema struct {
	name         string
	comment      string
	mu           sync.RWMutex
	tables       map[string]catalog.Table
	allowedUsers []string // Empty = allow all
}

func NewDynamicSchema(name, comment string, allowedUsers []string) *DynamicSchema {
	return &DynamicSchema{
		name:         name,
		comment:      comment,
		tables:       make(map[string]catalog.Table),
		allowedUsers: allowedUsers,
	}
}

// AddTable adds a table to the schema (safe for concurrent use).
func (s *DynamicSchema) AddTable(table catalog.Table) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[table.Name()] = table
}

func (s *DynamicSchema) canAccess(identity string) bool {
	if len(s.allowedUsers) == 0 {
		return true // No restrictions
	}

	for _, allowed := range s.allowedUsers {
		if allowed == identity {
			return true
		}
	}
	return false
}

// Name implements catalog.Schema.
func (s *DynamicSchema) Name() string {
	return s.name
}

// Comment implements catalog.Schema.
func (s *DynamicSchema) Comment() string {
	return s.comment
}

// Tables implements catalog.Schema.
func (s *DynamicSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []catalog.Table
	for _, table := range s.tables {
		result = append(result, table)
	}
	return result, nil
}

// Table implements catalog.Schema.
func (s *DynamicSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	table, ok := s.tables[name]
	if !ok {
		return nil, nil
	}
	return table, nil
}

// ScalarFunctions implements catalog.Schema.
func (s *DynamicSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	return nil, nil
}

// TableFunctions implements catalog.Schema.
func (s *DynamicSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	return nil, nil
}

// LiveTable demonstrates a table with data that changes over time.
type LiveTable struct {
	name    string
	comment string
	schema  *arrow.Schema
	getData func() [][]interface{} // Function that returns current data
}

func (t *LiveTable) Name() string {
	return t.name
}

func (t *LiveTable) Comment() string {
	return t.comment
}

func (t *LiveTable) ArrowSchema() *arrow.Schema {
	return t.schema
}

func (t *LiveTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	// Get current data
	data := t.getData()

	// Build Arrow record
	builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
	defer builder.Release()

	// Populate builder with current data
	for _, row := range data {
		for i, val := range row {
			switch t.schema.Field(i).Type {
			case arrow.PrimitiveTypes.Int64:
				builder.Field(i).(*array.Int64Builder).Append(val.(int64))
			case arrow.BinaryTypes.String:
				builder.Field(i).(*array.StringBuilder).Append(val.(string))
			}
		}
	}

	record := builder.NewRecord()
	defer record.Release()

	return array.NewRecordReader(t.schema, []arrow.Record{record})
}

func main() {
	// Create dynamic catalog
	cat := NewDynamicCatalog()

	// Create public schema (accessible to everyone)
	publicSchema := NewDynamicSchema("public", "Public data", nil)

	// Create metrics table with live data
	metricsSchema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "metric", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	startTime := time.Now()
	metricsTable := &LiveTable{
		name:    "metrics",
		comment: "Live server metrics",
		schema:  metricsSchema,
		getData: func() [][]interface{} {
			// Return current metrics
			elapsed := time.Since(startTime).Seconds()
			return [][]interface{}{
				{int64(time.Now().Unix()), "uptime_seconds", int64(elapsed)},
				{int64(time.Now().Unix()), "request_count", int64(100)},
			}
		},
	}

	publicSchema.AddTable(metricsTable)
	cat.AddSchema("public", publicSchema)

	// Create admin schema (only accessible to admin users)
	adminSchema := NewDynamicSchema("admin", "Admin-only data", []string{"admin"})

	// Admin gets additional privileged tables
	adminTableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "setting", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	adminTable := &LiveTable{
		name:    "settings",
		comment: "Server configuration",
		schema:  adminTableSchema,
		getData: func() [][]interface{} {
			return [][]interface{}{
				{"max_connections", "1000"},
				{"log_level", "info"},
			}
		},
	}

	adminSchema.AddTable(adminTable)
	cat.AddSchema("admin", adminSchema)

	// Create gRPC server with authentication
	config := airport.ServerConfig{
		Catalog: cat,
		Auth: airport.BearerAuth(func(token string) (string, error) {
			// Simple token validation
			validTokens := map[string]string{
				"admin-token": "admin",
				"user-token":  "user",
			}
			if identity, ok := validTokens[token]; ok {
				return identity, nil
			}
			return "", airport.ErrUnauthorized
		}),
	}

	opts := airport.ServerOptions(config)
	grpcServer := grpc.NewServer(opts...)

	if err := airport.NewServer(grpcServer, config); err != nil {
		log.Fatalf("Failed to register server: %v", err)
	}

	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	fmt.Println("Dynamic Airport server listening on :50053")
	fmt.Println("")
	fmt.Println("Catalog structure:")
	fmt.Println("  - public schema (accessible to all)")
	fmt.Println("    - metrics table (live data)")
	fmt.Println("  - admin schema (admin only)")
	fmt.Println("    - settings table")
	fmt.Println("")
	fmt.Println("Valid tokens:")
	fmt.Println("  - admin-token (sees both schemas)")
	fmt.Println("  - user-token (sees only public schema)")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
