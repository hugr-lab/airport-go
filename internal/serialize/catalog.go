// Package serialize provides catalog serialization to Arrow IPC format.
// Used by ListFlights RPC to serialize and compress catalog metadata.
package serialize

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
)

// SerializeCatalog serializes a catalog to Arrow IPC format following Flight SQL schema.
// Returns compressed bytes containing catalog metadata.
// The format follows Flight SQL standard for catalog discovery:
//   - GetTables schema: catalog_name, db_schema_name, table_name, table_type
func SerializeCatalog(ctx context.Context, cat catalog.Catalog, allocator memory.Allocator) ([]byte, error) {
	// Get all schemas from catalog
	schemas, err := cat.Schemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get schemas: %w", err)
	}

	// Build GetTables response following Flight SQL format
	// Schema: catalog_name, db_schema_name, table_name, table_type
	tablesSchema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// Build record with table metadata
	builder := array.NewRecordBuilder(allocator, tablesSchema)
	defer builder.Release()

	catalogNameBuilder := builder.Field(0).(*array.StringBuilder)
	schemaNameBuilder := builder.Field(1).(*array.StringBuilder)
	tableNameBuilder := builder.Field(2).(*array.StringBuilder)
	tableTypeBuilder := builder.Field(3).(*array.StringBuilder)

	// Iterate through schemas and tables
	for _, schema := range schemas {
		tables, err := schema.Tables(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get tables for schema %s: %w", schema.Name(), err)
		}

		for _, table := range tables {
			catalogNameBuilder.AppendNull() // No catalog concept in Airport
			schemaNameBuilder.Append(schema.Name())
			tableNameBuilder.Append(table.Name())
			tableTypeBuilder.Append("TABLE") // All are tables in Airport
		}
	}

	record := builder.NewRecord()
	defer record.Release()

	// Serialize to Arrow IPC format
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(tablesSchema), ipc.WithAllocator(allocator))
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write IPC record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close IPC writer: %w", err)
	}

	return buf.Bytes(), nil
}

// CompressCatalog compresses serialized catalog data using ZStandard.
func CompressCatalog(data []byte) ([]byte, error) {
	compressor, err := NewCompressor()
	if err != nil {
		return nil, err
	}
	defer compressor.Close()

	return compressor.Compress(data)
}
