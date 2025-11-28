package catalog

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Table represents a queryable table or view with fixed schema.
// For tables with dynamic schemas (based on parameters/time), see DynamicSchemaTable.
// Implementations MUST be goroutine-safe.
type Table interface {
	// Name returns the table name (e.g., "users", "orders").
	// MUST return non-empty string.
	Name() string

	// Comment returns optional table documentation.
	// Returns empty string if no comment provided.
	Comment() string

	// ArrowSchema returns the logical schema describing table columns.
	// Returns nil if schema is dynamic (see DynamicSchemaTable interface).
	// If non-nil, MUST return valid *arrow.Schema.
	ArrowSchema() *arrow.Schema

	// Scan executes a scan operation and returns a RecordReader.
	// Context allows cancellation; implementation MUST respect ctx.Done().
	// Caller MUST call reader.Release() to free memory.
	// Returned RecordReader schema MUST match ArrowSchema() (if non-nil).
	Scan(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}

// DynamicSchemaTable extends Table for tables with parameter/time-dependent schemas.
// Used for table functions and time-travel queries.
// Implements DuckDB Airport Extension actions:
//   - table_function_flight_info (schema depends on parameters)
//   - endpoints (schema depends on time point)
type DynamicSchemaTable interface {
	Table

	// SchemaForRequest returns the schema for a specific request.
	// Request contains parameters and/or time point.
	// Used by GetFlightInfo to provide schema without executing full scan.
	// Returns error if parameters are invalid or schema cannot be determined.
	SchemaForRequest(ctx context.Context, req *SchemaRequest) (*arrow.Schema, error)
}
