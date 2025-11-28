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

// InsertableTable extends Table with INSERT capability.
// Tables implement this interface to accept new rows via DoPut.
// Implementations MUST be goroutine-safe.
type InsertableTable interface {
	Table

	// Insert adds new rows to the table.
	// The rows RecordReader provides batches of data to insert.
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	// Caller MUST call rows.Release() after Insert returns.
	Insert(ctx context.Context, rows array.RecordReader) (*DMLResult, error)
}

// UpdatableTable extends Table with UPDATE capability.
// Tables must have a rowid mechanism to identify rows for update.
// Implementations MUST be goroutine-safe.
type UpdatableTable interface {
	Table

	// Update modifies existing rows identified by rowIDs.
	// The rows RecordReader provides replacement data for matched rows.
	// Row order in RecordReader must correspond to rowIDs order.
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*DMLResult, error)
}

// DeletableTable extends Table with DELETE capability.
// Tables must have a rowid mechanism to identify rows for deletion.
// Implementations MUST be goroutine-safe.
type DeletableTable interface {
	Table

	// Delete removes rows identified by rowIDs.
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	Delete(ctx context.Context, rowIDs []int64) (*DMLResult, error)
}
