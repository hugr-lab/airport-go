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
	// If columns is nil or empty, returns full schema.
	// If columns is provided, returns projected schema with only those columns.
	// Column order in the returned schema matches the order in columns slice.
	// Returns nil if schema is dynamic (see DynamicSchemaTable interface).
	// If non-nil, MUST return valid *arrow.Schema.
	ArrowSchema(columns []string) *arrow.Schema

	// Scan executes a scan operation and returns a RecordReader.
	// Context allows cancellation; implementation MUST respect ctx.Done().
	// Caller MUST call reader.Release() to free memory.
	// If opts.Columns is specified, implementation MAY return either:
	//   - Full schema records (server will apply projection)
	//   - Projected schema records (optimization)
	// Either way, returned data will match what client expects.
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
// Tables implement this interface to accept new rows via DoExchange.
// Implementations MUST be goroutine-safe.
type InsertableTable interface {
	Table

	// Insert adds new rows to the table.
	// The rows RecordReader provides batches of data to insert.
	// The opts parameter provides options including RETURNING clause information:
	//   - opts.Returning: true if RETURNING clause was specified
	//   - opts.ReturningColumns: column names to include in RETURNING results
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	// Caller MUST call rows.Release() after Insert returns.
	Insert(ctx context.Context, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}

// UpdatableTable extends Table with UPDATE capability.
// Tables must have a rowid mechanism to identify rows for update.
// Implementations MUST be goroutine-safe.
type UpdatableTable interface {
	Table

	// Update modifies existing rows identified by rowIDs.
	// The rows RecordReader provides replacement data for matched rows.
	// Row order in RecordReader must correspond to rowIDs order.
	// The opts parameter provides options including RETURNING clause information:
	//   - opts.Returning: true if RETURNING clause was specified
	//   - opts.ReturningColumns: column names to include in RETURNING results
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *DMLOptions) (*DMLResult, error)
}

// DeletableTable extends Table with DELETE capability.
// Tables must have a rowid mechanism to identify rows for deletion.
// Implementations MUST be goroutine-safe.
type DeletableTable interface {
	Table

	// Delete removes rows identified by rowIDs.
	// The opts parameter provides options including RETURNING clause information:
	//   - opts.Returning: true if RETURNING clause was specified
	//   - opts.ReturningColumns: column names to include in RETURNING results
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	Delete(ctx context.Context, rowIDs []int64, opts *DMLOptions) (*DMLResult, error)
}

// UpdatableBatchTable extends Table with batch-oriented UPDATE capability.
// The Update method receives the complete input RecordBatch including the rowid column.
// Implementations extract rowid values from the rowid column in the RecordBatch.
// This interface is preferred over UpdatableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type UpdatableBatchTable interface {
	Table

	// Update modifies existing rows using data from the RecordBatch.
	// The rows RecordBatch contains both the rowid column (identifying rows to update)
	// and the new column values. Implementations MUST extract rowid values from
	// the rowid column (identified by name "rowid" or metadata key "is_rowid").
	// Use FindRowIDColumn(rows.Schema()) to locate the rowid column.
	// Implementations MUST return ErrNullRowID if any rowid value is null.
	// Row order in RecordBatch determines update order.
	// The opts parameter provides options including RETURNING clause information:
	//   - opts.Returning: true if RETURNING clause was specified
	//   - opts.ReturningColumns: column names to include in RETURNING results
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	// Caller MUST call rows.Release() after Update returns.
	Update(ctx context.Context, rows arrow.RecordBatch, opts *DMLOptions) (*DMLResult, error)
}

// DeletableBatchTable extends Table with batch-oriented DELETE capability.
// The Delete method receives a RecordBatch containing the rowid column.
// Implementations extract rowid values from the rowid column in the RecordBatch.
// This interface is preferred over DeletableTable when both are implemented.
// Implementations MUST be goroutine-safe.
type DeletableBatchTable interface {
	Table

	// Delete removes rows identified by rowid values in the RecordBatch.
	// The rows RecordBatch contains the rowid column (identified by name "rowid"
	// or metadata key "is_rowid") that identifies rows to delete.
	// Use FindRowIDColumn(rows.Schema()) to locate the rowid column.
	// Implementations MUST return ErrNullRowID if any rowid value is null.
	// The opts parameter provides options including RETURNING clause information:
	//   - opts.Returning: true if RETURNING clause was specified
	//   - opts.ReturningColumns: column names to include in RETURNING results
	// Returns DMLResult with affected row count and optional returning data.
	// Context may contain transaction ID for coordinated operations.
	// Caller MUST call rows.Release() after Delete returns.
	Delete(ctx context.Context, rows arrow.RecordBatch, opts *DMLOptions) (*DMLResult, error)
}

// ColumnStats contains statistics for a single table column.
// All fields are nullable to support partial statistics reporting.
// Implementations may return nil for any field they cannot compute.
type ColumnStats struct {
	// HasNotNull indicates whether the column contains non-null values.
	HasNotNull *bool

	// HasNull indicates whether the column contains null values.
	HasNull *bool

	// DistinctCount is the approximate or exact number of unique values.
	DistinctCount *uint64

	// Min is the minimum value in the column.
	// Must be a Go type compatible with the column's Arrow type
	// (e.g., int64 for Int64, string for String).
	Min any

	// Max is the maximum value in the column.
	// Must be a Go type compatible with the column's Arrow type.
	Max any

	// MaxStringLength is the maximum string length (for string columns only).
	MaxStringLength *uint64

	// ContainsUnicode indicates whether strings contain unicode characters.
	ContainsUnicode *bool
}

// StatisticsTable extends Table with column statistics capability.
// Tables implement this interface to enable DuckDB query optimization
// through the column_statistics action.
// Implementations MUST be goroutine-safe.
type StatisticsTable interface {
	Table

	// ColumnStatistics returns statistics for a specific column.
	// columnName identifies the column to get statistics for.
	// columnType is the DuckDB type name (e.g., "VARCHAR", "INTEGER").
	// Returns ColumnStats with nil fields for unavailable statistics.
	// Returns ErrNotFound if the column doesn't exist.
	ColumnStatistics(ctx context.Context, columnName string, columnType string) (*ColumnStats, error)
}
