package catalog

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ScanOptions provides options for table scans.
type ScanOptions struct {
	// Columns to return. If nil/empty, return all columns.
	Columns []string

	// Filter predicate (serialized Arrow expression).
	// If nil, no filtering (return all rows).
	Filter []byte

	// Limit is maximum rows to return.
	// If 0 or negative, no limit.
	Limit int64

	// BatchSize is hint for RecordReader batch size.
	// If 0, implementation chooses default.
	// Implementations MAY ignore this hint.
	BatchSize int

	// TimePoint specifies point-in-time for time-travel queries.
	// Nil for "current" time (no time travel).
	// Supports DuckDB Airport Extension "endpoints" action.
	TimePoint *TimePoint
}

// TimePoint represents a point-in-time for time-travel queries.
// Supports DuckDB Airport Extension temporal query protocol.
type TimePoint struct {
	// Unit specifies time granularity ("timestamp", "version", "snapshot").
	Unit string

	// Value is the time point value (format depends on Unit).
	// Examples:
	//   - Unit="timestamp", Value="2024-01-15T10:30:00Z"
	//   - Unit="version", Value="42"
	//   - Unit="snapshot", Value="abc123def"
	Value string
}

// SchemaRequest contains parameters for determining dynamic schema.
// Used by DynamicSchemaTable.SchemaForRequest().
type SchemaRequest struct {
	// Parameters for table functions (MessagePack-decoded values).
	// Nil for regular tables without parameters.
	Parameters []any

	// TimePoint for time-travel queries.
	// Nil for "current" time.
	TimePoint *TimePoint

	// Columns requested (for projection pushdown).
	// Nil/empty means all columns.
	Columns []string
}

// FunctionSignature describes scalar/table function types.
type FunctionSignature struct {
	// Parameters is list of parameter types (in order).
	// MUST have at least 1 parameter.
	Parameters []arrow.DataType

	// ReturnType is the function's return type (for scalar functions).
	// Nil for table functions (schema determined by SchemaForParameters).
	ReturnType arrow.DataType

	// Variadic indicates if last parameter accepts multiple values.
	Variadic bool
}

// ScanFunc is a function type for table data retrieval.
// User implements this to connect to their data source.
type ScanFunc func(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)

// DMLResult holds the outcome of INSERT, UPDATE, or DELETE operations.
// Returned by InsertableTable.Insert, UpdatableTable.Update, and DeletableTable.Delete.
type DMLResult struct {
	// AffectedRows is the count of rows inserted, updated, or deleted.
	// For INSERT: number of rows successfully inserted.
	// For UPDATE: number of rows matched and modified.
	// For DELETE: number of rows removed.
	AffectedRows int64

	// ReturningData contains rows affected by the operation when
	// a RETURNING clause was specified. nil if no RETURNING requested.
	// Caller is responsible for releasing resources (RecordReader.Release).
	ReturningData array.RecordReader
}
