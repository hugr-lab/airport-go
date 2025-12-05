package catalog

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ScanOptions provides options for table scans.
type ScanOptions struct {
	// Columns to return. If nil/empty, all columns are projected.
	// The server should return a full schema, the projection is handled by duckdb.
	// The server may use this information for optimization.
	// The unselected columns can contain nulls or default values in the returned arrays.
	Columns []string

	// Filter contains a serialized JSON predicate expression from DuckDB.
	// If nil, no filtering (return all rows).
	//
	// The JSON format is documented at https://airport.query.farm/server_predicate_pushdown.html
	// It contains expression trees with comparison operators, column references,
	// constants, and logical conjunctions (AND/OR).
	//
	// Currently, implementations must parse the raw JSON bytes manually.
	// Future versions will provide helper types and functions to interpret
	// the filter structure (expression trees, operators, value extraction).
	//
	// Example JSON structure:
	//   {
	//     "filters": [...],
	//     "column_binding_names_by_index": ["col1", "col2", ...]
	//   }
	//
	// Expression types include:
	//   - BOUND_COMPARISON: Comparison ops (COMPARE_EQUAL, COMPARE_GREATERTHAN, etc.)
	//   - BOUND_COLUMN_REF: Column references with binding info
	//   - BOUND_CONSTANT: Literal values with type information
	//   - BOUND_CONJUNCTION: Logical operators (CONJUNCTION_AND, CONJUNCTION_OR)
	//   - BOUND_FUNCTION: Function calls with arguments
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
	// Nil for "current" time (no time travel).
	TimePoint *TimePoint

	// Columns requested (for projection pushdown).
	// Nil/empty means all columns.
	// The server should return a full schema, the projection is handled by duckdb.
	// The server may use this information for optimization.
	// The unselected columns can contain nulls or default values in the returned arrays.
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
	// Do not use: Not implemented yet in the airport extension.
	Variadic bool
}

// ScanFunc is a function type for table data retrieval.
// User implements this to connect to their data source.
type ScanFunc func(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)

// DMLOptions carries options for DML operations (INSERT, UPDATE, DELETE).
// Designed for extensibility - new fields can be added without breaking interfaces.
type DMLOptions struct {
	// Returning indicates whether a RETURNING clause was specified in the SQL statement.
	// When true, the implementation should populate DMLResult.ReturningData.
	// When false, no RETURNING data is expected (DMLResult.ReturningData should be nil).
	Returning bool

	// ReturningColumns specifies which columns to include in RETURNING results.
	// Only meaningful when Returning is true.
	//
	// IMPORTANT: DuckDB Airport extension does NOT communicate which specific
	// columns are in the RETURNING clause (e.g., "RETURNING id" vs "RETURNING *").
	// The protocol only sends a boolean flag (return-chunks header).
	//
	// The server populates ReturningColumns with ALL table column names
	// (excluding pseudo-columns like rowid) to indicate "return all columns".
	// DuckDB handles column projection CLIENT-SIDE: the server returns all
	// available columns, and DuckDB filters to only the requested columns.
	//
	// Semantics:
	// - If Returning=false: ReturningColumns is ignored
	// - If Returning=true: ReturningColumns contains all table column names
	//
	// Implementations can use ReturningColumns to know what data to return,
	// or ignore it and return all columns (DuckDB filters client-side anyway).
	ReturningColumns []string
}

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

// ProjectSchema returns a projected schema containing only the specified columns.
// If columns is nil or empty, returns the full schema unchanged.
// Column order in the returned schema matches the order in columns slice.
// Original schema metadata is preserved in the projected schema.
// This is a helper function for implementing Table.ArrowSchema(columns).
func ProjectSchema(schema *arrow.Schema, columns []string) *arrow.Schema {
	if len(columns) == 0 {
		return schema
	}

	// Build column name to index map
	colIndex := make(map[string]int, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		colIndex[schema.Field(i).Name] = i
	}

	// Select only requested columns in order
	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		if idx, ok := colIndex[col]; ok {
			fields = append(fields, schema.Field(idx))
		}
	}

	if len(fields) == 0 {
		// No matching columns - return original schema
		return schema
	}

	// Preserve original schema metadata
	meta := schema.Metadata()
	return arrow.NewSchema(fields, &meta)
}
