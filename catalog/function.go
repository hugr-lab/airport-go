package catalog

import (
	"context"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// TableFunction represents a table-valued function (returns table, not scalar).
// Example: read_parquet('file.parquet'), read_json_auto('data.json')
// Supports DuckDB Airport Extension table_function_flight_info action.
type TableFunction interface {
	// Name returns the function name (e.g., "READ_PARQUET").
	// By convention, use UPPERCASE for function names.
	// MUST return non-empty string.
	Name() string

	// Comment returns optional function documentation.
	// Returns empty string if no comment provided.
	Comment() string

	// Signature returns the function signature.
	// Defines parameter types (parameters determine output schema).
	Signature() FunctionSignature

	// SchemaForParameters returns the output schema for given parameters.
	// Used by GetFlightInfo before executing function.
	// Parameters are MessagePack-decoded values matching Signature.
	// Returns error if parameters are invalid.
	SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error)

	// Execute runs the table function and returns a RecordReader.
	// Parameters match Signature types.
	// Returned RecordReader schema MUST match SchemaForParameters result.
	// Caller MUST call reader.Release().
	Execute(ctx context.Context, params []any, opts *ScanOptions) (array.RecordReader, error)
}

// ScalarFunction represents a user-defined scalar function.
// Callable from DuckDB queries via Airport extension.
// Implementations MUST be goroutine-safe.
type ScalarFunction interface {
	// Name returns the function name (e.g., "UPPERCASE").
	// By convention, use UPPERCASE for function names.
	// MUST return non-empty string.
	Name() string

	// Comment returns optional function documentation.
	// Returns empty string if no comment provided.
	Comment() string

	// Signature returns the function signature.
	// Defines parameter types and return type.
	// MUST return valid signature with at least 1 parameter.
	Signature() FunctionSignature

	// Execute runs the function on input record and returns result record.
	// Input record columns MUST match parameter types from Signature.
	// Returned record MUST have single column matching return type.
	// Caller MUST call result.Release() to free memory.
	// MUST respect context cancellation.
	// Processes entire batch at once (vectorized execution).
	Execute(ctx context.Context, input arrow.Record) (arrow.Record, error)
}
