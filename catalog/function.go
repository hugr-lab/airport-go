package catalog

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

// TableFunctionInOut represents a table function that accepts row sets as input.
// Example: transform_rows((SELECT * FROM table WHERE condition))
// Uses DoExchange bidirectional streaming to process input rows and return output rows.
// Reference: https://airport.query.farm/table_returning_functions.html
type TableFunctionInOut interface {
	// Name returns the function name (e.g., "TRANSFORM_ROWS").
	// By convention, use UPPERCASE for function names.
	// MUST return non-empty string.
	Name() string

	// Comment returns optional function documentation.
	// Returns empty string if no comment provided.
	Comment() string

	// Signature returns the function signature.
	// First N-1 parameters are scalars (sent in initial metadata).
	// Last parameter type describes the input row schema.
	// ReturnType should be nil for table functions.
	Signature() FunctionSignature

	// SchemaForParameters returns the output schema based on scalar parameters and input schema.
	// params: scalar parameters from function call (e.g., ['arg1', 'arg2'])
	// inputSchema: schema of the row set being passed to the function
	// Returns the schema that output rows will conform to.
	SchemaForParameters(ctx context.Context, params []any, inputSchema *arrow.Schema) (*arrow.Schema, error)

	// Execute processes input rows and returns output rows.
	// params: scalar parameters from function call
	// input: RecordReader providing input rows from subquery
	// Returns RecordReader with output rows matching SchemaForParameters.
	// Caller MUST call reader.Release().
	Execute(ctx context.Context, params []any, input array.RecordReader, opts *ScanOptions) (array.RecordReader, error)
}

// ScalarFunction represents a user-defined scalar function.
// Callable from DuckDB queries via Airport extension using DoExchange bidirectional streaming.
// Implementations MUST be goroutine-safe.
// Reference: https://airport.query.farm/scalar_functions.html
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

	// Execute runs the function on input record batch and returns result array.
	// Input record columns MUST match parameter types from Signature.
	// Returned array MUST match return type from Signature.
	// Returned array length MUST equal input record row count.
	// Caller MUST call result.Release() to free memory.
	// MUST respect context cancellation.
	// Processes entire batch at once (vectorized execution).
	Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error)
}
