package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/paulmach/orb"
)

// TableRef represents a read-only table that delegates data reading to DuckDB
// function calls via data:// endpoint URIs. Instead of serving data directly
// through Arrow Flight DoGet (like Table), a TableRef returns one or more
// FunctionCall values that DuckDB executes locally.
//
// Table references appear as normal catalog tables in DuckDB (visible in
// SHOW TABLES, queryable with standard SQL). The only difference is in the
// endpoints action: the server returns data:// URIs instead of grpc:// URIs.
//
// Table references do not support DDL or DML operations.
//
// All methods must be safe for concurrent use by multiple goroutines.
type TableRef interface {
	// Name returns the table reference name (e.g., "remote_logs", "iceberg_orders").
	// Must return a non-empty string.
	Name() string

	// Comment returns optional documentation for the table reference.
	// Returns empty string if no comment is provided.
	Comment() string

	// ArrowSchema returns the Arrow schema describing the columns of the
	// referenced data. This schema is used for catalog discovery (list_schemas)
	// and schema inspection (DESCRIBE table).
	// Must return a non-nil schema.
	ArrowSchema() *arrow.Schema

	// FunctionCalls generates one or more DuckDB function calls for the given
	// request context. DuckDB calls this via the "endpoints" action, providing
	// filter predicates, column projection, function parameters, and time
	// travel specification.
	//
	// Returning multiple function calls enables parallel reads (e.g., one call
	// per partition or file).
	//
	// Must return at least one function call.
	// Must respect context cancellation.
	FunctionCalls(ctx context.Context, req *FunctionCallRequest) ([]FunctionCall, error)
}

// DynamicSchemaTableRef extends TableRef for table references with
// parameter-dependent or time-dependent schemas. This is optional -
// implement it only if the table reference schema varies based on
// function parameters or time travel.
//
// Follows the same pattern as DynamicSchemaTable.
type DynamicSchemaTableRef interface {
	TableRef

	// SchemaForRequest returns the schema for a specific request context.
	// Used by the flight_info action to provide schema without generating
	// function calls.
	// Returns error if parameters are invalid or schema cannot be determined.
	SchemaForRequest(ctx context.Context, req *SchemaRequest) (*arrow.Schema, error)
}

// FunctionCallRequest contains the scan context passed to a TableRef when
// generating function calls. All fields are optional.
type FunctionCallRequest struct {
	// Filters contains JSON filter predicates from the DuckDB query optimizer.
	// Empty string means no filters.
	Filters string

	// Columns contains column names for projection pushdown.
	// Nil or empty means all columns are requested.
	Columns []string

	// Parameters contains function call parameters decoded from Arrow IPC.
	// Nil means no parameters were provided.
	Parameters []any

	// TimePoint specifies a point-in-time for time-travel queries.
	// Nil means current time (no time travel).
	TimePoint *TimePoint
}

// FunctionCall represents a DuckDB function call that will be
// encoded as a data:// URI in the endpoint response.
//
// Example: to call read_csv('/data/file.csv', header=true), create:
//
//	FunctionCall{
//	    FunctionName: "read_csv",
//	    Args: []FunctionCallArg{
//	        {Value: "/data/file.csv", Type: arrow.BinaryTypes.String},
//	        {Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
//	    },
//	}
type FunctionCall struct {
	// FunctionName is the DuckDB function to execute (e.g., "read_csv",
	// "read_parquet", "iceberg_scan"). Must be non-empty.
	FunctionName string

	// Args contains the function arguments, both positional and named.
	// Positional arguments (Name == "") are encoded as arg_0, arg_1, etc.
	// Named arguments use their Name as the Arrow field name.
	Args []FunctionCallArg
}

// FunctionCallArg represents a single argument for a DuckDB function call.
type FunctionCallArg struct {
	// Name is the parameter name. If empty, the argument is positional
	// and will be encoded as arg_N based on its position in the Args slice.
	Name string

	// Value is the argument value. Supported Go types:
	//   - string
	//   - bool
	//   - int, int8, int16, int32, int64
	//   - uint8, uint16, uint32, uint64
	//   - float32, float64
	//   - time.Time (encoded as Arrow Timestamp)
	//   - orb.Geometry (encoded as WKB binary via GeometryExtensionType)
	//   - []byte (binary)
	//   - []any (list)
	//   - map[string]any (struct)
	Value any

	// Type is the Arrow data type for encoding this argument in the
	// Arrow IPC table. Must be compatible with the Value type.
	Type arrow.DataType
}

// Validate checks that the FunctionCallArg is well-formed:
//   - Type must be non-nil
//   - Value must be non-nil
//   - Value type must be a supported Go type
//
// Returns an error describing the validation failure, or nil if valid.
func (a FunctionCallArg) Validate() error {
	if a.Type == nil {
		return fmt.Errorf("FunctionCallArg: Type must not be nil (name=%q)", a.Name)
	}
	if a.Value == nil {
		return fmt.Errorf("FunctionCallArg: Value must not be nil (name=%q)", a.Name)
	}

	switch a.Value.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint8, uint16, uint32, uint64,
		float32, float64,
		time.Time, orb.Geometry,
		[]byte, []any, map[string]any:
		return nil
	default:
		return fmt.Errorf("FunctionCallArg: unsupported Value type %T (name=%q)", a.Value, a.Name)
	}
}

// SchemaWithTableRefs is an optional extension interface for Schema.
// Schemas that contain table references implement this interface.
//
// This follows the same pattern as DynamicSchema, VersionedCatalog, and
// StatisticsTable - optional interfaces discovered via type assertion.
type SchemaWithTableRefs interface {
	// TableRefs returns all table references in this schema.
	// Returns empty slice (not nil) if no table references exist.
	TableRefs(ctx context.Context) ([]TableRef, error)

	// TableRef returns a specific table reference by name.
	// Returns (nil, nil) if the table reference doesn't exist.
	// Returns (nil, err) if lookup fails for other reasons.
	TableRef(ctx context.Context, name string) (TableRef, error)
}
