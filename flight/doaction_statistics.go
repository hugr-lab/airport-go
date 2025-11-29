package flight

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// ColumnStatisticsParams for column_statistics action.
type ColumnStatisticsParams struct {
	FlightDescriptor []byte `msgpack:"flight_descriptor"`
	ColumnName       string `msgpack:"column_name"`
	Type             string `msgpack:"type"`
}

// handleColumnStatisticsAction implements the column_statistics DoAction handler.
// It returns column statistics for a specific table column as an Arrow RecordBatch.
func (s *Server) handleColumnStatisticsAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params ColumnStatisticsParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode column_statistics parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid column_statistics payload: %v", err)
	}

	s.logger.Debug("handleColumnStatistics called",
		"column_name", params.ColumnName,
		"type", params.Type,
	)

	// Validate required fields
	if params.ColumnName == "" {
		return status.Error(codes.InvalidArgument, "column_name is required")
	}
	if params.Type == "" {
		return status.Error(codes.InvalidArgument, "type is required")
	}
	if len(params.FlightDescriptor) == 0 {
		return status.Error(codes.InvalidArgument, "flight_descriptor is required")
	}

	// Parse FlightDescriptor to extract schema and table name
	var descriptor flight.FlightDescriptor
	if err := proto.Unmarshal(params.FlightDescriptor, &descriptor); err != nil {
		s.logger.Error("Failed to unmarshal FlightDescriptor", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid flight_descriptor: %v", err)
	}

	if descriptor.Type != flight.DescriptorPATH || len(descriptor.Path) < 2 {
		return status.Error(codes.InvalidArgument, "flight_descriptor must be PATH type with schema and table")
	}

	schemaName := descriptor.Path[0]
	tableName := descriptor.Path[1]

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", schemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema %q not found", schemaName)
	}

	// Look up table
	table, err := schema.Table(ctx, tableName)
	if err != nil {
		s.logger.Error("Failed to get table", "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return status.Errorf(codes.NotFound, "table %q not found", tableName)
	}

	// Check if table implements StatisticsTable
	statsTable, ok := table.(catalog.StatisticsTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support statistics")
	}

	// Get column statistics
	stats, err := statsTable.ColumnStatistics(ctx, params.ColumnName, params.Type)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column %q not found", params.ColumnName)
	}
	if err != nil {
		s.logger.Error("Failed to get column statistics", "column", params.ColumnName, "error", err)
		return status.Errorf(codes.Internal, "failed to get column statistics: %v", err)
	}

	// Convert DuckDB type to Arrow type
	arrowType := duckdbTypeToArrow(params.Type)

	// Build statistics schema
	statsSchema := buildStatisticsSchema(arrowType)

	// Build statistics RecordBatch
	record := buildStatisticsRecordBatch(s.allocator, statsSchema, stats)
	defer record.Release()

	// Serialize RecordBatch to IPC format
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(statsSchema), ipc.WithAllocator(s.allocator))
	if err := writer.Write(record); err != nil {
		s.logger.Error("Failed to write IPC record", "error", err)
		return status.Errorf(codes.Internal, "failed to serialize statistics: %v", err)
	}
	if err := writer.Close(); err != nil {
		s.logger.Error("Failed to close IPC writer", "error", err)
		return status.Errorf(codes.Internal, "failed to close IPC writer: %v", err)
	}

	// Send response
	if err := stream.Send(&flight.Result{Body: buf.Bytes()}); err != nil {
		s.logger.Error("Failed to send column_statistics response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleColumnStatistics completed",
		"schema", schemaName,
		"table", tableName,
		"column", params.ColumnName,
	)
	return nil
}

// duckdbTypeToArrow converts a DuckDB type name to the corresponding Arrow type.
func duckdbTypeToArrow(duckdbType string) arrow.DataType {
	// Normalize type name (uppercase, trim whitespace)
	normalized := strings.ToUpper(strings.TrimSpace(duckdbType))

	switch normalized {
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT", "INT1":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT", "INT2":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER", "INT", "INT4":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT", "INT8":
		return arrow.PrimitiveTypes.Int64
	case "UTINYINT":
		return arrow.PrimitiveTypes.Uint8
	case "USMALLINT":
		return arrow.PrimitiveTypes.Uint16
	case "UINTEGER":
		return arrow.PrimitiveTypes.Uint32
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64
	case "FLOAT", "FLOAT4", "REAL":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE", "FLOAT8":
		return arrow.PrimitiveTypes.Float64
	case "VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR":
		return arrow.BinaryTypes.String
	case "BLOB", "BYTEA":
		return arrow.BinaryTypes.Binary
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "TIME":
		return arrow.FixedWidthTypes.Time64us
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case "INTERVAL":
		return arrow.FixedWidthTypes.MonthDayNanoInterval
	case "UUID":
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}

// buildStatisticsSchema creates the Arrow schema for column statistics response.
// The min and max fields use the provided column type.
func buildStatisticsSchema(columnType arrow.DataType) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "has_not_null", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "has_null", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "distinct_count", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "min", Type: columnType, Nullable: true},
		{Name: "max", Type: columnType, Nullable: true},
		{Name: "max_string_length", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "contains_unicode", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

// buildStatisticsRecordBatch creates an Arrow RecordBatch with one row of statistics.
// Note: DuckDB's airport extension expects has_not_null, has_null, and contains_unicode
// to be non-NULL boolean values. If the catalog doesn't provide them, we use sensible defaults.
func buildStatisticsRecordBatch(alloc memory.Allocator, schema *arrow.Schema, stats *catalog.ColumnStats) arrow.RecordBatch {
	// Build each column
	builders := make([]array.Builder, 7)

	// has_not_null (bool) - REQUIRED: DuckDB expects non-NULL
	builders[0] = array.NewBooleanBuilder(alloc)
	boolBuilder0 := builders[0].(*array.BooleanBuilder)
	if stats.HasNotNull != nil {
		boolBuilder0.Append(*stats.HasNotNull)
	} else {
		// Default to true (assume column has non-null values)
		boolBuilder0.Append(true)
	}

	// has_null (bool) - REQUIRED: DuckDB expects non-NULL
	builders[1] = array.NewBooleanBuilder(alloc)
	boolBuilder1 := builders[1].(*array.BooleanBuilder)
	if stats.HasNull != nil {
		boolBuilder1.Append(*stats.HasNull)
	} else {
		// Default to false (assume column has no null values)
		boolBuilder1.Append(false)
	}

	// distinct_count (uint64) - REQUIRED: DuckDB expects non-NULL
	builders[2] = array.NewUint64Builder(alloc)
	uint64Builder2 := builders[2].(*array.Uint64Builder)
	if stats.DistinctCount != nil {
		uint64Builder2.Append(*stats.DistinctCount)
	} else {
		// Default to 0 (unknown distinct count)
		uint64Builder2.Append(0)
	}

	// min (dynamic type) - REQUIRED: DuckDB expects non-NULL
	// We need to provide a default value based on the type
	minType := schema.Field(3).Type
	builders[3] = array.NewBuilder(alloc, minType)
	if stats.Min != nil {
		appendValue(builders[3], stats.Min)
	} else {
		appendDefaultValue(builders[3], minType, true) // true = min default
	}

	// max (dynamic type) - REQUIRED: DuckDB expects non-NULL
	maxType := schema.Field(4).Type
	builders[4] = array.NewBuilder(alloc, maxType)
	if stats.Max != nil {
		appendValue(builders[4], stats.Max)
	} else {
		appendDefaultValue(builders[4], maxType, false) // false = max default
	}

	// max_string_length (uint64) - DuckDB expects non-NULL if column exists
	builders[5] = array.NewUint64Builder(alloc)
	uint64Builder5 := builders[5].(*array.Uint64Builder)
	if stats.MaxStringLength != nil {
		uint64Builder5.Append(*stats.MaxStringLength)
	} else {
		// Default to 0 (unknown max string length)
		uint64Builder5.Append(0)
	}

	// contains_unicode (bool) - REQUIRED: DuckDB expects non-NULL
	builders[6] = array.NewBooleanBuilder(alloc)
	boolBuilder6 := builders[6].(*array.BooleanBuilder)
	if stats.ContainsUnicode != nil {
		boolBuilder6.Append(*stats.ContainsUnicode)
	} else {
		// Default to false (assume no unicode characters)
		boolBuilder6.Append(false)
	}

	// Build arrays
	arrays := make([]arrow.Array, 7)
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	// Create record batch
	return array.NewRecordBatch(schema, arrays, 1)
}

// appendValue appends a value to a builder, handling nil and type conversion.
func appendValue(builder array.Builder, value any) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int8Builder:
		switch v := value.(type) {
		case int8:
			b.Append(v)
		case int:
			b.Append(int8(v))
		case int64:
			b.Append(int8(v))
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch v := value.(type) {
		case int16:
			b.Append(v)
		case int:
			b.Append(int16(v))
		case int64:
			b.Append(int16(v))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch v := value.(type) {
		case int32:
			b.Append(v)
		case int:
			b.Append(int32(v))
		case int64:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		case int32:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Uint8Builder:
		switch v := value.(type) {
		case uint8:
			b.Append(v)
		case uint:
			b.Append(uint8(v))
		case uint64:
			b.Append(uint8(v))
		default:
			b.AppendNull()
		}
	case *array.Uint16Builder:
		switch v := value.(type) {
		case uint16:
			b.Append(v)
		case uint:
			b.Append(uint16(v))
		case uint64:
			b.Append(uint16(v))
		default:
			b.AppendNull()
		}
	case *array.Uint32Builder:
		switch v := value.(type) {
		case uint32:
			b.Append(v)
		case uint:
			b.Append(uint32(v))
		case uint64:
			b.Append(uint32(v))
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch v := value.(type) {
		case uint64:
			b.Append(v)
		case uint:
			b.Append(uint64(v))
		case int:
			b.Append(uint64(v))
		case int64:
			b.Append(uint64(v))
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.BinaryBuilder:
		switch v := value.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			b.AppendNull()
		}
	case *array.Date32Builder:
		switch v := value.(type) {
		case arrow.Date32:
			b.Append(v)
		case int32:
			b.Append(arrow.Date32(v))
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := value.(type) {
		case arrow.Timestamp:
			b.Append(v)
		case int64:
			b.Append(arrow.Timestamp(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// appendDefaultValue appends a sensible default value for min/max statistics.
// For min values, we use the maximum possible value (so any real value is less).
// For max values, we use the minimum possible value (so any real value is greater).
// isMin: true for min default, false for max default
func appendDefaultValue(builder array.Builder, _ arrow.DataType, isMin bool) {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		b.Append(!isMin) // min=false, max=true
	case *array.Int8Builder:
		if isMin {
			b.Append(127) // max int8
		} else {
			b.Append(-128) // min int8
		}
	case *array.Int16Builder:
		if isMin {
			b.Append(32767) // max int16
		} else {
			b.Append(-32768) // min int16
		}
	case *array.Int32Builder:
		if isMin {
			b.Append(2147483647) // max int32
		} else {
			b.Append(-2147483648) // min int32
		}
	case *array.Int64Builder:
		if isMin {
			b.Append(9223372036854775807) // max int64
		} else {
			b.Append(-9223372036854775808) // min int64
		}
	case *array.Uint8Builder:
		if isMin {
			b.Append(255) // max uint8
		} else {
			b.Append(0) // min uint8
		}
	case *array.Uint16Builder:
		if isMin {
			b.Append(65535) // max uint16
		} else {
			b.Append(0) // min uint16
		}
	case *array.Uint32Builder:
		if isMin {
			b.Append(4294967295) // max uint32
		} else {
			b.Append(0) // min uint32
		}
	case *array.Uint64Builder:
		if isMin {
			b.Append(18446744073709551615) // max uint64
		} else {
			b.Append(0) // min uint64
		}
	case *array.Float32Builder:
		if isMin {
			b.Append(3.4028235e+38) // max float32
		} else {
			b.Append(-3.4028235e+38) // min float32
		}
	case *array.Float64Builder:
		if isMin {
			b.Append(1.7976931348623157e+308) // max float64
		} else {
			b.Append(-1.7976931348623157e+308) // min float64
		}
	case *array.StringBuilder:
		// For strings: use simple safe defaults
		// Empty string works for both min (lowest) and max (highest isn't needed for optimization)
		b.Append("")
	case *array.BinaryBuilder:
		if isMin {
			b.Append([]byte{0xff, 0xff, 0xff, 0xff})
		} else {
			b.Append([]byte{})
		}
	case *array.Date32Builder:
		if isMin {
			b.Append(arrow.Date32(2147483647)) // max date
		} else {
			b.Append(arrow.Date32(-2147483648)) // min date
		}
	case *array.TimestampBuilder:
		if isMin {
			b.Append(arrow.Timestamp(9223372036854775807)) // max timestamp
		} else {
			b.Append(arrow.Timestamp(-9223372036854775808)) // min timestamp
		}
	default:
		// Fallback: append null (may cause issues with some types)
		builder.AppendNull()
	}
}
