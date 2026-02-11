package flight

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	aflight "github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

const dataURIPrefix = "data:application/x-msgpack-duckdb-function-call;base64,"

// EncodeFunctionCallURI encodes a FunctionCall into a data:// URI string.
//
// The URI format is:
//
//	data:application/x-msgpack-duckdb-function-call;base64,{BASE64_DATA}
//
// Where BASE64_DATA decodes to a msgpack map:
//
//	{"function_name": string, "data": string}
//
// The "data" field is a base64-encoded Arrow IPC stream containing a single-row
// table with the function arguments.
func EncodeFunctionCallURI(fc catalog.FunctionCall, alloc memory.Allocator) (string, error) {
	if fc.FunctionName == "" {
		return "", fmt.Errorf("FunctionCall: FunctionName must not be empty")
	}

	// Validate all args before encoding
	for i, arg := range fc.Args {
		if err := arg.Validate(); err != nil {
			return "", fmt.Errorf("FunctionCall arg %d: %w", i, err)
		}
	}

	// Build Arrow IPC from args (raw bytes)
	ipcBytes, err := buildArrowIPCFromArgs(fc.Args, alloc)
	if err != nil {
		return "", fmt.Errorf("failed to build Arrow IPC: %w", err)
	}

	// Create msgpack map with raw binary data (msgpack bin type)
	msgpackData := map[string]any{
		"function_name": fc.FunctionName,
		"data":          ipcBytes,
	}

	msgpackBytes, err := msgpack.Encode(msgpackData)
	if err != nil {
		return "", fmt.Errorf("failed to encode msgpack: %w", err)
	}

	// Base64-encode the msgpack
	uriData := base64.StdEncoding.EncodeToString(msgpackBytes)

	return dataURIPrefix + uriData, nil
}

// buildArrowIPCFromArgs creates a single-row Arrow IPC stream from function arguments.
// Positional arguments use field names "arg_0", "arg_1", etc.
// Named arguments use their Name as the field name.
// Returns the raw IPC bytes.
func buildArrowIPCFromArgs(args []catalog.FunctionCallArg, alloc memory.Allocator) ([]byte, error) {
	if len(args) == 0 {
		// Empty args: create schema with no fields and empty record
		schema := arrow.NewSchema([]arrow.Field{}, nil)
		return encodeEmptyRecord(schema, alloc)
	}

	// Build schema fields
	fields := make([]arrow.Field, len(args))
	positionalIdx := 0
	for i, arg := range args {
		name := arg.Name
		if name == "" {
			name = fmt.Sprintf("arg_%d", positionalIdx)
			positionalIdx++
		}
		fields[i] = arrow.Field{
			Name:     name,
			Type:     arg.Type,
			Nullable: true,
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Build single-row record batch
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for i, arg := range args {
		if err := appendArgValue(builder.Field(i), arg); err != nil {
			return nil, fmt.Errorf("failed to append arg %d: %w", i, err)
		}
	}

	record := builder.NewRecordBatch()
	defer record.Release()

	// Serialize to IPC stream
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write IPC record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close IPC writer: %w", err)
	}

	return buf.Bytes(), nil
}

// encodeEmptyRecord creates a raw Arrow IPC stream with an empty record.
func encodeEmptyRecord(schema *arrow.Schema, alloc memory.Allocator) ([]byte, error) {
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	record := builder.NewRecordBatch()
	defer record.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write empty IPC record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close IPC writer: %w", err)
	}

	return buf.Bytes(), nil
}

// appendArgValue appends a FunctionCallArg value to an Arrow array builder.
func appendArgValue(b array.Builder, arg catalog.FunctionCallArg) error {
	switch v := arg.Value.(type) {
	case string:
		b.(*array.StringBuilder).Append(v)
	case bool:
		b.(*array.BooleanBuilder).Append(v)
	case int:
		b.(*array.Int64Builder).Append(int64(v))
	case int8:
		b.(*array.Int8Builder).Append(v)
	case int16:
		b.(*array.Int16Builder).Append(v)
	case int32:
		b.(*array.Int32Builder).Append(v)
	case int64:
		b.(*array.Int64Builder).Append(v)
	case uint8:
		b.(*array.Uint8Builder).Append(v)
	case uint16:
		b.(*array.Uint16Builder).Append(v)
	case uint32:
		b.(*array.Uint32Builder).Append(v)
	case uint64:
		b.(*array.Uint64Builder).Append(v)
	case float32:
		b.(*array.Float32Builder).Append(v)
	case float64:
		b.(*array.Float64Builder).Append(v)
	case time.Time:
		tsBuilder := b.(*array.TimestampBuilder)
		tsType := arg.Type.(*arrow.TimestampType)
		ts, err := arrow.TimestampFromTime(v, tsType.Unit)
		if err != nil {
			return fmt.Errorf("failed to convert time: %w", err)
		}
		tsBuilder.Append(ts)
	case orb.Geometry:
		data, err := wkb.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to encode geometry as WKB: %w", err)
		}
		b.(*array.BinaryBuilder).Append(data)
	case []byte:
		b.(*array.BinaryBuilder).Append(v)
	default:
		return fmt.Errorf("unsupported value type %T for Arrow encoding", arg.Value)
	}
	return nil
}

// sendTableRefEndpointResponse sends data:// endpoint response for a TableRef.
// Each function call is encoded as a separate FlightEndpoint with a data:// URI location.
func (s *Server) sendTableRefEndpointResponse(
	schemaName, tableName string,
	functionCalls []catalog.FunctionCall,
	stream aflight.FlightService_DoActionServer,
) error {
	endpoints := make([]string, 0, len(functionCalls))

	for i, fc := range functionCalls {
		uri, err := EncodeFunctionCallURI(fc, s.allocator)
		if err != nil {
			s.logger.Error("Failed to encode function call URI",
				"schema", schemaName,
				"table", tableName,
				"function_call_index", i,
				"error", err,
			)
			return status.Errorf(codes.Internal, "failed to encode function call %d: %v", i, err)
		}

		// Create FlightEndpoint with data:// URI location and empty ticket
		endpoint := &aflight.FlightEndpoint{
			Ticket: &aflight.Ticket{
				Ticket: []byte("{}"),
			},
			Location: []*aflight.Location{
				{Uri: uri},
			},
		}

		endpointBytes, err := proto.Marshal(endpoint)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal endpoint: %v", err)
		}

		endpoints = append(endpoints, string(endpointBytes))
	}

	responseBody, err := msgpack.Encode(endpoints)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to encode endpoints: %v", err)
	}

	result := &aflight.Result{
		Body: responseBody,
	}

	if err := stream.Send(result); err != nil {
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("sendTableRefEndpointResponse completed",
		"schema", schemaName,
		"table", tableName,
		"endpoint_count", len(functionCalls),
	)
	return nil
}
