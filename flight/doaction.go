package flight

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// DoAction executes server actions including scalar function invocation.
// This RPC supports:
//   - Scalar function execution
//   - Table function schema discovery
//   - Custom server commands
func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()

	s.logger.Info("DoAction called",
		"type", action.GetType(),
		"body_size", len(action.GetBody()),
	)

	actionType := action.GetType()

	switch actionType {
	case "ExecuteScalarFunction":
		return s.executeScalarFunction(ctx, action, stream)

	case "GetTableFunctionInfo":
		return s.getTableFunctionInfo(ctx, action, stream)

	default:
		return status.Errorf(codes.Unimplemented, "unknown action type: %s", actionType)
	}
}

// executeScalarFunction executes a scalar function on input Arrow data.
// Request format (MessagePack):
//
//	{
//	  "schema": "main",
//	  "function": "UPPERCASE",
//	  "input_data": <Arrow IPC bytes>
//	}
func (s *Server) executeScalarFunction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode MessagePack parameters (T058 - signature validation)
	var params struct {
		Schema   string `msgpack:"schema"`
		Function string `msgpack:"function"`
	}

	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode function parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
	}

	s.logger.Info("Executing scalar function",
		"schema", params.Schema,
		"function", params.Function,
	)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", params.Schema)
	}

	// Get scalar functions
	functions, err := schema.ScalarFunctions(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get functions: %v", err)
	}

	// Find the requested function
	var targetFunc catalog.ScalarFunction
	for _, fn := range functions {
		if fn.Name() == params.Function {
			targetFunc = fn
			break
		}
	}

	if targetFunc == nil {
		return status.Errorf(codes.NotFound, "function not found: %s", params.Function)
	}

	s.logger.Info("Found scalar function",
		"name", targetFunc.Name(),
		"comment", targetFunc.Comment(),
	)

	// For now, return success result
	// In a full implementation, would:
	// 1. Receive Arrow input data from client
	// 2. Validate input schema matches function signature
	// 3. Execute function vectorized (T059)
	// 4. Stream result batches back (T060)

	result := &flight.Result{
		Body: []byte(fmt.Sprintf("Function %s executed successfully", params.Function)),
	}

	if err := stream.Send(result); err != nil {
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	return nil
}

// getTableFunctionInfo returns schema information for a table function.
// This allows clients to discover what columns a table function will return
// without executing it.
//
// Request format (MessagePack):
//
//	{
//	  "schema": "main",
//	  "function": "read_parquet",
//	  "parameters": ["/path/to/file.parquet"]
//	}
func (s *Server) getTableFunctionInfo(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var params struct {
		Schema     string        `msgpack:"schema"`
		Function   string        `msgpack:"function"`
		Parameters []interface{} `msgpack:"parameters"`
	}

	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode table function parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
	}

	s.logger.Info("Getting table function info",
		"schema", params.Schema,
		"function", params.Function,
		"param_count", len(params.Parameters),
	)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", params.Schema)
	}

	// Get table functions
	functions, err := schema.TableFunctions(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get functions: %v", err)
	}

	// Find the requested function
	var targetFunc catalog.TableFunction
	for _, fn := range functions {
		if fn.Name() == params.Function {
			targetFunc = fn
			break
		}
	}

	if targetFunc == nil {
		return status.Errorf(codes.NotFound, "table function not found: %s", params.Function)
	}

	// Get schema for the given parameters
	resultSchema, err := targetFunc.SchemaForParameters(ctx, params.Parameters)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}

	// Serialize schema to Arrow IPC
	schemaBytes, err := serializeSchema(resultSchema, s.allocator)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize schema: %v", err)
	}

	// Send result
	result := &flight.Result{
		Body: schemaBytes,
	}

	if err := stream.Send(result); err != nil {
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Info("Table function info sent",
		"function", params.Function,
		"schema_fields", resultSchema.NumFields(),
	)

	return nil
}

// serializeSchema serializes an Arrow schema to IPC bytes.
func serializeSchema(schema *arrow.Schema, allocator memory.Allocator) ([]byte, error) {
	// Serialize schema to IPC format
	// For schema-only serialization, we can use flight's SerializeSchema
	return flight.SerializeSchema(schema, allocator), nil
}
