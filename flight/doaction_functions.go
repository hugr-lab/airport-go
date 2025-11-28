package flight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// handleTableFunctionFlightInfo handles the table_function_flight_info action.
// This is called by DuckDB to get FlightInfo for a table function call with specific parameters.
// Returns FlightInfo containing the dynamic schema and ticket for execution.
//
// Request format (MessagePack):
// buildTableFunctionFlightInfo creates a FlightInfo response for a table function.
// This is the common response structure for both regular and in/out table functions.
func (s *Server) buildTableFunctionFlightInfo(schemaName, functionName string, params []interface{}, funcSchema *arrow.Schema) (*flight.Result, error) {
	// Create ticket with function call information
	ticketData := TicketData{
		Schema:         schemaName,
		TableFunction:  functionName,
		FunctionParams: params,
	}

	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		return nil, fmt.Errorf("failed to encode ticket: %w", err)
	}

	// Create FlightDescriptor
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{schemaName, functionName},
	}

	// Create FlightInfo with schema and endpoint
	flightInfo := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(funcSchema, s.allocator),
		FlightDescriptor: descriptor,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: ticketBytes,
				},
				Location: []*flight.Location{
					{Uri: fmt.Sprintf("grpc://%s", s.address)},
				},
			},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
		Ordered:      false,
	}

	// Serialize FlightInfo to send back
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal FlightInfo: %w", err)
	}

	return &flight.Result{Body: flightInfoBytes}, nil
}

// decodeTableFunctionParameters extracts parameter values from Arrow IPC encoded bytes.
// Returns a slice of interface{} values, one per parameter.
func decodeTableFunctionParameters(paramBytes []byte) ([]interface{}, error) {
	if len(paramBytes) == 0 {
		return []interface{}{}, nil
	}

	paramReader, err := ipc.NewReader(bytes.NewReader(paramBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize parameter record batch: %w", err)
	}
	defer paramReader.Release()

	// Read the parameter record
	if !paramReader.Next() {
		return nil, fmt.Errorf("no parameter record found")
	}
	paramRecord := paramReader.RecordBatch()

	// Extract parameter values from the record batch
	// Each column in the record batch represents one parameter value
	params := make([]interface{}, paramRecord.NumCols())
	for i := 0; i < int(paramRecord.NumCols()); i++ {
		col := paramRecord.Column(i)
		// Get the first value from each column (parameters are single values, not arrays)
		if col.Len() == 0 {
			params[i] = nil
		} else {
			params[i] = extractScalarValue(col, 0)
		}
	}

	return params, nil
}

// handleRegularTableFunction handles FlightInfo request for regular table functions.
// These functions take scalar parameters and return a table.
func (s *Server) handleRegularTableFunction(ctx context.Context, schemaName, functionName string, params []interface{}, fn catalog.TableFunction, stream flight.FlightService_DoActionServer) error {
	funcSchema, err := fn.SchemaForParameters(ctx, params)
	if err != nil {
		s.logger.Error("Failed to get function schema",
			"schema", schemaName,
			"function", functionName,
			"error", err,
		)
		return status.Errorf(codes.Internal, "failed to get function schema: %v", err)
	}

	result, err := s.buildTableFunctionFlightInfo(schemaName, functionName, params, funcSchema)
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	if err := stream.Send(result); err != nil {
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("Table function FlightInfo sent",
		"schema", schemaName,
		"function", functionName,
		"output_fields", funcSchema.NumFields(),
	)

	return nil
}

// handleInOutTableFunction handles FlightInfo request for in/out table functions.
// These functions accept row sets as input and return transformed rows.
func (s *Server) handleInOutTableFunction(ctx context.Context, schemaName, functionName string, params []interface{}, tableInputSchemaBytes []byte, fn catalog.TableFunctionInOut, stream flight.FlightService_DoActionServer) error {
	// Parse the table input schema
	var inputSchema *arrow.Schema
	if len(tableInputSchemaBytes) > 0 {
		schemaReader, err := ipc.NewReader(bytes.NewReader(tableInputSchemaBytes))
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to deserialize table input schema: %v", err)
		}
		defer schemaReader.Release()
		inputSchema = schemaReader.Schema()
	}

	funcSchema, err := fn.SchemaForParameters(ctx, params, inputSchema)
	if err != nil {
		s.logger.Error("Failed to get function schema",
			"schema", schemaName,
			"function", functionName,
			"error", err,
		)
		return status.Errorf(codes.Internal, "failed to get function schema: %v", err)
	}

	result, err := s.buildTableFunctionFlightInfo(schemaName, functionName, params, funcSchema)
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	if err := stream.Send(result); err != nil {
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("Table function FlightInfo sent",
		"schema", schemaName,
		"function", functionName,
		"output_fields", funcSchema.NumFields(),
	)

	return nil
}

// - descriptor: serialized FlightDescriptor (protobuf) containing path like ["schema", "function"]
// - parameters: serialized Arrow Record Batch with parameter values (bytes)
// - table_input_schema: input schema definition (bytes, optional)
// - at_unit: time travel unit (string, optional)
// - at_value: time travel value (string, optional)
func (s *Server) handleTableFunctionFlightInfo(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode the MessagePack request
	var request struct {
		Descriptor       []byte `msgpack:"descriptor"`         // Serialized FlightDescriptor
		Parameters       []byte `msgpack:"parameters"`         // Serialized Arrow Record Batch
		TableInputSchema []byte `msgpack:"table_input_schema"` // Optional
		AtUnit           string `msgpack:"at_unit"`            // Optional
		AtValue          string `msgpack:"at_value"`           // Optional
	}

	if err := msgpack.Decode(action.GetBody(), &request); err != nil {
		s.logger.Error("Failed to decode table function request", "error", err, "body_size", len(action.GetBody()))
		return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	// Deserialize the FlightDescriptor to get schema and function name
	var reqDescriptor flight.FlightDescriptor
	if err := proto.Unmarshal(request.Descriptor, &reqDescriptor); err != nil {
		s.logger.Error("Failed to unmarshal FlightDescriptor", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid descriptor: %v", err)
	}

	// Extract schema and function name from descriptor path
	if len(reqDescriptor.Path) != 2 {
		return status.Errorf(codes.InvalidArgument, "expected descriptor path [schema, function], got %v", reqDescriptor.Path)
	}
	schemaName := reqDescriptor.Path[0]
	functionName := reqDescriptor.Path[1]

	s.logger.Debug("handleTableFunctionFlightInfo called",
		"schema", schemaName,
		"function", functionName,
		"parameters_size", len(request.Parameters),
		"table_input_schema_size", len(request.TableInputSchema),
	)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", schemaName)
	}

	// Get table functions
	functions, err := schema.TableFunctions(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get table functions: %v", err)
	}

	// Get table functions (in/out)
	functionsInOut, err := schema.TableFunctionsInOut(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get table functions (in/out): %v", err)
	}

	// Find the requested function (check both regular and in/out)
	var targetFunc catalog.TableFunction
	var targetFuncInOut catalog.TableFunctionInOut
	var isInOut bool

	for _, fn := range functions {
		if fn.Name() == functionName {
			targetFunc = fn
			break
		}
	}

	if targetFunc == nil {
		// Check in/out functions
		for _, fn := range functionsInOut {
			if fn.Name() == functionName {
				targetFuncInOut = fn
				isInOut = true
				break
			}
		}
	}

	if targetFunc == nil && targetFuncInOut == nil {
		return status.Errorf(codes.NotFound, "table function not found: %s.%s", schemaName, functionName)
	}

	// Decode parameters from Arrow IPC format
	params, err := decodeTableFunctionParameters(request.Parameters)
	if err != nil {
		s.logger.Error("Failed to decode parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "failed to decode parameters: %v", err)
	}

	s.logger.Debug("Getting schema for table function",
		"schema", schemaName,
		"function", functionName,
		"is_in_out", isInOut,
		"param_count", len(params),
		"params", params,
	)

	// Route to appropriate handler based on function type
	if isInOut {
		return s.handleInOutTableFunction(ctx, schemaName, functionName, params, request.TableInputSchema, targetFuncInOut, stream)
	}
	return s.handleRegularTableFunction(ctx, schemaName, functionName, params, targetFunc, stream)
}
