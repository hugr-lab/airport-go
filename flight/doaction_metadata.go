package flight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// handleFlightInfo returns FlightInfo for a descriptor with time travel support.
// This action is used by DuckDB for time travel queries with AT syntax.
func (s *Server) handleFlightInfo(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// The flight_info action structure
	var request struct {
		Descriptor string `msgpack:"descriptor"`
		AtUnit     string `msgpack:"at_unit"`
		AtValue    string `msgpack:"at_value"`
	}

	if len(action.GetBody()) > 0 {
		s.logger.Debug("flight_info request body",
			"size", len(action.GetBody()),
			"hex", fmt.Sprintf("%x", action.GetBody()),
		)
		if err := msgpack.Decode(action.GetBody(), &request); err != nil {
			s.logger.Error("Failed to decode flight_info request", "error", err)
			return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
		}
	}

	// Parse the FlightDescriptor from the serialized descriptor string
	desc := &flight.FlightDescriptor{}
	if err := proto.Unmarshal([]byte(request.Descriptor), desc); err != nil {
		s.logger.Error("Failed to parse FlightDescriptor", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid descriptor: %v", err)
	}

	s.logger.Debug("handleFlightInfo called",
		"descriptor_type", desc.GetType(),
		"path", desc.GetPath(),
		"at_unit", fmt.Sprintf("%q", request.AtUnit),
		"at_value", fmt.Sprintf("%q", request.AtValue),
	)

	// Extract schema and table/function name from path
	path := desc.GetPath()
	if len(path) != 2 {
		return status.Errorf(codes.InvalidArgument, "invalid descriptor path, expected [schema, table], got %v", path)
	}

	schemaName := path[0]
	tableOrFunctionName := path[1]

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", schemaName)
	}

	// Look up table
	table, err := schema.Table(ctx, tableOrFunctionName)
	if err != nil {
		return status.Errorf(codes.NotFound, "table not found: %s.%s", schemaName, tableOrFunctionName)
	}

	// Determine the schema for this table (possibly at a specific time point)
	var tableSchema *arrow.Schema
	var timePoint *catalog.TimePoint

	// Parse time travel parameters if present
	if request.AtUnit != "" && request.AtValue != "" {
		timePoint = &catalog.TimePoint{
			Unit:  request.AtUnit,
			Value: request.AtValue,
		}

		s.logger.Debug("Time travel request",
			"unit", timePoint.Unit,
			"value", timePoint.Value,
		)
	}

	// Check if table supports dynamic schema (time travel)
	if timePoint != nil {
		dynamicTable, ok := table.(catalog.DynamicSchemaTable)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "table %s.%s does not support time travel queries", schemaName, tableOrFunctionName)
		}
		// Use SchemaForRequest to get schema at specific time point
		tableSchema, err = dynamicTable.SchemaForRequest(ctx, &catalog.SchemaRequest{
			TimePoint: timePoint,
		})
		if err != nil {
			s.logger.Error("Failed to get schema for time point",
				"error", err,
				"unit", timePoint.Unit,
				"value", timePoint.Value,
			)
			return status.Errorf(codes.Internal, "failed to get schema for time point: %v", err)
		}
		s.logger.Debug("Using dynamic schema from SchemaForRequest",
			"schema_fields", len(tableSchema.Fields()),
		)
	}
	if timePoint == nil {
		// Regular table - use current schema (no projection)
		tableSchema = table.ArrowSchema(nil)
	}
	if tableSchema == nil {
		return status.Errorf(codes.Internal, "table %s.%s has nil Arrow schema", schemaName, tableOrFunctionName)
	}

	// Create ticket (time travel handled via ScanOptions in DoGet, not ticket fields)
	ticketData := TicketData{
		Schema: schemaName,
		Table:  tableOrFunctionName,
	}
	ticket, err := json.Marshal(ticketData)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
	}

	// Prepare app_metadata - same structure as regular tables
	// Time travel info is encoded in the ticket, not in app_metadata
	appMetadata, _ := msgpack.Encode(map[string]interface{}{
		"type":         "table",
		"schema":       schema.Name(),
		"catalog":      "", // Empty catalog name
		"name":         table.Name(),
		"comment":      table.Comment(),
		"input_schema": nil,
		"action_name":  nil,
		"description":  nil,
		"extra_data":   nil,
	})
	// Create FlightInfo
	flightInfo := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(tableSchema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: ticket,
				},
				Location: []*flight.Location{
					{
						Uri: "grpc://" + s.address,
					},
				},
			},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
		Ordered:      false,
		AppMetadata:  appMetadata,
	}

	// Serialize FlightInfo
	infoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	// Send response
	result := &flight.Result{
		Body: infoBytes,
	}

	s.logger.Debug("handleFlightInfo completed",
		"schema", schemaName,
		"table", tableOrFunctionName,
		"has_time_travel", timePoint != nil,
	)

	return stream.Send(result)
}

// endpointsRequest holds the decoded parameters from handleEndpoints.
type endpointsRequest struct {
	Descriptor string
	Parameters struct {
		JsonFilters              string
		ColumnIDs                []uint64
		TableFunctionParameters  string
		TableFunctionInputSchema string
		AtUnit                   string
		AtValue                  string
	}
}

// handleEndpoints returns flight endpoints for a descriptor.
// This is a required Airport action that allows the server to receive additional context.
func (s *Server) handleEndpoints(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	request, err := s.decodeEndpointsRequest(action)
	if err != nil {
		return err
	}

	desc, err := s.parseDescriptor(request.Descriptor)
	if err != nil {
		return err
	}

	s.logger.Debug("handleEndpoints called",
		"descriptor_type", desc.GetType(),
		"path", desc.GetPath(),
		"has_filters", request.Parameters.JsonFilters != "",
		"column_count", len(request.Parameters.ColumnIDs),
		"at_unit", request.Parameters.AtUnit,
		"at_value", request.Parameters.AtValue,
	)

	if desc.GetType() != flight.DescriptorPATH || len(desc.GetPath()) != 2 {
		s.logger.Error("Invalid descriptor format", "type", desc.GetType(), "path_length", len(desc.GetPath()))
		return status.Errorf(codes.InvalidArgument, "descriptor must be PATH type with 2 elements [schema, table]")
	}

	schemaName := desc.GetPath()[0]
	tableOrFunctionName := desc.GetPath()[1]

	var ticket []byte
	if request.Parameters.TableFunctionParameters != "" {
		ticket, err = s.createTableFunctionTicket(ctx, schemaName, tableOrFunctionName, request)
	} else {
		ticket, err = s.createTableTicket(ctx, schemaName, tableOrFunctionName, request)
	}
	if err != nil {
		return err
	}

	return s.sendEndpointResponse(schemaName, tableOrFunctionName, ticket, stream)
}

// decodeEndpointsRequest decodes the msgpack request body.
func (s *Server) decodeEndpointsRequest(action *flight.Action) (*endpointsRequest, error) {
	var raw struct {
		Descriptor string `msgpack:"descriptor"`
		Parameters struct {
			JsonFilters              string   `msgpack:"json_filters"`
			ColumnIDs                []uint64 `msgpack:"column_ids"`
			TableFunctionParameters  string   `msgpack:"table_function_parameters"`
			TableFunctionInputSchema string   `msgpack:"table_function_input_schema"`
			AtUnit                   string   `msgpack:"at_unit"`
			AtValue                  string   `msgpack:"at_value"`
		} `msgpack:"parameters"`
	}

	if len(action.GetBody()) > 0 {
		if err := msgpack.Decode(action.GetBody(), &raw); err != nil {
			s.logger.Error("Failed to decode endpoints request", "error", err)
			return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
		}
	}

	req := &endpointsRequest{
		Descriptor: raw.Descriptor,
	}
	req.Parameters.JsonFilters = raw.Parameters.JsonFilters
	req.Parameters.ColumnIDs = raw.Parameters.ColumnIDs
	req.Parameters.TableFunctionParameters = raw.Parameters.TableFunctionParameters
	req.Parameters.TableFunctionInputSchema = raw.Parameters.TableFunctionInputSchema
	req.Parameters.AtUnit = raw.Parameters.AtUnit
	req.Parameters.AtValue = raw.Parameters.AtValue
	return req, nil
}

// parseDescriptor parses a protobuf-encoded FlightDescriptor.
func (s *Server) parseDescriptor(data string) (*flight.FlightDescriptor, error) {
	desc := &flight.FlightDescriptor{}
	if err := proto.Unmarshal([]byte(data), desc); err != nil {
		s.logger.Error("Failed to parse FlightDescriptor", "error", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid descriptor: %v", err)
	}
	return desc, nil
}

// createTableFunctionTicket creates a ticket for table function execution.
func (s *Server) createTableFunctionTicket(ctx context.Context, schemaName, functionName string, request *endpointsRequest) ([]byte, error) {
	paramBytes := []byte(request.Parameters.TableFunctionParameters)
	s.logger.Debug("Parsing table function parameters",
		"param_size", len(paramBytes),
		"first_bytes", fmt.Sprintf("%x", paramBytes[:min(20, len(paramBytes))]),
	)

	params, err := s.extractFunctionParams(paramBytes)
	if err != nil {
		return nil, err
	}

	ticketData := TicketData{
		Schema:         schemaName,
		TableFunction:  functionName,
		FunctionParams: params,
	}

	ticketData.Columns = s.resolveTableFunctionColumns(ctx, schemaName, functionName, params, request.Parameters.ColumnIDs)

	ticket, err := json.Marshal(ticketData)
	if err != nil {
		s.logger.Error("Failed to encode table function ticket", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
	}

	s.logger.Debug("Created table function ticket",
		"schema", schemaName,
		"function", functionName,
		"param_count", len(params),
	)
	return ticket, nil
}

// extractFunctionParams extracts parameter values from an Arrow IPC stream.
func (s *Server) extractFunctionParams(paramBytes []byte) ([]any, error) {
	paramReader, err := ipc.NewReader(bytes.NewReader(paramBytes))
	if err != nil {
		s.logger.Error("Failed to deserialize parameter record batch", "error", err)
		return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize parameters: %v", err)
	}
	defer paramReader.Release()

	if !paramReader.Next() {
		return nil, status.Errorf(codes.InvalidArgument, "no parameter record found")
	}
	paramRecord := paramReader.RecordBatch()

	params := make([]any, paramRecord.NumCols())
	for i := range int(paramRecord.NumCols()) {
		col := paramRecord.Column(i)
		if col.Len() == 0 {
			params[i] = nil
		} else {
			params[i] = extractScalarValue(col, 0)
		}
	}
	return params, nil
}

// resolveTableFunctionColumns maps column IDs to column names for a table function.
func (s *Server) resolveTableFunctionColumns(ctx context.Context, schemaName, functionName string, params []any, columnIDs []uint64) []string {
	if len(columnIDs) == 0 {
		return nil
	}

	schemaObj, err := s.catalog.Schema(ctx, schemaName)
	if err != nil || schemaObj == nil {
		return nil
	}

	funcs, err := schemaObj.TableFunctions(ctx)
	if err != nil {
		return nil
	}

	for _, fn := range funcs {
		if fn.Name() != functionName {
			continue
		}
		funcSchema, err := fn.SchemaForParameters(ctx, params)
		if err != nil || funcSchema == nil {
			return nil
		}
		columns := mapColumnIDsToNames(funcSchema, columnIDs)
		if len(columns) > 0 {
			s.logger.Debug("Table function column projection",
				"schema", schemaName,
				"function", functionName,
				"column_ids", columnIDs,
				"column_names", columns,
			)
		}
		return columns
	}
	return nil
}

// createTableTicket creates a ticket for regular table scan.
func (s *Server) createTableTicket(ctx context.Context, schemaName, tableName string, request *endpointsRequest) ([]byte, error) {
	ticketData := TicketData{
		Schema: schemaName,
		Table:  tableName,
	}

	ticketData.Columns = s.resolveTableColumns(ctx, schemaName, tableName, request.Parameters.ColumnIDs)

	if request.Parameters.AtUnit != "" && request.Parameters.AtValue != "" {
		ticketData.TimePointUnit = normalizeTimeUnit(request.Parameters.AtUnit)
		ticketData.TimePointValue = request.Parameters.AtValue
		s.logger.Debug("Added time travel to ticket",
			"schema", schemaName,
			"table", tableName,
			"unit", ticketData.TimePointUnit,
			"value", ticketData.TimePointValue,
		)
	}

	ticket, err := json.Marshal(ticketData)
	if err != nil {
		s.logger.Error("Failed to encode ticket", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
	}
	return ticket, nil
}

// resolveTableColumns maps column IDs to column names for a table.
func (s *Server) resolveTableColumns(ctx context.Context, schemaName, tableName string, columnIDs []uint64) []string {
	if len(columnIDs) == 0 {
		return nil
	}

	schemaObj, err := s.catalog.Schema(ctx, schemaName)
	if err != nil || schemaObj == nil {
		return nil
	}

	table, err := schemaObj.Table(ctx, tableName)
	if err != nil || table == nil {
		return nil
	}

	tableSchema := table.ArrowSchema(nil) // nil = full schema
	if tableSchema == nil {
		return nil
	}

	columns := mapColumnIDsToNames(tableSchema, columnIDs)
	if len(columns) > 0 {
		s.logger.Debug("Column projection",
			"schema", schemaName,
			"table", tableName,
			"column_ids", columnIDs,
			"column_names", columns,
		)
	}
	return columns
}

// mapColumnIDsToNames converts column IDs to column names using the schema.
// Skips invalid IDs (e.g., max uint64 used for DML operations).
func mapColumnIDsToNames(schema *arrow.Schema, columnIDs []uint64) []string {
	numFields := uint64(schema.NumFields())
	columns := make([]string, 0, len(columnIDs))
	for _, colID := range columnIDs {
		if colID < numFields {
			columns = append(columns, schema.Field(int(colID)).Name)
		}
	}
	return columns
}

// normalizeTimeUnit converts DuckDB time units to lowercase.
func normalizeTimeUnit(unit string) string {
	switch unit {
	case "TIMESTAMP":
		return "timestamp"
	case "TIMESTAMP_NS":
		return "timestamp_ns"
	case "VERSION":
		return "version"
	default:
		return unit
	}
}

// sendEndpointResponse sends the endpoint response to the client.
func (s *Server) sendEndpointResponse(schemaName, tableName string, ticket []byte, stream flight.FlightService_DoActionServer) error {
	// Create FlightEndpoint with location
	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{
			Ticket: ticket,
		},
	}

	// Add location if server address is configured
	endpoint.Location = []*flight.Location{{Uri: s.address}}

	// Serialize FlightEndpoint as protobuf
	endpointBytes, err := proto.Marshal(endpoint)
	if err != nil {
		s.logger.Error("Failed to marshal endpoint", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal endpoint: %v", err)
	}

	// Return as vector of strings (each string is a serialized FlightEndpoint)
	endpoints := []string{string(endpointBytes)}

	responseBody, err := msgpack.Encode(endpoints)
	if err != nil {
		s.logger.Error("Failed to encode endpoints", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	result := &flight.Result{
		Body: responseBody,
	}

	if err := stream.Send(result); err != nil {
		s.logger.Error("Failed to send endpoints", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleEndpoints completed",
		"schema", schemaName,
		"table", tableName,
		"endpoint_count", 1,
	)
	return nil
}

// handleCreateTransaction returns a transaction identifier.
// This is an optional Airport action for transaction management.
// If no TransactionManager is configured, returns nil identifier.
func (s *Server) handleCreateTransaction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode parameters
	var params struct {
		CatalogName string `msgpack:"catalog_name"`
	}

	if len(action.GetBody()) > 0 {
		if err := msgpack.Decode(action.GetBody(), &params); err != nil {
			s.logger.Error("Failed to decode create_transaction parameters", "error", err)
			return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
		}
	}

	s.logger.Debug("handleCreateTransaction called", "catalog_name", params.CatalogName)

	// If no TransactionManager is configured, return nil identifier
	if s.txManager == nil {
		response := map[string]interface{}{
			"identifier": nil,
		}

		responseBody, err := msgpack.Encode(response)
		if err != nil {
			s.logger.Error("Failed to encode transaction response", "error", err)
			return status.Errorf(codes.Internal, "failed to encode response: %v", err)
		}

		if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
			s.logger.Error("Failed to send transaction result", "error", err)
			return status.Errorf(codes.Internal, "failed to send result: %v", err)
		}

		s.logger.Debug("handleCreateTransaction completed", "has_transaction", false)
		return nil
	}

	// Create transaction using the TransactionManager
	txID, err := s.txManager.BeginTransaction(ctx)
	if err != nil {
		s.logger.Error("Failed to begin transaction", "error", err)
		return status.Errorf(codes.Internal, "failed to create transaction: %v", err)
	}

	response := map[string]interface{}{
		"identifier": txID,
	}

	responseBody, err := msgpack.Encode(response)
	if err != nil {
		s.logger.Error("Failed to encode transaction response", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
		s.logger.Error("Failed to send transaction result", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleCreateTransaction completed", "tx_id", txID)
	return nil
}

// handleGetTransactionStatus returns the current state of a transaction.
// This is an optional Airport action for transaction management.
func (s *Server) handleGetTransactionStatus(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode parameters
	var params struct {
		TransactionID string `msgpack:"transaction_id"`
	}

	if len(action.GetBody()) > 0 {
		if err := msgpack.Decode(action.GetBody(), &params); err != nil {
			s.logger.Error("Failed to decode get_transaction_status parameters", "error", err)
			return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
		}
	}

	s.logger.Debug("handleGetTransactionStatus called", "tx_id", params.TransactionID)

	// If no TransactionManager is configured, return not found
	if s.txManager == nil {
		response := map[string]interface{}{
			"status": "",
			"exists": false,
		}

		responseBody, err := msgpack.Encode(response)
		if err != nil {
			s.logger.Error("Failed to encode transaction status response", "error", err)
			return status.Errorf(codes.Internal, "failed to encode response: %v", err)
		}

		if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
			s.logger.Error("Failed to send transaction status result", "error", err)
			return status.Errorf(codes.Internal, "failed to send result: %v", err)
		}

		return nil
	}

	// Get transaction status
	state, exists := s.txManager.GetTransactionStatus(ctx, params.TransactionID)

	response := map[string]interface{}{
		"status": string(state),
		"exists": exists,
	}

	responseBody, err := msgpack.Encode(response)
	if err != nil {
		s.logger.Error("Failed to encode transaction status response", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
		s.logger.Error("Failed to send transaction status result", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleGetTransactionStatus completed", "tx_id", params.TransactionID, "exists", exists, "status", state)
	return nil
}

// extractScalarValue extracts a scalar value from an Arrow array at the given index.
// This is used to convert Arrow array values to Go interface{} values for function parameters.
func extractScalarValue(arr arrow.Array, idx int) interface{} {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Int8:
		return int64(a.Value(idx))
	case *array.Int16:
		return int64(a.Value(idx))
	case *array.Int32:
		return int64(a.Value(idx))
	case *array.Int64:
		return a.Value(idx)
	case *array.Uint8:
		return int64(a.Value(idx))
	case *array.Uint16:
		return int64(a.Value(idx))
	case *array.Uint32:
		return int64(a.Value(idx))
	case *array.Uint64:
		return int64(a.Value(idx))
	case *array.Float32:
		return float64(a.Value(idx))
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	default:
		// For unsupported types, return nil
		return nil
	}
}
