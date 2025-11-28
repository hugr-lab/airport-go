package flight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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

	// Get table schema
	tableSchema := table.ArrowSchema()
	if tableSchema == nil {
		return status.Errorf(codes.Internal, "table %s.%s has nil Arrow schema", schemaName, tableOrFunctionName)
	}

	// Parse time travel parameters and create ticket
	var ts *int64
	var tsNs *int64
	if request.AtUnit != "" && request.AtValue != "" {
		switch request.AtUnit {
		case "TIMESTAMP":
			// Parse timestamp string to Unix seconds
			// Value format: "2024-01-01 00:00:00"
			t, err := time.Parse("2006-01-02 15:04:05", request.AtValue)
			if err == nil {
				tsVal := t.Unix()
				ts = &tsVal
			} else {
				s.logger.Error("Failed to parse timestamp value", "error", err, "value", request.AtValue)
			}
		case "VERSION":
			// Parse version number
			var tsVal int64
			_, err := fmt.Sscanf(request.AtValue, "%d", &tsVal)
			if err == nil {
				ts = &tsVal
			} else {
				s.logger.Error("Failed to parse version value", "error", err, "value", request.AtValue)
			}
		default:
			s.logger.Error("Unsupported time travel unit", "unit", request.AtUnit)
		}
	}

	// Create ticket with time travel parameters
	ticketData := TicketData{
		Schema: schemaName,
		Table:  tableOrFunctionName,
		Ts:     ts,
		TsNs:   tsNs,
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
		"has_time_travel", ts != nil || tsNs != nil,
	)

	return stream.Send(result)
}

// handleEndpoints returns flight endpoints for a descriptor.
// This is a required Airport action that allows the server to receive additional context.
//
//nolint:unparam
func (s *Server) handleEndpoints(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode AirportGetFlightEndpointsRequest
	var request struct {
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
		if err := msgpack.Decode(action.GetBody(), &request); err != nil {
			s.logger.Error("Failed to decode endpoints request", "error", err)
			return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
		}
	}

	// Parse the FlightDescriptor from the serialized descriptor string
	desc := &flight.FlightDescriptor{}
	if err := proto.Unmarshal([]byte(request.Descriptor), desc); err != nil {
		s.logger.Error("Failed to parse FlightDescriptor", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid descriptor: %v", err)
	}

	s.logger.Debug("handleEndpoints called",
		"descriptor_type", desc.GetType(),
		"path", desc.GetPath(),
		"has_filters", request.Parameters.JsonFilters != "",
		"column_count", len(request.Parameters.ColumnIDs),
		"at_unit", request.Parameters.AtUnit,
		"at_value", request.Parameters.AtValue,
	)

	// Validate descriptor
	if desc.GetType() != flight.DescriptorPATH || len(desc.GetPath()) != 2 {
		s.logger.Error("Invalid descriptor format", "type", desc.GetType(), "path_length", len(desc.GetPath()))
		return status.Errorf(codes.InvalidArgument, "descriptor must be PATH type with 2 elements [schema, table]")
	}

	schemaName := desc.GetPath()[0]
	tableOrFunctionName := desc.GetPath()[1]

	// Generate ticket - differentiate between table and table function
	var ticket []byte
	var err error

	if request.Parameters.TableFunctionParameters != "" {
		// This is a table function call - parameters are in serialized Arrow Record Batch format
		paramBytes := []byte(request.Parameters.TableFunctionParameters)

		s.logger.Debug("Parsing table function parameters",
			"param_size", len(paramBytes),
			"first_bytes", fmt.Sprintf("%x", paramBytes[:min(20, len(paramBytes))]),
		)

		// Deserialize the Arrow Record Batch containing parameter values
		paramReader, err := ipc.NewReader(bytes.NewReader(paramBytes))
		if err != nil {
			s.logger.Error("Failed to deserialize parameter record batch", "error", err)
			return status.Errorf(codes.InvalidArgument, "failed to deserialize parameters: %v", err)
		}
		defer paramReader.Release()

		// Read the parameter record
		if !paramReader.Next() {
			return status.Errorf(codes.InvalidArgument, "no parameter record found")
		}
		paramRecord := paramReader.RecordBatch()

		// Extract parameter values from the record batch
		params := make([]interface{}, paramRecord.NumCols())
		for i := 0; i < int(paramRecord.NumCols()); i++ {
			col := paramRecord.Column(i)
			if col.Len() == 0 {
				params[i] = nil
			} else {
				params[i] = extractScalarValue(col, 0)
			}
		}

		// Create table function ticket with parameters
		ticketData := TicketData{
			Schema:         schemaName,
			TableFunction:  tableOrFunctionName,
			FunctionParams: params,
		}
		ticket, err = json.Marshal(ticketData)
		if err != nil {
			s.logger.Error("Failed to encode table function ticket", "error", err)
			return status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
		}

		s.logger.Debug("Created table function ticket",
			"schema", schemaName,
			"function", tableOrFunctionName,
			"param_count", len(params),
		)
	} else {
		// Regular table scan - create ticket with optional time travel parameters
		ticketData := TicketData{
			Schema: schemaName,
			Table:  tableOrFunctionName,
		}

		// Parse time travel parameters if present
		if request.Parameters.AtUnit != "" && request.Parameters.AtValue != "" {
			// Store timestamp based on unit (DuckDB sends "TIMESTAMP" and "VERSION" in uppercase)
			switch request.Parameters.AtUnit {
			case "TIMESTAMP", "timestamp":
				// Parse timestamp string to Unix seconds
				// Value format could be: "2024-01-01 00:00:00" or Unix epoch string
				// Try parsing as timestamp string first
				t, err := time.Parse("2006-01-02 15:04:05", request.Parameters.AtValue)
				if err == nil {
					tsVal := t.Unix()
					ticketData.Ts = &tsVal
					s.logger.Debug("Added time travel to ticket",
						"schema", schemaName,
						"table", tableOrFunctionName,
						"unit", "timestamp",
						"value", tsVal,
						"parsed_from", request.Parameters.AtValue,
					)
				} else {
					// Try parsing as Unix epoch integer
					var tsVal int64
					if _, err := fmt.Sscanf(request.Parameters.AtValue, "%d", &tsVal); err != nil {
						s.logger.Error("Failed to parse timestamp", "at_value", request.Parameters.AtValue, "error", err)
						return status.Errorf(codes.InvalidArgument, "invalid timestamp value: %v", err)
					}
					ticketData.Ts = &tsVal
					s.logger.Debug("Added time travel to ticket",
						"schema", schemaName,
						"table", tableOrFunctionName,
						"unit", "timestamp",
						"value", tsVal,
					)
				}
			case "TIMESTAMP_NS", "timestamp_ns":
				// Parse nanosecond timestamp
				var tsVal int64
				if _, err := fmt.Sscanf(request.Parameters.AtValue, "%d", &tsVal); err != nil {
					s.logger.Error("Failed to parse timestamp_ns", "at_value", request.Parameters.AtValue, "error", err)
					return status.Errorf(codes.InvalidArgument, "invalid timestamp_ns value: %v", err)
				}
				ticketData.TsNs = &tsVal
				s.logger.Debug("Added time travel to ticket",
					"schema", schemaName,
					"table", tableOrFunctionName,
					"unit", "timestamp_ns",
					"value", tsVal,
				)
			case "VERSION", "version":
				// Parse version number as Unix seconds
				var tsVal int64
				if _, err := fmt.Sscanf(request.Parameters.AtValue, "%d", &tsVal); err != nil {
					s.logger.Error("Failed to parse version", "at_value", request.Parameters.AtValue, "error", err)
					return status.Errorf(codes.InvalidArgument, "invalid version value: %v", err)
				}
				ticketData.Ts = &tsVal
				s.logger.Debug("Added time travel to ticket",
					"schema", schemaName,
					"table", tableOrFunctionName,
					"unit", "version",
					"value", tsVal,
				)
			default:
				s.logger.Error("Unsupported at_unit", "at_unit", request.Parameters.AtUnit)
				return status.Errorf(codes.InvalidArgument, "unsupported at_unit: %s", request.Parameters.AtUnit)
			}
		}

		ticket, err = json.Marshal(ticketData)
		if err != nil {
			s.logger.Error("Failed to encode ticket", "error", err)
			return status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
		}
	}

	// Create FlightEndpoint with location
	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{
			Ticket: ticket,
		},
	}

	// Add location if server address is configured
	if s.address != "" {
		endpoint.Location = []*flight.Location{
			{
				Uri: "grpc://" + s.address,
			},
		}
	}

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
		"table", tableOrFunctionName,
		"endpoint_count", 1,
	)
	return nil
}

// handleCreateTransaction returns a transaction identifier.
// This is an optional Airport action for transaction management.
//
//nolint:unparam
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

	// For now, return nil identifier (no transaction support)
	// Per spec: GetTransactionIdentifierResult with optional identifier
	// IMPORTANT: The field must be present in the map, with nil value
	response := map[string]interface{}{
		"identifier": nil, // Explicitly set to nil for optional field
	}

	responseBody, err := msgpack.Encode(response)
	if err != nil {
		s.logger.Error("Failed to encode transaction response", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	result := &flight.Result{
		Body: responseBody,
	}

	if err := stream.Send(result); err != nil {
		s.logger.Error("Failed to send transaction result", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleCreateTransaction completed", "has_transaction", false)
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
