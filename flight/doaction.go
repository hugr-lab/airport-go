package flight

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
	"github.com/hugr-lab/airport-go/internal/serialize"
)

// DoAction executes server actions including scalar function invocation.
// This RPC supports:
//   - Scalar function execution
//   - Table function schema discovery
//   - Custom server commands
func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := stream.Context()

	s.logger.Debug("DoAction called",
		"type", action.GetType(),
		"body_size", len(action.GetBody()),
	)

	actionType := action.GetType()

	switch actionType {
	case "ExecuteScalarFunction":
		return s.executeScalarFunction(ctx, action, stream)

	case "GetTableFunctionInfo":
		return s.getTableFunctionInfo(ctx, action, stream)

	case "table_function_flight_info":
		return s.handleTableFunctionFlightInfo(ctx, action, stream)

	// flight_info is used for time travel queries with AT syntax
	case "flight_info":
		return s.handleFlightInfo(ctx, action, stream)

	// DDL operations
	case "CreateSchema", "DropSchema", "CreateTable", "DropTable", "AlterTableAddColumn", "AlterTableDropColumn":
		return s.handleDDLAction(action, stream)

	// DML operations
	case "Delete":
		return s.handleDMLAction(action, stream)

	// Required Airport actions
	case "list_schemas":
		return s.handleListSchemas(ctx, action, stream)

	case "endpoints":
		return s.handleEndpoints(ctx, action, stream)

	// Optional Airport actions
	case "list_tables":
		return s.handleListTables(ctx, action, stream)

	case "create_transaction":
		return s.handleCreateTransaction(ctx, action, stream)

	default:
		return status.Errorf(codes.Unimplemented, "unknown action type: %s", actionType)
	}
}

func (s *Server) handleListSchemas(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode parameters if provided
	var params struct {
		CatalogName string `msgpack:"catalog_name"`
	}

	if len(action.GetBody()) > 0 {
		if err := msgpack.Decode(action.GetBody(), &params); err != nil {
			s.logger.Error("Failed to decode list_schemas parameters", "error", err)
			// Continue anyway - treat as no parameters
		}
	}

	s.logger.Debug("handleListSchemas called", "catalog_name", params.CatalogName)

	// Get all schemas from catalog
	catalogSchemas, err := s.catalog.Schemas(ctx)
	if err != nil {
		s.logger.Error("Failed to get schemas", "error", err)
		return status.Errorf(codes.Internal, "failed to get schemas: %v", err)
	}

	// Build AirportSerializedCatalogRoot structure to match C++ code expectations
	// Looking at the C++ code, it accesses: catalog_root.schemas where each schema has .name field
	// The MSGPACK_DEFINE_MAP in the docs shows "schema" but the actual C++ uses .name
	schemaObjects := make([]map[string]interface{}, 0, len(catalogSchemas))
	for _, schema := range catalogSchemas {
		// Generate serialized schema contents with FlightInfo for all tables
		serializedContents, sha256Hash, err := s.serializeSchemaContents(ctx, schema)
		if err != nil {
			s.logger.Error("Failed to serialize schema contents",
				"schema", schema.Name(),
				"error", err,
			)
			return status.Errorf(codes.Internal, "failed to serialize schema contents: %v", err)
		}

		// AirportSerializedContentsWithSHA256Hash for each schema
		schemaContents := map[string]interface{}{
			"sha256":     sha256Hash,
			"url":        nil,                // Optional field must be present
			"serialized": serializedContents, // Inline serialized FlightInfo data
		}

		// Mark "some_schema" as default (first schema is default)
		isDefault := len(schemaObjects) == 0 // First schema is default

		schemaObj := map[string]interface{}{
			"name":        schema.Name(), // C++ code uses schema.name
			"description": schema.Comment(),
			"tags":        map[string]string{},
			"contents":    schemaContents,
			"is_default":  isDefault, // Mark main schema as default
		}
		schemaObjects = append(schemaObjects, schemaObj)
	}

	// Create the catalog root structure
	// C++ struct AirportSerializedCatalogRoot has ALL fields required by MSGPACK_DEFINE_MAP
	// AirportSerializedContentsWithSHA256Hash for catalog contents
	catalogContents := map[string]interface{}{
		"sha256":     "0000000000000000000000000000000000000000000000000000000000000000",
		"url":        nil, // Optional field must be present
		"serialized": nil, // Optional field must be present
	}

	// AirportGetCatalogVersionResult
	versionInfo := map[string]interface{}{
		"catalog_version": uint64(1),
		"is_fixed":        true,
	}

	catalogRoot := map[string]interface{}{
		"contents":     catalogContents,
		"schemas":      schemaObjects,
		"version_info": versionInfo,
	}

	// Serialize to MessagePack
	uncompressed, err := msgpack.Encode(catalogRoot)
	if err != nil {
		s.logger.Error("Failed to encode catalog root", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	// Compress with ZStandard (use the serialize package's compression)
	compressed, err := s.compressCatalog(uncompressed)
	if err != nil {
		s.logger.Error("Failed to compress catalog", "error", err)
		return status.Errorf(codes.Internal, "failed to compress response: %v", err)
	}

	// Create AirportSerializedCompressedContent
	// CRITICAL: MSGPACK_DEFINE (not _MAP) encodes as ARRAY, not map!
	// C++ struct uses MSGPACK_DEFINE(length, data) which creates [length, data] array
	compressedContent := []interface{}{
		uint32(len(uncompressed)), // length as first element
		string(compressed),        // data as second element
	}

	responseBody, err := msgpack.Encode(compressedContent)
	if err != nil {
		s.logger.Error("Failed to encode compressed content", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	// Send result
	result := &flight.Result{
		Body: responseBody,
	}

	if err := stream.Send(result); err != nil {
		s.logger.Error("Failed to send schemas", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleListSchemas completed",
		"schema_count", len(catalogSchemas),
		"uncompressed_bytes", len(uncompressed),
		"compressed_bytes", len(compressed),
	)
	return nil
}

// compressCatalog compresses data using ZStandard
func (s *Server) compressCatalog(data []byte) ([]byte, error) {
	// Use the serialize package's compression function
	return serialize.CompressCatalog(data)
}

// serializeSchemaContents generates serialized Flight IPC stream containing FlightInfo
// for all tables and functions in the schema. Returns the serialized bytes and SHA256 hash.
// Format: ZStandard compressed msgpack array of serialized Arrow FlightInfo structures
func (s *Server) serializeSchemaContents(ctx context.Context, schema catalog.Schema) (string, string, error) {
	// Get all tables in the schema
	tables, err := schema.Tables(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to get tables: %w", err)
	}

	// Get all table functions in the schema
	tableFunctions, err := schema.TableFunctions(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to get table functions: %w", err)
	}

	// Get all scalar functions in the schema
	scalarFunctions, err := schema.ScalarFunctions(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to get scalar functions: %w", err)
	}

	// Get all table functions (in/out) in the schema
	tableFunctionsInOut, err := schema.TableFunctionsInOut(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to get table functions (in/out): %w", err)
	}

	// Create msgpack array of serialized FlightInfo (protobuf) for each table and function
	flightInfoBytesArray := make([][]byte, 0, len(tables)+len(tableFunctions)+len(scalarFunctions)+len(tableFunctionsInOut))

	// Serialize tables
	for _, table := range tables {
		arrowSchema := table.ArrowSchema()
		if arrowSchema == nil {
			continue
		}

		// Create Flight app_metadata matching AirportSerializedFlightAppMetadata
		appMetadata := map[string]interface{}{
			"type":         "table",
			"schema":       schema.Name(),
			"catalog":      "", // Empty catalog name
			"name":         table.Name(),
			"comment":      table.Comment(),
			"input_schema": nil,
			"action_name":  nil,
			"description":  nil,
			"extra_data":   nil,
		}

		appMetadataBytes, err := msgpack.Encode(appMetadata)
		if err != nil {
			return "", "", fmt.Errorf("failed to encode app metadata: %w", err)
		}

		// Create FlightDescriptor
		descriptor := &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{schema.Name(), table.Name()},
		}

		// Generate ticket for this table
		ticket, err := EncodeTicket(schema.Name(), table.Name())
		if err != nil {
			return "", "", fmt.Errorf("failed to encode ticket: %w", err)
		}

		// Create FlightInfo with endpoint
		// The endpoint tells DuckDB how to fetch data via DoGet
		flightInfo := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(arrowSchema, s.allocator),
			FlightDescriptor: descriptor,
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{
						Ticket: ticket,
					},
					// Location can be empty - DuckDB will use the same connection
				},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
			Ordered:      false,
			AppMetadata:  appMetadataBytes,
		}

		// Serialize FlightInfo as protobuf
		flightInfoBytes, err := proto.Marshal(flightInfo)
		if err != nil {
			return "", "", fmt.Errorf("failed to marshal FlightInfo: %w", err)
		}

		flightInfoBytesArray = append(flightInfoBytesArray, flightInfoBytes)
	}

	// Serialize table functions
	for _, tableFunc := range tableFunctions {
		// Get function signature to build input schema
		signature := tableFunc.Signature()

		// Build Arrow schema from function parameters
		inputFields := make([]arrow.Field, len(signature.Parameters))
		for i, paramType := range signature.Parameters {
			inputFields[i] = arrow.Field{
				Name: fmt.Sprintf("param%d", i+1),
				Type: paramType,
			}
		}
		inputSchema := arrow.NewSchema(inputFields, nil)

		// Serialize input schema for DuckDB
		inputSchemaBytes := flight.SerializeSchema(inputSchema, s.allocator)

		// For dynamic schema table functions, use empty output schema
		// The actual schema will be determined by SchemaForParameters at call time
		outputSchema := arrow.NewSchema([]arrow.Field{}, nil)

		// Create Flight app_metadata for table function
		appMetadata := map[string]interface{}{
			"type":         "table_function",
			"schema":       schema.Name(),
			"catalog":      "", // Empty catalog name
			"name":         tableFunc.Name(),
			"comment":      tableFunc.Comment(),
			"input_schema": inputSchemaBytes, // Serialized Arrow schema of parameters
			"action_name":  "table_function_flight_info",
			"description":  tableFunc.Comment(),
			"extra_data":   nil,
		}

		appMetadataBytes, err := msgpack.Encode(appMetadata)
		if err != nil {
			return "", "", fmt.Errorf("failed to encode table function app metadata: %w", err)
		}

		// Create FlightDescriptor for table function
		descriptor := &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{schema.Name(), tableFunc.Name()},
		}

		// For table functions, we don't create a ticket upfront since parameters are needed
		// Instead, use empty ticket - DuckDB will call GetTableFunctionInfo when needed
		emptyTicket := []byte("{}")

		// Create FlightInfo with empty output schema (actual schema determined at call time)
		flightInfo := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(outputSchema, s.allocator),
			FlightDescriptor: descriptor,
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{
						Ticket: emptyTicket,
					},
				},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
			Ordered:      false,
			AppMetadata:  appMetadataBytes,
		}

		// Serialize FlightInfo as protobuf
		flightInfoBytes, err := proto.Marshal(flightInfo)
		if err != nil {
			return "", "", fmt.Errorf("failed to marshal table function FlightInfo: %w", err)
		}

		flightInfoBytesArray = append(flightInfoBytesArray, flightInfoBytes)
	}

	// Serialize scalar functions
	for _, scalarFunc := range scalarFunctions {
		// Get function signature
		signature := scalarFunc.Signature()

		// Build Arrow schema from function parameters (input schema)
		inputFields := make([]arrow.Field, len(signature.Parameters))
		for i, paramType := range signature.Parameters {
			inputFields[i] = arrow.Field{
				Name: fmt.Sprintf("param%d", i+1),
				Type: paramType,
			}
		}
		inputSchema := arrow.NewSchema(inputFields, nil)

		// Serialize input schema for DuckDB
		inputSchemaBytes := flight.SerializeSchema(inputSchema, s.allocator)

		// Output schema - must be exactly one column for scalar functions
		outputSchema := arrow.NewSchema([]arrow.Field{
			{
				Name: "result",
				Type: signature.ReturnType,
			},
		}, nil)

		// Create Flight app_metadata for scalar function
		appMetadata := map[string]interface{}{
			"type":         "scalar_function",
			"schema":       schema.Name(),
			"catalog":      "", // Empty catalog name
			"name":         scalarFunc.Name(),
			"comment":      scalarFunc.Comment(),
			"input_schema": inputSchemaBytes,  // Serialized Arrow schema of parameters
			"action_name":  scalarFunc.Name(), // Action name for DoExchange
			"description":  scalarFunc.Comment(),
			"extra_data":   nil,
		}

		appMetadataBytes, err := msgpack.Encode(appMetadata)
		if err != nil {
			return "", "", fmt.Errorf("failed to encode scalar function app metadata: %w", err)
		}

		// Create FlightDescriptor for scalar function
		descriptor := &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{schema.Name(), scalarFunc.Name()},
		}

		// Scalar functions don't use tickets - execution is via DoExchange
		emptyTicket := []byte("{}")

		// Create FlightInfo with output schema
		flightInfo := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(outputSchema, s.allocator),
			FlightDescriptor: descriptor,
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{
						Ticket: emptyTicket,
					},
				},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
			Ordered:      false,
			AppMetadata:  appMetadataBytes,
		}

		// Serialize FlightInfo as protobuf
		flightInfoBytes, err := proto.Marshal(flightInfo)
		if err != nil {
			return "", "", fmt.Errorf("failed to marshal scalar function FlightInfo: %w", err)
		}

		flightInfoBytesArray = append(flightInfoBytesArray, flightInfoBytes)
	}

	// Serialize table functions (in/out)
	for _, tableFuncInOut := range tableFunctionsInOut {
		// Get function signature
		signature := tableFuncInOut.Signature()

		// Build Arrow schema from function parameters
		// For in/out functions, the last parameter represents the table input
		// Mark it with is_table_type metadata so DuckDB knows to use DoExchange
		inputFields := make([]arrow.Field, len(signature.Parameters))
		for i, paramType := range signature.Parameters {
			fieldName := fmt.Sprintf("param%d", i+1)

			// Last parameter is the table input - mark with is_table_type metadata
			if i == len(signature.Parameters)-1 {
				inputFields[i] = arrow.Field{
					Name: fieldName,
					Type: paramType,
					Metadata: arrow.MetadataFrom(map[string]string{
						"is_table_type": "1", // Non-empty string marks this as table input
					}),
				}
			} else {
				inputFields[i] = arrow.Field{
					Name: fieldName,
					Type: paramType,
				}
			}
		}
		inputSchema := arrow.NewSchema(inputFields, nil)

		// Serialize input schema for DuckDB
		inputSchemaBytes := flight.SerializeSchema(inputSchema, s.allocator)

		// For in/out table functions, use empty output schema
		// The actual schema will be determined by SchemaForParameters at call time
		outputSchema := arrow.NewSchema([]arrow.Field{}, nil)

		// Create Flight app_metadata for table function (in/out)
		appMetadata := map[string]interface{}{
			"type":         "table_function",
			"schema":       schema.Name(),
			"catalog":      "", // Empty catalog name
			"name":         tableFuncInOut.Name(),
			"comment":      tableFuncInOut.Comment(),
			"input_schema": inputSchemaBytes,             // Serialized Arrow schema of parameters
			"action_name":  "table_function_flight_info", // Use same action as normal table functions
			"description":  tableFuncInOut.Comment(),
			"extra_data":   nil,
		}

		appMetadataBytes, err := msgpack.Encode(appMetadata)
		if err != nil {
			return "", "", fmt.Errorf("failed to encode table function (in/out) app metadata: %w", err)
		}

		// Create FlightDescriptor for table function
		descriptor := &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{schema.Name(), tableFuncInOut.Name()},
		}

		// For table functions, we don't create a ticket upfront since parameters are needed
		// Instead, use empty ticket - DuckDB will call table_function_flight_info when needed
		emptyTicket := []byte("{}")

		// Create FlightInfo with empty output schema (actual schema determined at call time)
		flightInfo := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(outputSchema, s.allocator),
			FlightDescriptor: descriptor,
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{
						Ticket: emptyTicket,
					},
				},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
			Ordered:      false,
			AppMetadata:  appMetadataBytes,
		}

		// Serialize FlightInfo as protobuf
		flightInfoBytes, err := proto.Marshal(flightInfo)
		if err != nil {
			return "", "", fmt.Errorf("failed to marshal table function (in/out) FlightInfo: %w", err)
		}

		flightInfoBytesArray = append(flightInfoBytesArray, flightInfoBytes)
	}

	s.logger.Debug("Serialized schema contents",
		"schema", schema.Name(),
		"tables", len(tables),
		"table_functions", len(tableFunctions),
		"scalar_functions", len(scalarFunctions),
		"table_functions_in_out", len(tableFunctionsInOut),
	)

	// Serialize the array of FlightInfo bytes to msgpack
	uncompressed, err := msgpack.Encode(flightInfoBytesArray)
	if err != nil {
		return "", "", fmt.Errorf("failed to encode FlightInfo bytes array: %w", err)
	}

	// Compress using ZStandard
	compressed, err := s.compressCatalog(uncompressed)
	if err != nil {
		return "", "", fmt.Errorf("failed to compress schema contents: %w", err)
	}

	// Wrap in AirportSerializedCompressedContent format: [length, data]
	compressedContent := []interface{}{
		uint32(len(uncompressed)),
		string(compressed),
	}

	// Serialize the compressed content wrapper
	serialized, err := msgpack.Encode(compressedContent)
	if err != nil {
		return "", "", fmt.Errorf("failed to encode compressed content: %w", err)
	}

	// Calculate SHA256 hash of the final serialized value
	hash := sha256.Sum256(serialized)
	hashHex := hex.EncodeToString(hash[:])

	s.logger.Debug("Generated schema contents",
		"tables", len(tables),
		"uncompressed_bytes", len(uncompressed),
		"compressed_bytes", len(compressed),
		"serialized_bytes", len(serialized),
		"sha256", hashHex,
	)

	// Return the serialized compressed content
	return string(serialized), hashHex, nil
}

// handleListTables returns list of all tables in a specific schema.
// This is used by DuckDB Airport extension for table discovery.
// Returns MessagePack-encoded list of tables.
func (s *Server) handleListTables(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode schema name from action body
	var params struct {
		SchemaName string `msgpack:"schema_name"`
	}

	if len(action.GetBody()) > 0 {
		if err := msgpack.Decode(action.GetBody(), &params); err != nil {
			s.logger.Error("Failed to decode list_tables parameters", "error", err)
			return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
		}
	}

	// If no schema specified, list tables from all schemas
	if params.SchemaName == "" {
		return s.handleListAllTables(ctx, stream)
	}

	s.logger.Debug("handleListTables called", "schema", params.SchemaName)

	// Get schema from catalog
	schema, err := s.catalog.Schema(ctx, params.SchemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.SchemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", params.SchemaName)
	}

	// Get tables from schema
	tables, err := schema.Tables(ctx)
	if err != nil {
		s.logger.Error("Failed to get tables", "schema", params.SchemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get tables: %v", err)
	}

	// Build table names list
	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, table.Name())
	}

	// Encode as MessagePack
	responseBody, err := msgpack.Encode(map[string]interface{}{
		"schema": params.SchemaName,
		"tables": tableNames,
	})
	if err != nil {
		s.logger.Error("Failed to encode tables", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	// Send result
	result := &flight.Result{
		Body: responseBody,
	}

	if err := stream.Send(result); err != nil {
		s.logger.Error("Failed to send tables", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleListTables completed", "schema", params.SchemaName, "table_count", len(tables))
	return nil
}

// handleListAllTables returns tables from all schemas.
// Returns MessagePack-encoded map of schema names to table lists.
func (s *Server) handleListAllTables(ctx context.Context, stream flight.FlightService_DoActionServer) error {
	s.logger.Debug("handleListAllTables called")

	// Get all schemas
	schemas, err := s.catalog.Schemas(ctx)
	if err != nil {
		s.logger.Error("Failed to get schemas", "error", err)
		return status.Errorf(codes.Internal, "failed to get schemas: %v", err)
	}

	// Build map of schema -> table names
	tablesMap := make(map[string][]string)
	totalTables := 0

	for _, schema := range schemas {
		tables, err := schema.Tables(ctx)
		if err != nil {
			s.logger.Error("Failed to get tables", "schema", schema.Name(), "error", err)
			continue
		}

		tableNames := make([]string, 0, len(tables))
		for _, table := range tables {
			tableNames = append(tableNames, table.Name())
			totalTables++
		}
		tablesMap[schema.Name()] = tableNames
	}

	// Encode as MessagePack
	responseBody, err := msgpack.Encode(map[string]interface{}{
		"tables": tablesMap,
	})
	if err != nil {
		s.logger.Error("Failed to encode tables", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	// Send result
	result := &flight.Result{
		Body: responseBody,
	}

	if err := stream.Send(result); err != nil {
		s.logger.Error("Failed to send tables", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleListAllTables completed", "schema_count", len(schemas), "table_count", totalTables)
	return nil
}

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
