package flight

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
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

// executeScalarFunction validates scalar function existence and signature.
// For now, scalar functions are primarily for catalog discovery.
// Full execution support via DuckDB will be implemented when Airport extension adds support.
//
// Request format (MessagePack):
//
//	{
//	  "schema": "main",
//	  "function": "UPPERCASE"
//	}
//
// Response format: Function signature and metadata
func (s *Server) executeScalarFunction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode MessagePack parameters
	var params struct {
		Schema   string `msgpack:"schema"`
		Function string `msgpack:"function"`
	}

	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode function parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
	}

	s.logger.Info("Scalar function discovery",
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

	// Get function signature
	signature := targetFunc.Signature()

	s.logger.Info("Found scalar function",
		"name", targetFunc.Name(),
		"comment", targetFunc.Comment(),
		"param_count", len(signature.Parameters),
	)

	// Return function metadata
	result := &flight.Result{
		Body: []byte(fmt.Sprintf("Scalar function %s found with %d parameters", targetFunc.Name(), len(signature.Parameters))),
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

// handleListSchemas returns list of all schemas in the catalog.
// This is used by DuckDB Airport extension for ATTACH operations.
// Returns compressed MessagePack following Airport specification:
// https://airport.query.farm/server_action_list_schemas.html
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

	s.logger.Info("handleListSchemas called", "catalog_name", params.CatalogName)

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
			"url":        nil,                 // Optional field must be present
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
		string(compressed),         // data as second element
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

	s.logger.Info("handleListSchemas completed",
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

	// Create msgpack array of serialized FlightInfo (protobuf) for each table
	flightInfoBytesArray := make([][]byte, 0, len(tables))
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

	s.logger.Info("Generated schema contents",
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

	s.logger.Info("handleListTables called", "schema", params.SchemaName)

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

	s.logger.Info("handleListTables completed", "schema", params.SchemaName, "table_count", len(tables))
	return nil
}

// handleListAllTables returns tables from all schemas.
// Returns MessagePack-encoded map of schema names to table lists.
func (s *Server) handleListAllTables(ctx context.Context, stream flight.FlightService_DoActionServer) error {
	s.logger.Info("handleListAllTables called")

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

	s.logger.Info("handleListAllTables completed", "schema_count", len(schemas), "table_count", totalTables)
	return nil
}

// handleEndpoints returns flight endpoints for a descriptor.
// This is a required Airport action that allows the server to receive additional context.
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

	s.logger.Info("handleEndpoints called",
		"descriptor_type", desc.GetType(),
		"path", desc.GetPath(),
		"has_filters", request.Parameters.JsonFilters != "",
		"column_count", len(request.Parameters.ColumnIDs),
	)

	// Validate descriptor
	if desc.GetType() != flight.DescriptorPATH || len(desc.GetPath()) != 2 {
		s.logger.Error("Invalid descriptor format", "type", desc.GetType(), "path_length", len(desc.GetPath()))
		return status.Errorf(codes.InvalidArgument, "descriptor must be PATH type with 2 elements [schema, table]")
	}

	schemaName := desc.GetPath()[0]
	tableName := desc.GetPath()[1]

	// Generate ticket for this table
	// TODO: Include filter/column/time parameters in ticket
	ticket, err := EncodeTicket(schemaName, tableName)
	if err != nil {
		s.logger.Error("Failed to encode ticket", "error", err)
		return status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
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

	s.logger.Info("handleEndpoints completed",
		"schema", schemaName,
		"table", tableName,
		"endpoint_count", 1,
	)
	return nil
}

// handleCreateTransaction returns a transaction identifier.
// This is an optional Airport action for transaction management.
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

	s.logger.Info("handleCreateTransaction called", "catalog_name", params.CatalogName)

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

	s.logger.Info("handleCreateTransaction completed", "has_transaction", false)
	return nil
}
