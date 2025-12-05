package flight

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
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
	case "table_function_flight_info":
		return s.handleTableFunctionFlightInfo(ctx, action, stream)

	// flight_info is used for time travel queries with AT syntax
	case "flight_info":
		return s.handleFlightInfo(ctx, action, stream)

	// DDL operations (snake_case per Airport protocol)
	case "create_schema":
		return s.handleCreateSchemaAction(ctx, action, stream)
	case "drop_schema":
		return s.handleDropSchemaAction(ctx, action, stream)
	case "create_table":
		return s.handleCreateTableAction(ctx, action, stream)
	case "drop_table":
		return s.handleDropTableAction(ctx, action, stream)
	case "add_column":
		return s.handleAddColumnAction(ctx, action, stream)
	case "remove_column":
		return s.handleRemoveColumnAction(ctx, action, stream)
	case "rename_column":
		return s.handleRenameColumnAction(ctx, action, stream)
	case "rename_table":
		return s.handleRenameTableAction(ctx, action, stream)
	case "change_column_type":
		return s.handleChangeColumnTypeAction(ctx, action, stream)
	case "set_not_null":
		return s.handleSetNotNullAction(ctx, action, stream)
	case "drop_not_null":
		return s.handleDropNotNullAction(ctx, action, stream)
	case "set_default":
		return s.handleSetDefaultAction(ctx, action, stream)
	case "add_field":
		return s.handleAddFieldAction(ctx, action, stream)
	case "rename_field":
		return s.handleRenameFieldAction(ctx, action, stream)

	// Catalog version action
	case "catalog_version":
		return s.handleCatalogVersionAction(ctx, action, stream)

	// Statistics action
	case "column_statistics":
		return s.handleColumnStatisticsAction(ctx, action, stream)

	// Required Airport actions
	case "list_schemas":
		return s.handleListSchemas(ctx, action, stream)

	case "endpoints":
		return s.handleEndpoints(ctx, action, stream)

	// Optional Airport actions
	case "create_transaction":
		return s.handleCreateTransaction(ctx, action, stream)

	case "get_transaction_status":
		return s.handleGetTransactionStatus(ctx, action, stream)

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

		isDefault := schema.Name() == catalog.DefaultSchemaName // First schema is default

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
		arrowSchema := table.ArrowSchema(nil)
		if arrowSchema == nil {
			continue
		}

		// Check if table implements StatisticsTable interface
		// If so, add "can_produce_statistics" to Arrow schema metadata
		// This tells DuckDB that this table can provide column statistics
		if _, ok := table.(catalog.StatisticsTable); ok {
			arrowSchema = addSchemaMetadata(arrowSchema, "can_produce_statistics", "true")
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

// addSchemaMetadata adds a key-value pair to an Arrow schema's metadata.
// Returns a new schema with the added metadata.
func addSchemaMetadata(schema *arrow.Schema, key, value string) *arrow.Schema {
	// Build metadata map with existing metadata
	metaMap := make(map[string]string)
	existingMeta := schema.Metadata()
	for i := 0; i < existingMeta.Len(); i++ {
		metaMap[existingMeta.Keys()[i]] = existingMeta.Values()[i]
	}
	// Add new key-value
	metaMap[key] = value

	// Build keys and values slices
	keys := make([]string, 0, len(metaMap))
	values := make([]string, 0, len(metaMap))
	for k, v := range metaMap {
		keys = append(keys, k)
		values = append(values, v)
	}

	// Create new schema with updated metadata
	newMeta := arrow.NewMetadata(keys, values)
	return arrow.NewSchema(schema.Fields(), &newMeta)
}
