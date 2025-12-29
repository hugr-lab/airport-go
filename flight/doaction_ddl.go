package flight

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// Msgpack request parameter structs for DDL operations.
// These match the Airport protocol specification exactly.

// CreateSchemaParams for create_schema action.
type CreateSchemaParams struct {
	CatalogName string            `msgpack:"catalog_name"`
	Schema      string            `msgpack:"schema"`
	Comment     *string           `msgpack:"comment,omitempty"`
	Tags        map[string]string `msgpack:"tags,omitempty"`
}

// DropSchemaParams for drop_schema action.
type DropSchemaParams struct {
	Type           string `msgpack:"type"` // Always "schema"
	CatalogName    string `msgpack:"catalog_name"`
	SchemaName     string `msgpack:"schema_name"`
	Name           string `msgpack:"name"`
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// CreateTableParams for create_table action.
type CreateTableParams struct {
	CatalogName        string   `msgpack:"catalog_name"`
	SchemaName         string   `msgpack:"schema_name"`
	TableName          string   `msgpack:"table_name"`
	ArrowSchema        []byte   `msgpack:"arrow_schema"` // IPC serialized Arrow schema
	OnConflict         string   `msgpack:"on_conflict"`  // "error", "ignore", "replace"
	NotNullConstraints []uint64 `msgpack:"not_null_constraints"`
	UniqueConstraints  []uint64 `msgpack:"unique_constraints"`
	CheckConstraints   []string `msgpack:"check_constraints"`
}

// DropTableParams for drop_table action.
type DropTableParams struct {
	Type           string `msgpack:"type"` // Always "table"
	CatalogName    string `msgpack:"catalog_name"`
	SchemaName     string `msgpack:"schema_name"`
	Name           string `msgpack:"name"`
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// AddColumnParams for add_column action.
type AddColumnParams struct {
	Catalog           string `msgpack:"catalog"`
	Schema            string `msgpack:"schema"`
	Name              string `msgpack:"name"`          // Table name
	ColumnSchema      []byte `msgpack:"column_schema"` // IPC serialized Arrow schema with single field
	IgnoreNotFound    bool   `msgpack:"ignore_not_found"`
	IfColumnNotExists bool   `msgpack:"if_column_not_exists"`
}

// RemoveColumnParams for remove_column action.
type RemoveColumnParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`           // Table name
	RemovedColumn  string `msgpack:"removed_column"` // Column name to remove
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
	IfColumnExists bool   `msgpack:"if_column_exists"`
	Cascade        bool   `msgpack:"cascade"`
}

// RenameColumnParams for rename_column action.
type RenameColumnParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`     // Table name
	OldName        string `msgpack:"old_name"` // Current column name
	NewName        string `msgpack:"new_name"` // Desired column name
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// RenameTableParams for rename_table action.
type RenameTableParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`           // Current table name
	NewTableName   string `msgpack:"new_table_name"` // Desired table name
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// ChangeColumnTypeParams for change_column_type action.
type ChangeColumnTypeParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`          // Table name
	ColumnSchema   []byte `msgpack:"column_schema"` // IPC serialized Arrow schema with single field
	Expression     string `msgpack:"expression"`    // Type conversion expression
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// SetNotNullParams for set_not_null action.
type SetNotNullParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`        // Table name
	ColumnName     string `msgpack:"column_name"` // Column to add NOT NULL constraint
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// DropNotNullParams for drop_not_null action.
type DropNotNullParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`        // Table name
	ColumnName     string `msgpack:"column_name"` // Column to remove NOT NULL constraint
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// SetDefaultParams for set_default action.
type SetDefaultParams struct {
	Catalog        string `msgpack:"catalog"`
	Schema         string `msgpack:"schema"`
	Name           string `msgpack:"name"`        // Table name
	ColumnName     string `msgpack:"column_name"` // Column to set default
	Expression     string `msgpack:"expression"`  // Default value expression
	IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// AddFieldParams for add_field action (struct column field addition).
type AddFieldParams struct {
	Catalog          string `msgpack:"catalog"`
	Schema           string `msgpack:"schema"`
	Name             string `msgpack:"name"`                // Table name
	ColumnSchema     []byte `msgpack:"column_schema"`       // IPC serialized Arrow schema with field path and type
	IfFieldNotExists bool   `msgpack:"if_field_not_exists"` // Conditional field addition
	IgnoreNotFound   bool   `msgpack:"ignore_not_found"`
}

// RenameFieldParams for rename_field action (struct column field renaming).
type RenameFieldParams struct {
	Catalog        string   `msgpack:"catalog"`
	Schema         string   `msgpack:"schema"`
	Name           string   `msgpack:"name"`        // Table name
	ColumnPath     []string `msgpack:"column_path"` // Path to source field
	NewName        string   `msgpack:"new_name"`    // Column new name
	IgnoreNotFound bool     `msgpack:"ignore_not_found"`
}

// CatalogVersionParams for catalog_version action.
type CatalogVersionParams struct {
	CatalogName string `msgpack:"catalog_name"`
}

// CatalogVersionResponse for catalog_version action response.
type CatalogVersionResponse struct {
	CatalogVersion uint64 `msgpack:"catalog_version"`
	IsFixed        bool   `msgpack:"is_fixed"`
}

// handleCreateSchema implements the create_schema DoAction handler.
func (s *Server) handleCreateSchemaAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params CreateSchemaParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode create_schema parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid create_schema payload: %v", err)
	}

	s.logger.Debug("handleCreateSchema called",
		"catalog_name", params.CatalogName,
		"schema", params.Schema,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema name is required")
	}

	// Check if catalog supports dynamic operations
	dynCat, ok := s.catalog.(catalog.DynamicCatalog)
	if !ok {
		return status.Error(codes.Unimplemented, "catalog does not support schema creation")
	}

	// Build options
	opts := catalog.CreateSchemaOptions{
		Tags: params.Tags,
	}
	if params.Comment != nil {
		opts.Comment = *params.Comment
	}

	// Create the schema
	schema, err := dynCat.CreateSchema(ctx, params.Schema, opts)
	if errors.Is(err, catalog.ErrAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "schema %q already exists", params.Schema)
	}
	if err != nil {
		s.logger.Error("Failed to create schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to create schema: %v", err)
	}

	// Generate serialized schema contents
	serializedContents, sha256Hash, err := s.serializeSchemaContents(ctx, schema)
	if err != nil {
		s.logger.Error("Failed to serialize schema contents", "schema", schema.Name(), "error", err)
		return status.Errorf(codes.Internal, "failed to serialize schema contents: %v", err)
	}

	// Build AirportSerializedContentsWithSHA256Hash response
	response := map[string]interface{}{
		"sha256":     sha256Hash,
		"url":        nil,
		"serialized": []byte(serializedContents),
	}

	responseBody, err := msgpack.Encode(response)
	if err != nil {
		s.logger.Error("Failed to encode create_schema response", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
		s.logger.Error("Failed to send create_schema response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleCreateSchema completed", "schema", params.Schema)
	return nil
}

// handleDropSchema implements the drop_schema DoAction handler.
func (s *Server) handleDropSchemaAction(ctx context.Context, action *flight.Action, _ flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params DropSchemaParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode drop_schema parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid drop_schema payload: %v", err)
	}

	// Use Name field as the schema name (per protocol spec)
	schemaName := params.Name
	if schemaName == "" {
		schemaName = params.SchemaName
	}

	s.logger.Debug("handleDropSchema called",
		"catalog_name", params.CatalogName,
		"schema_name", schemaName,
		"ignore_not_found", params.IgnoreNotFound,
	)

	// Check if catalog supports dynamic operations
	dynCat, ok := s.catalog.(catalog.DynamicCatalog)
	if !ok {
		return status.Error(codes.Unimplemented, "catalog does not support schema deletion")
	}

	// Build options
	opts := catalog.DropSchemaOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Drop the schema
	err := dynCat.DropSchema(ctx, schemaName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "schema %q not found", schemaName)
	}
	if errors.Is(err, catalog.ErrSchemaNotEmpty) {
		return status.Errorf(codes.FailedPrecondition, "schema %q contains tables", schemaName)
	}
	if err != nil {
		s.logger.Error("Failed to drop schema", "schema", schemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to drop schema: %v", err)
	}

	// drop_schema returns empty response on success
	s.logger.Debug("handleDropSchema completed", "schema", schemaName)
	return nil
}

// handleCreateTable implements the create_table DoAction handler.
func (s *Server) handleCreateTableAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params CreateTableParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode create_table parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid create_table payload: %v", err)
	}

	s.logger.Debug("handleCreateTable called",
		"catalog_name", params.CatalogName,
		"schema_name", params.SchemaName,
		"table_name", params.TableName,
		"on_conflict", params.OnConflict,
	)

	// Validate required fields
	if params.SchemaName == "" {
		return status.Error(codes.InvalidArgument, "schema_name is required")
	}
	if params.TableName == "" {
		return status.Error(codes.InvalidArgument, "table_name is required")
	}
	if len(params.ArrowSchema) == 0 {
		return status.Error(codes.InvalidArgument, "arrow_schema is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.SchemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.SchemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema %q not found", params.SchemaName)
	}

	// Check if schema supports dynamic operations
	dynSchema, ok := schema.(catalog.DynamicSchema)
	if !ok {
		return status.Error(codes.Unimplemented, "schema does not support table creation")
	}

	// Deserialize Arrow schema
	arrowSchema, err := flight.DeserializeSchema(params.ArrowSchema, s.allocator)
	if err != nil {
		s.logger.Error("Failed to deserialize arrow_schema", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid arrow_schema: %v", err)
	}

	// Map on_conflict parameter
	var onConflict catalog.OnConflict
	switch params.OnConflict {
	case "ignore":
		onConflict = catalog.OnConflictIgnore
	case "replace":
		onConflict = catalog.OnConflictReplace
	case "error", "":
		onConflict = catalog.OnConflictError
	default:
		return status.Errorf(codes.InvalidArgument, "invalid on_conflict value: %q", params.OnConflict)
	}

	// Build options
	opts := catalog.CreateTableOptions{
		OnConflict:         onConflict,
		NotNullConstraints: params.NotNullConstraints,
		UniqueConstraints:  params.UniqueConstraints,
		CheckConstraints:   params.CheckConstraints,
	}

	// Create the table
	table, err := dynSchema.CreateTable(ctx, params.TableName, arrowSchema, opts)
	if errors.Is(err, catalog.ErrAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "table %q already exists", params.TableName)
	}
	if err != nil {
		s.logger.Error("Failed to create table", "table", params.TableName, "error", err)
		return status.Errorf(codes.Internal, "failed to create table: %v", err)
	}

	// Build FlightInfo response
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send create_table response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleCreateTable completed",
		"schema", params.SchemaName,
		"table", params.TableName,
	)
	return nil
}

// handleDropTable implements the drop_table DoAction handler.
func (s *Server) handleDropTableAction(ctx context.Context, action *flight.Action, _ flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params DropTableParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode drop_table parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid drop_table payload: %v", err)
	}

	// Use Name field as the table name (per protocol spec)
	tableName := params.Name

	s.logger.Debug("handleDropTable called",
		"catalog_name", params.CatalogName,
		"schema_name", params.SchemaName,
		"table_name", tableName,
		"ignore_not_found", params.IgnoreNotFound,
	)

	// Validate required fields
	if params.SchemaName == "" {
		return status.Error(codes.InvalidArgument, "schema_name is required")
	}
	if tableName == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.SchemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.SchemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil // Schema doesn't exist, but we're ignoring that
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.SchemaName)
	}

	// Check if schema supports dynamic operations
	dynSchema, ok := schema.(catalog.DynamicSchema)
	if !ok {
		return status.Error(codes.Unimplemented, "schema does not support table deletion")
	}

	// Build options
	opts := catalog.DropTableOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Drop the table
	err = dynSchema.DropTable(ctx, tableName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "table %q not found", tableName)
	}
	if err != nil {
		s.logger.Error("Failed to drop table", "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "failed to drop table: %v", err)
	}

	// drop_table returns empty response on success
	s.logger.Debug("handleDropTable completed", "table", tableName)
	return nil
}

// handleAddColumn implements the add_column DoAction handler.
func (s *Server) handleAddColumnAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params AddColumnParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode add_column parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid add_column payload: %v", err)
	}

	s.logger.Debug("handleAddColumn called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"if_column_not_exists", params.IfColumnNotExists,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if len(params.ColumnSchema) == 0 {
		return status.Error(codes.InvalidArgument, "column_schema is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Deserialize column schema
	columnSchema, err := flight.DeserializeSchema(params.ColumnSchema, s.allocator)
	if err != nil {
		s.logger.Error("Failed to deserialize column_schema", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid column_schema: %v", err)
	}

	// Validate column schema has exactly one field
	if columnSchema.NumFields() != 1 {
		return status.Errorf(codes.InvalidArgument, "column_schema must contain exactly one field, got %d", columnSchema.NumFields())
	}

	// Build options
	opts := catalog.AddColumnOptions{
		IfColumnNotExists: params.IfColumnNotExists,
		IgnoreNotFound:    params.IgnoreNotFound,
	}

	// Add the column
	err = dynTable.AddColumn(ctx, columnSchema, opts)
	if errors.Is(err, catalog.ErrAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "column %q already exists", columnSchema.Field(0).Name)
	}
	if err != nil {
		s.logger.Error("Failed to add column", "column", columnSchema.Field(0).Name, "error", err)
		return status.Errorf(codes.Internal, "failed to add column: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send add_column response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleAddColumn completed",
		"table", params.Name,
		"column", columnSchema.Field(0).Name,
	)
	return nil
}

// handleRemoveColumn implements the remove_column DoAction handler.
func (s *Server) handleRemoveColumnAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params RemoveColumnParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode remove_column parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid remove_column payload: %v", err)
	}

	s.logger.Debug("handleRemoveColumn called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"column", params.RemovedColumn,
		"if_column_exists", params.IfColumnExists,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if params.RemovedColumn == "" {
		return status.Error(codes.InvalidArgument, "removed_column is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Build options
	opts := catalog.RemoveColumnOptions{
		IfColumnExists: params.IfColumnExists,
		IgnoreNotFound: params.IgnoreNotFound,
		Cascade:        params.Cascade,
	}

	// Remove the column
	err = dynTable.RemoveColumn(ctx, params.RemovedColumn, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column %q not found", params.RemovedColumn)
	}
	if err != nil {
		s.logger.Error("Failed to remove column", "column", params.RemovedColumn, "error", err)
		return status.Errorf(codes.Internal, "failed to remove column: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send remove_column response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleRemoveColumn completed",
		"table", params.Name,
		"column", params.RemovedColumn,
	)
	return nil
}

// handleRenameColumnAction implements the rename_column DoAction handler.
func (s *Server) handleRenameColumnAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params RenameColumnParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode rename_column parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid rename_column payload: %v", err)
	}

	s.logger.Debug("handleRenameColumn called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"old_name", params.OldName,
		"new_name", params.NewName,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if params.OldName == "" {
		return status.Error(codes.InvalidArgument, "old_name is required")
	}
	if params.NewName == "" {
		return status.Error(codes.InvalidArgument, "new_name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Build options
	opts := catalog.RenameColumnOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Rename the column
	err = dynTable.RenameColumn(ctx, params.OldName, params.NewName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column %q not found", params.OldName)
	}
	if errors.Is(err, catalog.ErrAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "column %q already exists", params.NewName)
	}
	if err != nil {
		s.logger.Error("Failed to rename column", "old_name", params.OldName, "new_name", params.NewName, "error", err)
		return status.Errorf(codes.Internal, "failed to rename column: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send rename_column response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleRenameColumn completed",
		"table", params.Name,
		"old_name", params.OldName,
		"new_name", params.NewName,
	)
	return nil
}

// handleRenameTableAction implements the rename_table DoAction handler.
func (s *Server) handleRenameTableAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params RenameTableParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode rename_table parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid rename_table payload: %v", err)
	}

	s.logger.Debug("handleRenameTable called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"new_table_name", params.NewTableName,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if params.NewTableName == "" {
		return status.Error(codes.InvalidArgument, "new_table_name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Check if schema supports dynamic operations
	dynSchema, ok := schema.(catalog.DynamicSchema)
	if !ok {
		return status.Error(codes.Unimplemented, "schema does not support table modification")
	}

	// Build options
	opts := catalog.RenameTableOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Rename the table
	err = dynSchema.RenameTable(ctx, params.Name, params.NewTableName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}
	if errors.Is(err, catalog.ErrAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "table %q already exists", params.NewTableName)
	}
	if err != nil {
		s.logger.Error("Failed to rename table", "old_name", params.Name, "new_name", params.NewTableName, "error", err)
		return status.Errorf(codes.Internal, "failed to rename table: %v", err)
	}

	// Look up renamed table for FlightInfo response
	table, err := schema.Table(ctx, params.NewTableName)
	if err != nil {
		s.logger.Error("Failed to get renamed table", "table", params.NewTableName, "error", err)
		return status.Errorf(codes.Internal, "failed to get renamed table: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send rename_table response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleRenameTable completed",
		"old_name", params.Name,
		"new_name", params.NewTableName,
	)
	return nil
}

// handleChangeColumnTypeAction implements the change_column_type DoAction handler.
func (s *Server) handleChangeColumnTypeAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params ChangeColumnTypeParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode change_column_type parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid change_column_type payload: %v", err)
	}

	s.logger.Debug("handleChangeColumnType called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"expression", params.Expression,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if len(params.ColumnSchema) == 0 {
		return status.Error(codes.InvalidArgument, "column_schema is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Deserialize column schema
	columnSchema, err := flight.DeserializeSchema(params.ColumnSchema, s.allocator)
	if err != nil {
		s.logger.Error("Failed to deserialize column_schema", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid column_schema: %v", err)
	}

	// Build options
	opts := catalog.ChangeColumnTypeOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Change the column type
	err = dynTable.ChangeColumnType(ctx, columnSchema, params.Expression, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column not found")
	}
	if err != nil {
		s.logger.Error("Failed to change column type", "error", err)
		return status.Errorf(codes.Internal, "failed to change column type: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send change_column_type response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleChangeColumnType completed", "table", params.Name)
	return nil
}

// handleSetNotNullAction implements the set_not_null DoAction handler.
func (s *Server) handleSetNotNullAction(ctx context.Context, action *flight.Action, _ flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params SetNotNullParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode set_not_null parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid set_not_null payload: %v", err)
	}

	s.logger.Debug("handleSetNotNull called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"column_name", params.ColumnName,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if params.ColumnName == "" {
		return status.Error(codes.InvalidArgument, "column_name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Build options
	opts := catalog.SetNotNullOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Set NOT NULL constraint
	err = dynTable.SetNotNull(ctx, params.ColumnName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column %q not found", params.ColumnName)
	}
	if err != nil {
		s.logger.Error("Failed to set NOT NULL", "column", params.ColumnName, "error", err)
		return status.Errorf(codes.Internal, "failed to set NOT NULL: %v", err)
	}

	// set_not_null returns no response on success
	s.logger.Debug("handleSetNotNull completed",
		"table", params.Name,
		"column", params.ColumnName,
	)
	return nil
}

// handleDropNotNullAction implements the drop_not_null DoAction handler.
func (s *Server) handleDropNotNullAction(ctx context.Context, action *flight.Action, _ flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params DropNotNullParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode drop_not_null parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid drop_not_null payload: %v", err)
	}

	s.logger.Debug("handleDropNotNull called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"column_name", params.ColumnName,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if params.ColumnName == "" {
		return status.Error(codes.InvalidArgument, "column_name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Build options
	opts := catalog.DropNotNullOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Drop NOT NULL constraint
	err = dynTable.DropNotNull(ctx, params.ColumnName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column %q not found", params.ColumnName)
	}
	if err != nil {
		s.logger.Error("Failed to drop NOT NULL", "column", params.ColumnName, "error", err)
		return status.Errorf(codes.Internal, "failed to drop NOT NULL: %v", err)
	}

	// drop_not_null returns no response on success
	s.logger.Debug("handleDropNotNull completed",
		"table", params.Name,
		"column", params.ColumnName,
	)
	return nil
}

// handleSetDefaultAction implements the set_default DoAction handler.
func (s *Server) handleSetDefaultAction(ctx context.Context, action *flight.Action, _ flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params SetDefaultParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode set_default parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid set_default payload: %v", err)
	}

	s.logger.Debug("handleSetDefault called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"column_name", params.ColumnName,
		"expression", params.Expression,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if params.ColumnName == "" {
		return status.Error(codes.InvalidArgument, "column_name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Build options
	opts := catalog.SetDefaultOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Set default value
	err = dynTable.SetDefault(ctx, params.ColumnName, params.Expression, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column %q not found", params.ColumnName)
	}
	if err != nil {
		s.logger.Error("Failed to set default", "column", params.ColumnName, "error", err)
		return status.Errorf(codes.Internal, "failed to set default: %v", err)
	}

	// set_default returns no response on success
	s.logger.Debug("handleSetDefault completed",
		"table", params.Name,
		"column", params.ColumnName,
	)
	return nil
}

// handleAddFieldAction implements the add_field DoAction handler.
func (s *Server) handleAddFieldAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params AddFieldParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode add_field parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid add_field payload: %v", err)
	}

	s.logger.Debug("handleAddField called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"if_field_not_exists", params.IfFieldNotExists,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if len(params.ColumnSchema) == 0 {
		return status.Error(codes.InvalidArgument, "column_schema is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Deserialize column schema
	columnSchema, err := flight.DeserializeSchema(params.ColumnSchema, s.allocator)
	if err != nil {
		s.logger.Error("Failed to deserialize column_schema", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid column_schema: %v", err)
	}

	// Build options
	opts := catalog.AddFieldOptions{
		IgnoreNotFound:   params.IgnoreNotFound,
		IfFieldNotExists: params.IfFieldNotExists,
	}

	// Add the field
	err = dynTable.AddField(ctx, columnSchema, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "column path not found")
	}
	if errors.Is(err, catalog.ErrAlreadyExists) {
		return status.Errorf(codes.AlreadyExists, "field already exists")
	}
	if err != nil {
		s.logger.Error("Failed to add field", "error", err)
		return status.Errorf(codes.Internal, "failed to add field: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send add_field response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleAddField completed", "table", params.Name)
	return nil
}

// handleRenameFieldAction implements the rename_field DoAction handler.
func (s *Server) handleRenameFieldAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params RenameFieldParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode rename_field parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid rename_field payload: %v", err)
	}

	s.logger.Debug("handleRenameField called",
		"catalog", params.Catalog,
		"schema", params.Schema,
		"table", params.Name,
		"column_path", params.ColumnPath,
		"new_name", params.NewName,
	)

	// Validate required fields
	if params.Schema == "" {
		return status.Error(codes.InvalidArgument, "schema is required")
	}
	if params.Name == "" {
		return status.Error(codes.InvalidArgument, "table name is required")
	}
	if len(params.ColumnPath) == 0 {
		return status.Error(codes.InvalidArgument, "column_path is required")
	}
	if params.NewName == "" {
		return status.Error(codes.InvalidArgument, "new_name is required")
	}

	// Look up schema
	schema, err := s.catalog.Schema(ctx, params.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", params.Schema, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "schema %q not found", params.Schema)
	}

	// Look up table
	table, err := schema.Table(ctx, params.Name)
	if err != nil {
		s.logger.Error("Failed to get table", "table", params.Name, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		if params.IgnoreNotFound {
			return nil
		}
		return status.Errorf(codes.NotFound, "table %q not found", params.Name)
	}

	// Check if table supports dynamic operations
	dynTable, ok := table.(catalog.DynamicTable)
	if !ok {
		return status.Error(codes.Unimplemented, "table does not support column modification")
	}

	// Build options
	opts := catalog.RenameFieldOptions{
		IgnoreNotFound: params.IgnoreNotFound,
	}

	// Rename the field
	err = dynTable.RenameField(ctx, params.ColumnPath, params.NewName, opts)
	if errors.Is(err, catalog.ErrNotFound) {
		return status.Errorf(codes.NotFound, "field path not found")
	}
	if err != nil {
		s.logger.Error("Failed to rename field", "column_path", params.ColumnPath, "new_name", params.NewName, "error", err)
		return status.Errorf(codes.Internal, "failed to rename field: %v", err)
	}

	// Build FlightInfo response with updated schema
	flightInfo, err := s.buildTableFlightInfo(ctx, schema, table)
	if err != nil {
		s.logger.Error("Failed to build FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to build FlightInfo: %v", err)
	}

	// Serialize FlightInfo as protobuf
	flightInfoBytes, err := proto.Marshal(flightInfo)
	if err != nil {
		s.logger.Error("Failed to marshal FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to marshal FlightInfo: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: flightInfoBytes}); err != nil {
		s.logger.Error("Failed to send rename_field response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleRenameField completed",
		"table", params.Name,
		"column_path", params.ColumnPath,
		"new_name", params.NewName,
	)
	return nil
}

// handleCatalogVersionAction implements the catalog_version DoAction handler.
func (s *Server) handleCatalogVersionAction(ctx context.Context, action *flight.Action, stream flight.FlightService_DoActionServer) error {
	// Decode msgpack parameters
	var params CatalogVersionParams
	if err := msgpack.Decode(action.GetBody(), &params); err != nil {
		s.logger.Error("Failed to decode catalog_version parameters", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid catalog_version payload: %v", err)
	}

	s.logger.Debug("handleCatalogVersion called", "catalog_name", params.CatalogName)

	// Check if catalog supports versioning
	versionedCat, ok := s.catalog.(catalog.VersionedCatalog)
	if !ok {
		// Return a default fixed version if catalog doesn't support versioning
		response := CatalogVersionResponse{
			CatalogVersion: 1,
			IsFixed:        true,
		}

		responseBody, err := msgpack.Encode(response)
		if err != nil {
			s.logger.Error("Failed to encode catalog_version response", "error", err)
			return status.Errorf(codes.Internal, "failed to encode response: %v", err)
		}

		if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
			s.logger.Error("Failed to send catalog_version response", "error", err)
			return status.Errorf(codes.Internal, "failed to send result: %v", err)
		}

		s.logger.Debug("handleCatalogVersion completed (default)", "version", 1, "is_fixed", true)
		return nil
	}

	// Get version from the versioned catalog
	version, err := versionedCat.CatalogVersion(ctx)
	if err != nil {
		s.logger.Error("Failed to get catalog version", "error", err)
		return status.Errorf(codes.Internal, "failed to get catalog version: %v", err)
	}

	response := CatalogVersionResponse{
		CatalogVersion: version.Version,
		IsFixed:        version.IsFixed,
	}

	responseBody, err := msgpack.Encode(response)
	if err != nil {
		s.logger.Error("Failed to encode catalog_version response", "error", err)
		return status.Errorf(codes.Internal, "failed to encode response: %v", err)
	}

	if err := stream.Send(&flight.Result{Body: responseBody}); err != nil {
		s.logger.Error("Failed to send catalog_version response", "error", err)
		return status.Errorf(codes.Internal, "failed to send result: %v", err)
	}

	s.logger.Debug("handleCatalogVersion completed",
		"version", version.Version,
		"is_fixed", version.IsFixed,
	)
	return nil
}

// buildTableFlightInfo creates a FlightInfo for a table.
func (s *Server) buildTableFlightInfo(_ context.Context, schema catalog.Schema, table catalog.Table) (*flight.FlightInfo, error) {
	arrowSchema := table.ArrowSchema(nil)
	if arrowSchema == nil {
		return nil, errors.New("table has no schema")
	}

	// Create Flight app_metadata matching AirportSerializedFlightAppMetadata
	appMetadata := map[string]interface{}{
		"type":         "table",
		"schema":       schema.Name(),
		"catalog":      "",
		"name":         table.Name(),
		"comment":      table.Comment(),
		"input_schema": nil,
		"action_name":  nil,
		"description":  nil,
		"extra_data":   nil,
	}

	appMetadataBytes, err := msgpack.Encode(appMetadata)
	if err != nil {
		return nil, err
	}

	// Create FlightDescriptor
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{schema.Name(), table.Name()},
	}

	// Generate ticket for this table
	ticket, err := EncodeTicket(schema.Name(), table.Name())
	if err != nil {
		return nil, err
	}

	// Create FlightInfo with endpoint
	flightInfo := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(arrowSchema, s.allocator),
		FlightDescriptor: descriptor,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: ticket,
				},
			},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
		Ordered:      false,
		AppMetadata:  appMetadataBytes,
	}

	return flightInfo, nil
}
