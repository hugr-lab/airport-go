package flight

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetFlightInfo returns schema metadata and ticket for table queries.
// This RPC allows clients to discover table schemas before fetching data.
//
// The descriptor.Path should contain [schema_name, table_name].
// Returns FlightInfo with:
//   - Schema: Arrow schema for the table
//   - Ticket: Opaque byte slice encoding schema/table names
//   - Endpoints: Single endpoint with the ticket
func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.logger.Debug("GetFlightInfo called",
		"type", desc.GetType(),
		"path_length", len(desc.GetPath()),
	)

	// Validate descriptor
	if desc.GetType() != flight.DescriptorPATH {
		return nil, status.Error(codes.InvalidArgument, "descriptor must be PATH type")
	}

	path := desc.GetPath()
	if len(path) != 2 {
		return nil, status.Error(codes.InvalidArgument, "path must contain exactly 2 elements: [schema_name, table_name]")
	}

	schemaName := path[0]
	tableName := path[1]

	s.logger.Debug("GetFlightInfo request",
		"schema", schemaName,
		"table", tableName,
	)

	// Look up schema in catalog
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		s.logger.Error("Failed to get schema from catalog",
			"schema", schemaName,
			"error", err,
		)
		return nil, status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return nil, status.Errorf(codes.NotFound, "schema not found: %s", schemaName)
	}

	// Look up table in schema
	table, err := schema.Table(ctx, tableName)
	if err != nil {
		s.logger.Error("Failed to get table from schema",
			"schema", schemaName,
			"table", tableName,
			"error", err,
		)
		return nil, status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return nil, status.Errorf(codes.NotFound, "table not found: %s.%s", schemaName, tableName)
	}

	// Get Arrow schema from table
	arrowSchema := table.ArrowSchema()
	if arrowSchema == nil {
		s.logger.Error("Table returned nil Arrow schema",
			"schema", schemaName,
			"table", tableName,
		)
		return nil, status.Errorf(codes.Internal, "table %s.%s has nil Arrow schema", schemaName, tableName)
	}

	// Generate ticket
	ticket, err := EncodeTicket(schemaName, tableName)
	if err != nil {
		s.logger.Error("Failed to encode ticket",
			"schema", schemaName,
			"table", tableName,
			"error", err,
		)
		return nil, status.Errorf(codes.Internal, "failed to encode ticket: %v", err)
	}

	// Create flight info
	flightInfo := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(arrowSchema, s.allocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: ticket,
				},
			},
		},
		TotalRecords: -1, // Unknown until scan
		TotalBytes:   -1, // Unknown until scan
	}

	s.logger.Debug("GetFlightInfo successful",
		"schema", schemaName,
		"table", tableName,
		"num_fields", arrowSchema.NumFields(),
	)

	return flightInfo, nil
}
