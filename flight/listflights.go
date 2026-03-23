package flight

import (
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/internal/serialize"
)

// ListFlights returns available catalog metadata.
// This RPC allows clients to discover schemas and tables without executing queries.
//
// The response contains:
//   - FlightInfo with catalog metadata serialized as Arrow IPC
//   - ZStandard-compressed payload for efficient network transfer
//   - Flight SQL standard schema format (GetTables)
//
// Criteria parameter is currently ignored (returns all tables).
func (s *Server) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	ctx := EnrichContextMetadata(stream.Context())

	s.logger.Debug("ListFlights called")

	// Serialize catalog to Arrow IPC format following Flight SQL schema
	catalogData, err := serialize.SerializeCatalog(ctx, s.catalog, s.allocator)
	if err != nil {
		s.logger.Error("Failed to serialize catalog", "error", err)
		return status.Errorf(codes.Internal, "failed to serialize catalog: %v", err)
	}

	s.logger.Debug("Catalog serialized",
		"uncompressed_bytes", len(catalogData),
	)

	// Compress with ZStandard for efficient network transfer
	compressed, err := serialize.CompressCatalog(catalogData)
	if err != nil {
		s.logger.Error("Failed to compress catalog", "error", err)
		return status.Errorf(codes.Internal, "failed to compress catalog: %v", err)
	}

	compressionRatio := float64(len(catalogData)) / float64(len(compressed))
	s.logger.Debug("Catalog compressed",
		"uncompressed_bytes", len(catalogData),
		"compressed_bytes", len(compressed),
		"compression_ratio", compressionRatio,
	)

	// Create FlightInfo with compressed catalog data
	// The descriptor path indicates this is a catalog listing
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("ListFlights"),
	}

	flightInfo := &flight.FlightInfo{
		FlightDescriptor: descriptor,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: compressed,
				},
			},
		},
		TotalRecords: -1, // Unknown - catalog size varies
		TotalBytes:   int64(len(compressed)),
	}

	// Send the single FlightInfo response
	if err := stream.Send(flightInfo); err != nil {
		s.logger.Error("Failed to send FlightInfo", "error", err)
		return status.Errorf(codes.Internal, "failed to send flight info: %v", err)
	}

	s.logger.Debug("ListFlights completed successfully",
		"compressed_bytes", len(compressed),
	)

	return nil
}
