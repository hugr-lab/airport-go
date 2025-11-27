// Package flight provides Flight RPC handler implementations.
package flight

import (
	"log/slog"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go/catalog"
)

// Server implements the Flight service handlers.
// Embeds BaseFlightServer for forward compatibility with protocol changes.
type Server struct {
	flight.BaseFlightServer

	catalog   catalog.Catalog
	allocator memory.Allocator
	logger    *slog.Logger
	address   string // Server's public address for FlightEndpoint locations
}

// NewServer creates a new Flight server with the given catalog and allocator.
// The logger is used for internal logging of errors and important events.
// The address parameter specifies the server's public address for FlightEndpoint locations.
func NewServer(cat catalog.Catalog, allocator memory.Allocator, logger *slog.Logger, address string) *Server {
	return &Server{
		catalog:   cat,
		allocator: allocator,
		logger:    logger,
		address:   address,
	}
}

// RegisterFlightServer registers the Flight service on the provided gRPC server.
// This follows the standard gRPC service registration pattern.
func RegisterFlightServer(grpcServer *grpc.Server, flightServer *Server) {
	flight.RegisterFlightServiceServer(grpcServer, flightServer)
}
