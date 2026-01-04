// Package flight provides Flight RPC handler implementations.
package flight

import (
	"log/slog"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	address   string                     // Server's public address for FlightEndpoint locations
	txManager catalog.TransactionManager // Optional transaction coordinator
}

// NewServer creates a new Flight server with the given catalog and allocator.
// The logger is used for internal logging of errors and important events.
// The address parameter specifies the server's public address for FlightEndpoint locations.
func NewServer(cat catalog.Catalog, allocator memory.Allocator, logger *slog.Logger, address string) *Server {
	switch {
	case address == "":
		address = flight.LocationReuseConnection
	case !strings.HasPrefix(address, "grpc://") && !strings.HasPrefix(address, "grpc+tls://"):
		address = "grpc://" + address
	}
	return &Server{
		catalog:   cat,
		allocator: allocator,
		logger:    logger,
		address:   address,
	}
}

// NewServerWithTxManager creates a new Flight server with transaction management support.
// The txManager parameter is optional - if nil, operations execute without transaction coordination.
func NewServerWithTxManager(cat catalog.Catalog, allocator memory.Allocator, logger *slog.Logger, address string, txManager catalog.TransactionManager) *Server {
	switch {
	case address == "":
		address = flight.LocationReuseConnection
	case !strings.HasPrefix(address, "grpc://") && !strings.HasPrefix(address, "grpc+tls://"):
		address = "grpc://" + address
	}
	return &Server{
		catalog:   cat,
		allocator: allocator,
		logger:    logger,
		address:   address,
		txManager: txManager,
	}
}

// SetTransactionManager sets the transaction manager for the server.
// This allows adding transaction support after server creation.
// Can be set to nil to disable transaction coordination.
func (s *Server) SetTransactionManager(txManager catalog.TransactionManager) {
	s.txManager = txManager
}

// RegisterFlightServer registers the Flight service on the provided gRPC server.
// This follows the standard gRPC service registration pattern.
func RegisterFlightServer(grpcServer *grpc.Server, flightServer *Server) {
	flight.RegisterFlightServiceServer(grpcServer, flightServer)
}

func (s *Server) CatalogName() string {
	if namedCat, ok := s.catalog.(catalog.NamedCatalog); ok {
		return namedCat.Name()
	}
	return ""
}
