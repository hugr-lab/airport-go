package airport

import (
	"fmt"
	"log/slog"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go/auth"
	"github.com/hugr-lab/airport-go/flight"
)

// NewServer registers Airport Flight service handlers on the provided gRPC server.
// This is the main entry point for the airport package.
//
// The function:
//  1. Validates the ServerConfig
//  2. Creates Flight service implementation
//  3. Registers it on grpcServer
//
// Returns error if config is invalid (e.g., nil Catalog).
// Does NOT start the gRPC server - user controls lifecycle via grpcServer.Serve().
//
// For authentication, use ServerOptions() to create a gRPC server with auth interceptors:
//
//	opts := airport.ServerOptions(airport.ServerConfig{
//	    Auth: airport.BearerAuth(validateToken),
//	})
//	grpcServer := grpc.NewServer(opts...)
//	err := airport.NewServer(grpcServer, config)
//
// Basic example without authentication:
//
//	grpcServer := grpc.NewServer()
//	err := airport.NewServer(grpcServer, airport.ServerConfig{
//	    Catalog: myCatalog,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	lis, _ := net.Listen("tcp", ":50051")
//	grpcServer.Serve(lis)
func NewServer(grpcServer *grpc.Server, config ServerConfig) error {
	// Validate configuration
	if err := validateConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	// Use defaults for optional fields
	allocator := config.Allocator
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Create Flight server
	flightServer := flight.NewServer(config.Catalog, allocator, logger, config.Address)

	// Register Flight service
	flight.RegisterFlightServer(grpcServer, flightServer)

	// Log successful registration
	logger.Info("Airport Flight server registered",
		"has_auth", config.Auth != nil,
		"max_message_size", config.MaxMessageSize,
	)

	return nil
}

// validateConfig checks that required ServerConfig fields are valid.
func validateConfig(config ServerConfig) error {
	if config.Catalog == nil {
		return fmt.Errorf("catalog is required")
	}
	return nil
}

// ServerOptions returns gRPC server options with authentication interceptors.
// Use this when creating a gRPC server if you want authentication enabled.
//
// Example:
//
//	config := airport.ServerConfig{
//	    Catalog: catalog,
//	    Auth: airport.BearerAuth(validateToken),
//	}
//	opts := airport.ServerOptions(config)
//	grpcServer := grpc.NewServer(opts...)
//	airport.NewServer(grpcServer, config)
func ServerOptions(config ServerConfig) []grpc.ServerOption {
	var opts []grpc.ServerOption

	// Add auth interceptors if authenticator is provided
	if config.Auth != nil {
		opts = append(opts,
			grpc.UnaryInterceptor(auth.UnaryServerInterceptor(config.Auth)),
			grpc.StreamInterceptor(auth.StreamServerInterceptor(config.Auth)),
		)
	}

	// Add max message size if specified
	if config.MaxMessageSize > 0 {
		opts = append(opts,
			grpc.MaxRecvMsgSize(config.MaxMessageSize),
			grpc.MaxSendMsgSize(config.MaxMessageSize),
		)
	}

	return opts
}
