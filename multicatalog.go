package airport

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go/auth"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/flight"
)

// MultiCatalogServer wraps flight.MultiCatalogServer to provide
// methods for adding and removing catalogs at runtime.
type MultiCatalogServer struct {
	server *flight.MultiCatalogServer
	config MultiCatalogServerConfig

	grpc *grpc.Server
}

// AddCatalog adds a new catalog to the multi-catalog server at runtime.
// Returns error if a catalog with the same name already exists.
func (s *MultiCatalogServer) AddCatalog(cat catalog.Catalog) error {
	server := newServerForCatalog(cat, s.config)
	return s.server.AddCatalog(server)
}

// RemoveCatalog removes a catalog by name from the multi-catalog server at runtime.
func (s *MultiCatalogServer) RemoveCatalog(name string) error {
	return s.server.RemoveCatalog(name)
}

// IsExists checks if a catalog with the given name exists.
func (s *MultiCatalogServer) IsExists(name string) bool {
	return s.server.IsExists(name)
}

// MultiCatalogServerConfig holds configuration for creating a multi-catalog server.
// Similar to ServerConfig but accepts multiple catalogs.
type MultiCatalogServerConfig struct {
	// Catalogs is the list of initial catalogs to serve. Optional (can be empty).
	// Each catalog must implement catalog.NamedCatalog for routing.
	// Catalog names must be unique (including empty string for default).
	// Additional catalogs can be added at runtime via AddCatalog().
	Catalogs []catalog.Catalog

	// Allocator for Arrow memory management. Optional, defaults to DefaultAllocator.
	Allocator memory.Allocator

	// Logger for server events. Optional, defaults to slog with Info level.
	Logger *slog.Logger

	// LogLevel sets the minimum log level. Optional, defaults to Info.
	// Only used if Logger is nil.
	LogLevel *slog.Level

	// Address is the server's public address for FlightEndpoint locations.
	// Optional, defaults to reuse connection.
	Address string

	// TransactionManager coordinates transactions across catalogs. Optional.
	// Must implement CatalogTransactionManager for multi-catalog support.
	TransactionManager catalog.CatalogTransactionManager

	// Auth is the authenticator for validating requests. Optional.
	// If the authenticator also implements CatalogAuthorizer, AuthorizeCatalog
	// is called after Authenticate to perform per-catalog authorization.
	Auth auth.Authenticator

	// MaxMessageSize is the maximum gRPC message size. Optional.
	MaxMessageSize int
}

// NewMultiCatalogServer creates and registers a multi-catalog Flight server.
// The function:
//   - Validates configuration (unique catalog names, no nil catalogs)
//   - Creates internal flight.Server for each catalog
//   - Creates the dispatch MultiCatalogServer
//   - Registers with the provided gRPC server
//
// Returns error if configuration is invalid.
// Note: Empty catalogs list is allowed; catalogs can be added at runtime via AddCatalog().
//
// Example:
//
//	mcs, err := airport.NewMultiCatalogServer(grpcServer, airport.MultiCatalogServerConfig{
//	    Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
//	    Logger:   slog.Default(),
//	})
func NewMultiCatalogServer(grpcServer *grpc.Server, config MultiCatalogServerConfig) (*MultiCatalogServer, error) {
	// Validate configuration
	if err := validateMultiCatalogConfig(config); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	// Use defaults for optional fields
	allocator := config.Allocator
	if allocator == nil {
		config.Allocator = memory.DefaultAllocator
	}

	logger := config.Logger
	if logger == nil {
		// Create logger with specified level or default to Info
		level := slog.LevelInfo
		if config.LogLevel != nil {
			level = *config.LogLevel
		}
		handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
		})
		config.Logger = slog.New(handler)
	}

	// Create flight.Server for each catalog
	servers := make([]*flight.Server, len(config.Catalogs))
	for i, cat := range config.Catalogs {
		servers[i] = newServerForCatalog(cat, config)
	}

	// Create MultiCatalogServer
	mcs, err := flight.NewMultiCatalogServerInternal(config.Logger, servers...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	// Register with gRPC server
	flight.RegisterFlightServer(grpcServer, mcs)

	// Log successful registration
	config.Logger.Info("Airport Multi-Catalog Flight server registered",
		"num_catalogs", len(config.Catalogs),
		"has_auth", config.Auth != nil,
		"has_tx_manager", config.TransactionManager != nil,
		"max_message_size", config.MaxMessageSize,
	)

	return &MultiCatalogServer{
		server: mcs,
		config: config,
		grpc:   grpcServer,
	}, nil
}

// validateMultiCatalogConfig checks that required MultiCatalogServerConfig fields are valid.
func validateMultiCatalogConfig(config MultiCatalogServerConfig) error {
	if len(config.Catalogs) == 0 {
		return nil
	}

	// Check for nil catalogs and duplicate names
	seen := make(map[string]bool, len(config.Catalogs))
	for _, cat := range config.Catalogs {
		if cat == nil {
			return flight.ErrNilCatalog
		}
		name := getCatalogName(cat)
		if seen[name] {
			return flight.ErrDuplicateCatalog{Name: name}
		}
		seen[name] = true
	}

	return nil
}

// newServerForCatalog creates a flight.Server for the given catalog and configuration.
// If TransactionManager is set in config, wraps it with an adapter for catalog context.
func newServerForCatalog(cat catalog.Catalog, config MultiCatalogServerConfig) *flight.Server {
	if config.TransactionManager != nil {
		// Create adapter that implements catalog.TransactionManager
		adapter := &catalogTxManagerAdapter{
			ctm:         config.TransactionManager,
			catalogName: getCatalogName(cat),
		}
		return flight.NewServerWithTxManager(cat, config.Allocator, config.Logger, config.Address, adapter)
	}

	return flight.NewServer(cat, config.Allocator, config.Logger, config.Address)
}

// getCatalogName returns the name of a catalog if it implements NamedCatalog.
func getCatalogName(cat catalog.Catalog) string {
	if named, ok := cat.(catalog.NamedCatalog); ok {
		return named.Name()
	}
	return ""
}

// MultiCatalogServerOptions returns gRPC server options with authentication interceptors
// configured for multi-catalog support.
// Use this when creating a gRPC server if you want authentication enabled.
//
// Example:
//
//	config := airport.MultiCatalogServerConfig{
//	    Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
//	    Auth: myCatalogAwareAuth,
//	}
//	opts := airport.MultiCatalogServerOptions(config)
//	grpcServer := grpc.NewServer(opts...)
//	airport.NewMultiCatalogServer(grpcServer, config)
func MultiCatalogServerOptions(config MultiCatalogServerConfig) []grpc.ServerOption {
	var opts []grpc.ServerOption

	// Add auth interceptors
	opts = append(opts,
		grpc.UnaryInterceptor(flight.UnaryServerInterceptor(config.Auth)),
		grpc.StreamInterceptor(flight.StreamServerInterceptor(config.Auth)),
	)
	// Add max message size if specified
	if config.MaxMessageSize > 0 {
		opts = append(opts,
			grpc.MaxRecvMsgSize(config.MaxMessageSize),
			grpc.MaxSendMsgSize(config.MaxMessageSize),
		)
	}

	return opts
}

// catalogTxManagerAdapter adapts CatalogTransactionManager to catalog.TransactionManager
// for use with individual flight.Server instances.
type catalogTxManagerAdapter struct {
	ctm         catalog.CatalogTransactionManager
	catalogName string
}

func (a *catalogTxManagerAdapter) BeginTransaction(ctx context.Context) (string, error) {
	return a.ctm.BeginTransaction(ctx, a.catalogName)
}

func (a *catalogTxManagerAdapter) CommitTransaction(ctx context.Context, txID string) error {
	return a.ctm.CommitTransaction(ctx, txID)
}

func (a *catalogTxManagerAdapter) RollbackTransaction(ctx context.Context, txID string) error {
	return a.ctm.RollbackTransaction(ctx, txID)
}

func (a *catalogTxManagerAdapter) GetTransactionStatus(ctx context.Context, txID string) (catalog.TransactionState, bool) {
	state, _, exists := a.ctm.GetTransactionStatus(ctx, txID)
	return state, exists
}
