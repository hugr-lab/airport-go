package airport

import (
	"errors"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/auth"
	"github.com/hugr-lab/airport-go/catalog"
)

// ServerConfig contains configuration for Airport Flight server.
type ServerConfig struct {
	// Catalog provides schemas, tables, and functions.
	// REQUIRED: MUST NOT be nil.
	Catalog catalog.Catalog

	// Auth provides authentication logic.
	// OPTIONAL: If nil, no authentication (all requests allowed).
	Auth auth.Authenticator

	// Allocator for Arrow memory management.
	// OPTIONAL: Uses memory.DefaultAllocator if nil.
	Allocator memory.Allocator

	// Logger for internal logging.
	// OPTIONAL: Uses slog.Default() if nil.
	// Note: If LogLevel is specified, a new logger will be created with that level.
	Logger *slog.Logger

	// LogLevel sets the logging level.
	// OPTIONAL: If nil, uses Info level.
	// Valid values: slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError
	// If Logger is also provided, LogLevel is ignored (use pre-configured logger).
	LogLevel *slog.Level

	// MaxMessageSize sets maximum gRPC message size in bytes.
	// OPTIONAL: If 0, uses gRPC default (4MB).
	// Recommended: 16MB for large Arrow batches.
	MaxMessageSize int

	// Address is the server's public address (e.g., "localhost:50051").
	// OPTIONAL: If empty, FlightEndpoint locations will not include URI.
	// Required for proper DoGet routing when DuckDB needs to reconnect.
	Address string
}

// Standard errors returned by airport package.
var (
	// ErrUnauthorized indicates authentication failed.
	// Return this from Authenticator.Authenticate() for invalid tokens.
	ErrUnauthorized = errors.New("unauthorized")

	// ErrInvalidConfig indicates ServerConfig validation failed.
	ErrInvalidConfig = errors.New("invalid server config")

	// ErrCatalogNotFound indicates catalog/schema/table lookup failed.
	ErrCatalogNotFound = errors.New("catalog entity not found")

	// ErrInvalidParameters indicates function parameters are invalid.
	ErrInvalidParameters = errors.New("invalid function parameters")
)
