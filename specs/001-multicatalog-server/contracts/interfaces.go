// Package contracts defines the API contracts for the multi-catalog server feature.
// This file is auto-generated during planning and serves as the implementation target.
// DO NOT EDIT manually - regenerate via /speckit.plan if changes needed.
package contracts

import (
	"context"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/airport-go/auth"
	"github.com/hugr-lab/airport-go/catalog"
	"google.golang.org/grpc"
)

// MultiCatalogServerConfig holds configuration for creating a multi-catalog server.
// Similar to ServerConfig but accepts multiple catalogs.
type MultiCatalogServerConfig struct {
	// Catalogs is the list of catalogs to serve. Required, must have at least one.
	// Each catalog must implement catalog.NamedCatalog for routing.
	// Catalog names must be unique (including empty string for default).
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
	TransactionManager CatalogTransactionManager

	// Auth is the authenticator for validating requests. Optional.
	// If the authenticator also implements CatalogAuthorizer, AuthorizeCatalog
	// is called after Authenticate to perform per-catalog authorization.
	Auth auth.Authenticator

	// MaxMessageSize is the maximum gRPC message size. Optional.
	MaxMessageSize int
}

// NewMultiCatalogServer creates and registers a multi-catalog Flight server.
// The function:
//   - Validates configuration (unique catalog names, at least one catalog)
//   - Creates internal flight.Server for each catalog
//   - Creates the dispatch MultiCatalogServer
//   - Registers with the provided gRPC server
//
// Returns error if configuration is invalid.
//
// Example:
//
//	err := airport.NewMultiCatalogServer(grpcServer, airport.MultiCatalogServerConfig{
//	    Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
//	    Logger:   slog.Default(),
//	})
func NewMultiCatalogServer(grpcServer *grpc.Server, config MultiCatalogServerConfig) error {
	// Implementation creates flight.Server per catalog and registers dispatcher
	panic("not implemented - this is a contract definition")
}

// MultiCatalogServer aggregates multiple flight.Server instances and routes
// requests based on the airport-catalog metadata header.
//
// Thread-safety: All methods are safe for concurrent use.
type MultiCatalogServer interface {
	flight.FlightServer

	// AddCatalog registers a new catalog at runtime.
	// Creates internal flight.Server and adds to routing.
	// Returns error if:
	//   - catalog is nil
	//   - catalog name already exists (including empty string for default)
	AddCatalog(cat catalog.Catalog) error

	// RemoveCatalog unregisters a catalog by name.
	// Returns error if catalog name does not exist.
	// In-flight requests to the removed catalog complete normally.
	RemoveCatalog(name string) error

	// Catalogs returns the list of registered catalogs.
	// The default catalog has an empty string name.
	Catalogs() []catalog.Catalog
}

// CatalogTransactionManager extends TransactionManager with catalog context.
// Transactions are scoped to a specific catalog.
type CatalogTransactionManager interface {
	// BeginTransaction creates a new transaction in the specified catalog.
	// The transaction ID should be globally unique (UUID recommended).
	// The catalog name is stored with the transaction for routing.
	BeginTransaction(ctx context.Context, catalogName string) (txID string, err error)

	// CommitTransaction commits a transaction.
	// The implementation must know which catalog the transaction belongs to.
	CommitTransaction(ctx context.Context, txID string) error

	// RollbackTransaction aborts a transaction.
	// The implementation must know which catalog the transaction belongs to.
	RollbackTransaction(ctx context.Context, txID string) error

	// GetTransactionStatus returns the state and catalog of a transaction.
	// Returns (state, catalogName, true) if transaction exists.
	// Returns ("", "", false) if transaction does not exist.
	GetTransactionStatus(ctx context.Context, txID string) (state catalog.TransactionState, catalogName string, exists bool)
}

// CatalogAuthorizer is an optional interface that Authenticator implementations
// can also implement to provide per-catalog authorization.
//
// When an Authenticator also implements CatalogAuthorizer:
//  1. Authenticate(ctx, token) is called first to validate the token
//  2. AuthorizeCatalog(ctx, catalog, token) is called to authorize catalog access
//
// This allows separating authentication (who are you?) from authorization
// (can you access this catalog?).
type CatalogAuthorizer interface {
	// AuthorizeCatalog authorizes access to a specific catalog.
	// Called after successful Authenticate() to check catalog-level permissions.
	// Parameters:
	//   - ctx: Request context with identity already set from Authenticate()
	//   - catalog: Target catalog name (empty string for default)
	//   - token: Bearer token (same as passed to Authenticate)
	// Returns:
	//   - ctx: Potentially enriched context (e.g., with catalog-specific claims)
	//   - err: Non-nil if authorization fails (returns gRPC PermissionDenied status)
	AuthorizeCatalog(ctx context.Context, catalog string, token string) (context.Context, error)
}

// RequestMetadata contains extracted metadata from incoming requests.
type RequestMetadata struct {
	// CatalogName is the target catalog from airport-catalog header.
	// Empty string if header not present (routes to default catalog).
	CatalogName string

	// TraceID is the distributed trace identifier from airport-trace-id header.
	// Empty string if header not present.
	TraceID string

	// SessionID is the client session identifier from airport-client-session-id header.
	// Empty string if header not present.
	SessionID string
}

// Context key types and helper functions for request metadata.
// These follow the pattern established in the auth package.

// ContextKey is an unexported type for context keys to prevent collisions.
type ContextKey int

const (
	// TraceIDKey is the context key for trace ID.
	TraceIDKey ContextKey = iota
	// SessionIDKey is the context key for session ID.
	SessionIDKey
	// CatalogNameKey is the context key for catalog name.
	CatalogNameKey
)

// WithTraceID returns a context with the trace ID set.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// TraceIDFromContext returns the trace ID from context, or empty string if not set.
func TraceIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(TraceIDKey).(string)
	return id
}

// WithSessionID returns a context with the session ID set.
func WithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, SessionIDKey, sessionID)
}

// SessionIDFromContext returns the session ID from context, or empty string if not set.
func SessionIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(SessionIDKey).(string)
	return id
}

// WithCatalogName returns a context with the catalog name set.
func WithCatalogName(ctx context.Context, catalogName string) context.Context {
	return context.WithValue(ctx, CatalogNameKey, catalogName)
}

// CatalogNameFromContext returns the catalog name from context, or empty string if not set.
func CatalogNameFromContext(ctx context.Context) string {
	name, _ := ctx.Value(CatalogNameKey).(string)
	return name
}

// Error types for multi-catalog operations.

// ErrCatalogNotFound is returned when a requested catalog does not exist.
type ErrCatalogNotFound struct {
	Name string
}

func (e ErrCatalogNotFound) Error() string {
	if e.Name == "" {
		return "default catalog not found"
	}
	return "catalog not found: " + e.Name
}

// ErrCatalogExists is returned when adding a catalog with a name that already exists.
type ErrCatalogExists struct {
	Name string
}

func (e ErrCatalogExists) Error() string {
	if e.Name == "" {
		return "default catalog already exists"
	}
	return "catalog already exists: " + e.Name
}

// ErrNilCatalog is returned when attempting to add a nil catalog.
type ErrNilCatalog struct{}

func (e ErrNilCatalog) Error() string {
	return "catalog cannot be nil"
}

// ErrDuplicateCatalog is returned during server creation if catalogs have duplicate names.
type ErrDuplicateCatalog struct {
	Name string
}

func (e ErrDuplicateCatalog) Error() string {
	if e.Name == "" {
		return "duplicate default catalog"
	}
	return "duplicate catalog name: " + e.Name
}

// ErrNoCatalogs is returned when creating server with empty catalog list.
type ErrNoCatalogs struct{}

func (e ErrNoCatalogs) Error() string {
	return "at least one catalog is required"
}
