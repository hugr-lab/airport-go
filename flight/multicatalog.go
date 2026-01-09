package flight

import (
	"context"
	"log/slog"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/catalog"
)

// MultiCatalogServer aggregates multiple flight.Server instances and routes
// requests based on the airport-catalog metadata header.
//
// Thread-safety: All methods are safe for concurrent use.
type MultiCatalogServer struct {
	flight.BaseFlightServer

	mu       sync.RWMutex
	servers  map[string]*Server         // catalog name -> server
	catalogs map[string]catalog.Catalog // catalog name -> catalog (for Catalogs() method)
	logger   *slog.Logger
}

// NewMultiCatalogServerInternal creates a new MultiCatalogServer with validation.
// This is the internal constructor used by the high-level NewMultiCatalogServer function.
// Returns error if:
//   - Duplicate catalog names
//   - Any catalog is nil
func NewMultiCatalogServerInternal(logger *slog.Logger, servers ...*Server) (*MultiCatalogServer, error) {

	mcs := &MultiCatalogServer{
		servers:  make(map[string]*Server, len(servers)),
		catalogs: make(map[string]catalog.Catalog, len(servers)),
		logger:   logger,
	}

	for _, srv := range servers {
		if srv == nil {
			return nil, ErrNilCatalog
		}
		name := srv.CatalogName()
		if _, exists := mcs.servers[name]; exists {
			return nil, ErrDuplicateCatalog{Name: name}
		}
		mcs.servers[name] = srv
		mcs.catalogs[name] = srv.Catalog()
	}

	return mcs, nil
}

// catalogServer returns the flight.Server for the given catalog name.
// Returns NotFound error if catalog doesn't exist.
func (m *MultiCatalogServer) catalogServer(catalogName string) (*Server, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	srv, exists := m.servers[catalogName]
	if !exists {
		return nil, status.Error(codes.NotFound, ErrCatalogNotFound.Error())
	}
	return srv, nil
}

// AddCatalog registers a new catalog at runtime.
// Creates internal flight.Server and adds to routing.
// Returns error if:
//   - catalog is nil
//   - catalog name already exists (including empty string for default)
func (m *MultiCatalogServer) AddCatalog(srv *Server) error {
	if srv == nil {
		return ErrNilCatalog
	}

	name := srv.CatalogName()

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.servers[name]; exists {
		return ErrCatalogExists
	}

	m.servers[name] = srv
	m.catalogs[name] = srv.Catalog()
	return nil
}

// RemoveCatalog unregisters a catalog by name.
// Returns error if catalog name does not exist.
// In-flight requests to the removed catalog complete normally.
func (m *MultiCatalogServer) RemoveCatalog(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.servers[name]; !exists {
		return ErrCatalogNotFound
	}

	delete(m.servers, name)
	delete(m.catalogs, name)
	return nil
}

// Catalogs returns the list of registered catalogs.
// The default catalog has an empty string name.
func (m *MultiCatalogServer) Catalogs() []catalog.Catalog {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]catalog.Catalog, 0, len(m.catalogs))
	for _, cat := range m.catalogs {
		result = append(result, cat)
	}
	return result
}

// CatalogExists checks if a catalog with the given name exists.
func (m *MultiCatalogServer) IsExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.servers[name]
	return exists
}

// Handshake implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	ctx := EnrichContextMetadata(stream.Context())

	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedHandshakeStream{
		FlightService_HandshakeServer: stream,

		ctx: ctx,
	}

	return srv.Handshake(wrappedStream)
}

// ListFlights implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	ctx := EnrichContextMetadata(stream.Context())

	catalog := CatalogNameFromContext(ctx)
	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedListFlightsStream{
		FlightService_ListFlightsServer: stream,

		ctx: ctx,
	}

	return srv.ListFlights(criteria, wrappedStream)
}

// GetFlightInfo implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) GetFlightInfo(ctx context.Context, descriptor *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ctx = EnrichContextMetadata(ctx)
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return nil, err
	}

	return srv.GetFlightInfo(ctx, descriptor)
}

// GetSchema implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	ctx = EnrichContextMetadata(ctx)
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return nil, err
	}

	return srv.GetSchema(ctx, descriptor)
}

// DoGet implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	ctx := EnrichContextMetadata(stream.Context())
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedDoGetStream{
		FlightService_DoGetServer: stream,

		ctx: ctx,
	}

	return srv.DoGet(ticket, wrappedStream)
}

// DoPut implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) DoPut(stream flight.FlightService_DoPutServer) error {
	ctx := EnrichContextMetadata(stream.Context())
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedDoPutStream{
		FlightService_DoPutServer: stream,

		ctx: ctx,
	}

	return srv.DoPut(wrappedStream)
}

// DoExchange implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	ctx := EnrichContextMetadata(stream.Context())
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedDoExchangeStream{
		FlightService_DoExchangeServer: stream,

		ctx: ctx,
	}

	return srv.DoExchange(wrappedStream)
}

// DoAction implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	ctx := EnrichContextMetadata(stream.Context())
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedDoActionStream{
		FlightService_DoActionServer: stream,

		ctx: ctx,
	}

	return srv.DoAction(action, wrappedStream)
}

// ListActions implements flight.FlightServer by delegating to the appropriate catalog server.
func (m *MultiCatalogServer) ListActions(empty *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	ctx := EnrichContextMetadata(stream.Context())
	catalog := CatalogNameFromContext(ctx)

	srv, err := m.catalogServer(catalog)
	if err != nil {
		return err
	}

	// Create wrapped stream with enriched context
	wrappedStream := &wrappedListActionsStream{
		FlightService_ListActionsServer: stream,

		ctx: ctx,
	}

	return srv.ListActions(empty, wrappedStream)
}

// Stream wrapper types for context propagation

type wrappedHandshakeStream struct {
	flight.FlightService_HandshakeServer
	ctx context.Context
}

func (w *wrappedHandshakeStream) Context() context.Context { return w.ctx }

type wrappedListFlightsStream struct {
	flight.FlightService_ListFlightsServer
	ctx context.Context
}

func (w *wrappedListFlightsStream) Context() context.Context { return w.ctx }

type wrappedDoGetStream struct {
	flight.FlightService_DoGetServer
	ctx context.Context
}

func (w *wrappedDoGetStream) Context() context.Context { return w.ctx }

type wrappedDoPutStream struct {
	flight.FlightService_DoPutServer
	ctx context.Context
}

func (w *wrappedDoPutStream) Context() context.Context { return w.ctx }

type wrappedDoExchangeStream struct {
	flight.FlightService_DoExchangeServer
	ctx context.Context
}

func (w *wrappedDoExchangeStream) Context() context.Context { return w.ctx }

type wrappedDoActionStream struct {
	flight.FlightService_DoActionServer
	ctx context.Context
}

func (w *wrappedDoActionStream) Context() context.Context { return w.ctx }

type wrappedListActionsStream struct {
	flight.FlightService_ListActionsServer
	ctx context.Context
}

func (w *wrappedListActionsStream) Context() context.Context { return w.ctx }
