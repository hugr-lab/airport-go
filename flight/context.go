package flight

import (
	"context"

	"github.com/hugr-lab/airport-go/catalog"
	"google.golang.org/grpc/metadata"
)

// contextKey is a private type for context keys to avoid collisions.
type contextKey int

const (
	airportParamsKey contextKey = iota
)

// Metadata header keys for multi-catalog routing and observability.
const (
	// HeaderAuthorization is the gRPC metadata header for authorization token.
	HeaderAuthorization = "authorization"
	// HeaderCatalog is the gRPC metadata header for specifying target catalog.
	HeaderCatalog = "airport-catalog"
	// HeaderTraceID is the gRPC metadata header for distributed trace identifier.
	HeaderTraceID = "airport-trace-id"
	// HeaderSessionID is the gRPC metadata header for client session identifier.
	HeaderSessionID = "airport-client-session-id"
	// TransactionIDHeader is the gRPC metadata key for transaction ID.
	TransactionIDHeader = "airport-transaction-id"
)

type ContextMeta struct {
	Authorization string
	TraceID       string
	SessionID     string
	CatalogName   string
}

func WithContextMeta(ctx context.Context, meta ContextMeta) context.Context {
	return context.WithValue(ctx, airportParamsKey, &meta)
}

func MetaFromContext(ctx context.Context) *ContextMeta {
	val := ctx.Value(airportParamsKey)
	if val == nil {
		return nil
	}
	params, ok := val.(*ContextMeta)
	if !ok {
		return nil
	}
	return params
}

// AuthorizationFromContext retrieves the authorization header from context.
// Returns empty string if not set.
func AuthorizationFromContext(ctx context.Context) string {
	meta := MetaFromContext(ctx)
	if meta == nil {
		return ""
	}
	return meta.Authorization
}

// TraceIDFromContext returns the trace ID from context, or empty string if not set.
func TraceIDFromContext(ctx context.Context) string {
	meta := MetaFromContext(ctx)
	if meta == nil {
		return ""
	}
	return meta.TraceID
}

// SessionIDFromContext returns the session ID from context, or empty string if not set.
func SessionIDFromContext(ctx context.Context) string {
	meta := MetaFromContext(ctx)
	if meta == nil {
		return ""
	}
	return meta.SessionID
}

// CatalogNameFromContext returns the catalog name from context, or empty string if not set.
func CatalogNameFromContext(ctx context.Context) string {
	meta := MetaFromContext(ctx)
	if meta == nil {
		return ""
	}
	return meta.CatalogName
}

// EnrichContextMetadata extracts metadata from gRPC context and
// returns a new context with the metadata stored.
// If the context is already enriched, it is returned unchanged.
func EnrichContextMetadata(ctx context.Context) context.Context {
	if MetaFromContext(ctx) != nil {
		// Already enriched
		return ctx
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	var meta ContextMeta
	if values := md.Get(HeaderAuthorization); len(values) != 0 {
		meta.Authorization = values[0]
	}
	if values := md.Get(HeaderCatalog); len(values) > 0 {
		meta.CatalogName = values[0]
	}
	if values := md.Get(HeaderTraceID); len(values) > 0 {
		meta.TraceID = values[0]
	}
	if values := md.Get(HeaderSessionID); len(values) > 0 {
		meta.SessionID = values[0]
	}
	if values := md.Get(TransactionIDHeader); len(values) > 0 && values[0] != "" {
		ctx = catalog.WithTransactionID(ctx, values[0])
	}

	return WithContextMeta(ctx, meta)
}
