package flight

import (
	"context"

	"github.com/hugr-lab/airport-go/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor creates a gRPC unary interceptor for authentication.
// Validates bearer tokens and propagates identity via context.
// If no authenticator is provided, requests pass through without auth.
func UnaryServerInterceptor(authenticator auth.Authenticator) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		ctx = EnrichContextMetadata(ctx)
		// If no authenticator, skip auth
		if authenticator == nil {
			return handler(ctx, req)
		}

		// Extract token from metadata
		token, err := auth.TokenFromAuthorizationHeader(
			AuthorizationFromContext(ctx),
		)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}

		ctx, err = auth.ValidateToken(ctx, token, authenticator)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}

		// Check for catalog-aware authorization
		if ca, ok := authenticator.(auth.CatalogAuthorizer); ok {
			catalogName := extractCatalogFromMetadata(ctx)
			ctx, err = ca.AuthorizeCatalog(ctx, catalogName)
			if err != nil {
				return nil, status.Errorf(codes.PermissionDenied, "catalog authorization failed: %v", err)
			}
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor creates a gRPC stream interceptor for authentication.
// Validates bearer tokens and propagates identity via context.
// If no authenticator is provided, requests pass through without auth.
func StreamServerInterceptor(authenticator auth.Authenticator) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := EnrichContextMetadata(ss.Context())
		// If no authenticator, skip auth
		// Wrap the stream with authenticated context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		if authenticator == nil {
			return handler(srv, wrappedStream)
		}

		// Extract token from metadata
		token, err := auth.TokenFromAuthorizationHeader(
			AuthorizationFromContext(ctx),
		)
		if err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}

		ctx, err = auth.ValidateToken(ctx, token, authenticator)
		if err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}

		// Check for catalog-aware authorization
		if ca, ok := authenticator.(auth.CatalogAuthorizer); ok {
			catalogName := extractCatalogFromMetadata(ctx)
			ctx, err = ca.AuthorizeCatalog(ctx, catalogName)
			if err != nil {
				return status.Errorf(codes.PermissionDenied, "catalog authorization failed: %v", err)
			}
		}
		wrappedStream.ctx = ctx

		return handler(srv, wrappedStream)
	}
}

// wrappedServerStream wraps grpc.ServerStream with a custom context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapper's custom context.
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// extractCatalogFromMetadata extracts the airport-catalog header from gRPC metadata.
// Returns empty string if header not present.
func extractCatalogFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("airport-catalog")
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
