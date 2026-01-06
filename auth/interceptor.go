package auth

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor creates a gRPC unary interceptor for authentication.
// Validates bearer tokens and propagates identity via context.
// If no authenticator is provided, requests pass through without auth.
func UnaryServerInterceptor(authenticator Authenticator) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// If no authenticator, skip auth
		if authenticator == nil {
			return handler(ctx, req)
		}

		// Extract token from metadata
		token, err := ExtractToken(ctx)
		if err != nil {
			return nil, err
		}

		ctx, err = ValidateToken(ctx, token, authenticator)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor creates a gRPC stream interceptor for authentication.
// Validates bearer tokens and propagates identity via context.
// If no authenticator is provided, requests pass through without auth.
func StreamServerInterceptor(authenticator Authenticator) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// If no authenticator, skip auth
		if authenticator == nil {
			return handler(srv, ss)
		}

		ctx := ss.Context()

		// Extract token from metadata
		token, err := ExtractToken(ctx)
		if err != nil {
			return err
		}

		ctx, err = ValidateToken(ctx, token, authenticator)
		if err != nil {
			return err
		}
		// Wrap the stream with authenticated context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

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
