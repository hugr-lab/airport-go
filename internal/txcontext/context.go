// Package txcontext provides transaction context utilities for Flight handlers.
// This package provides helpers for extracting and propagating transaction IDs
// through gRPC request contexts.
package txcontext

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// TransactionIDHeader is the gRPC metadata key for transaction ID.
const TransactionIDHeader = "x-transaction-id"

// txKey is the unexported context key for transaction ID.
type txKey struct{}

// WithTransactionID returns a new context with the transaction ID stored.
func WithTransactionID(ctx context.Context, txID string) context.Context {
	return context.WithValue(ctx, txKey{}, txID)
}

// TransactionIDFromContext retrieves the transaction ID if present.
// Returns ("", false) if no transaction ID is set.
func TransactionIDFromContext(ctx context.Context) (string, bool) {
	txID, ok := ctx.Value(txKey{}).(string)
	return txID, ok
}

// ExtractTransactionID extracts transaction ID from gRPC incoming metadata.
// Returns empty string if no transaction ID is present.
func ExtractTransactionID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	txIDs := md.Get(TransactionIDHeader)
	if len(txIDs) == 0 {
		return ""
	}

	return txIDs[0]
}

// ExtractAndStoreTransactionID extracts transaction ID from gRPC metadata
// and returns a new context with it stored. If no transaction ID is present,
// returns the original context unchanged.
func ExtractAndStoreTransactionID(ctx context.Context) context.Context {
	txID := ExtractTransactionID(ctx)
	if txID == "" {
		return ctx
	}
	return WithTransactionID(ctx, txID)
}
