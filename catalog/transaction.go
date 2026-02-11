package catalog

import (
	"context"
)

// TransactionState represents the lifecycle stage of a transaction.
type TransactionState string

const (
	// TransactionActive indicates an open transaction awaiting operations.
	TransactionActive TransactionState = "active"

	// TransactionCommitted indicates a successfully completed transaction.
	TransactionCommitted TransactionState = "committed"

	// TransactionAborted indicates a rolled-back transaction.
	TransactionAborted TransactionState = "aborted"
)

// TransactionManager coordinates transactions across operations.
// This interface is OPTIONAL - servers operate normally without it.
// Implementations handle persistence and transaction state management.
//
// Usage:
//   - Client calls create_transaction action to get a transaction ID
//   - Client includes transaction ID in x-transaction-id header for DML operations
//   - Server automatically commits on success, rolls back on failure
//
// Implementations MUST be goroutine-safe.
type TransactionManager interface {
	// BeginTransaction creates a new transaction and returns its unique ID.
	// The ID should be globally unique (UUID recommended).
	// Returns error if transaction creation fails.
	BeginTransaction(ctx context.Context) (txID string, err error)

	// CommitTransaction marks a transaction as successfully completed.
	// Called automatically by Flight handlers on operation success.
	// Idempotent - safe to call multiple times with same txID.
	// Returns error if commit fails or txID is invalid.
	CommitTransaction(ctx context.Context, txID string) error

	// RollbackTransaction aborts a transaction.
	// Called automatically by Flight handlers on operation failure.
	// Idempotent - safe to call multiple times with same txID.
	// Returns error only for infrastructure failures (not "already rolled back").
	RollbackTransaction(ctx context.Context, txID string) error

	// GetTransactionStatus returns the current state of a transaction.
	// Returns (state, true) if transaction exists, ("", false) otherwise.
	// Used by handlers to validate transaction state before operations.
	GetTransactionStatus(ctx context.Context, txID string) (TransactionState, bool)
}

// CatalogTransactionManager extends TransactionManager with catalog context.
// Transactions are scoped to a specific catalog, enabling correct routing of
// commit/rollback operations in multi-catalog deployments.
//
// Implementations MUST be goroutine-safe.
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
	GetTransactionStatus(ctx context.Context, txID string) (state TransactionState, catalogName string, exists bool)
}

// contextKey is a private type for context keys to avoid collisions.
type contextKey int

const transactionIDKey contextKey = iota

// WithTransactionID returns a new context with the transaction ID set.
func WithTransactionID(ctx context.Context, txID string) context.Context {
	return context.WithValue(ctx, transactionIDKey, txID)
}

// TransactionIDFromContext retrieves the transaction ID if present.
// Returns ("", false) if no transaction ID is set.
func TransactionIDFromContext(ctx context.Context) (string, bool) {
	txId, ok := ctx.Value(transactionIDKey).(string)
	if !ok {
		return "", false
	}
	return txId, true
}
