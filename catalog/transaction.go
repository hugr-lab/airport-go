package catalog

import (
	"context"

	"github.com/hugr-lab/airport-go/internal/txcontext"
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

// TransactionIDFromContext retrieves the transaction ID if present.
// Returns ("", false) if no transaction ID is set.
func TransactionIDFromContext(ctx context.Context) (string, bool) {
	return txcontext.TransactionIDFromContext(ctx)
}
