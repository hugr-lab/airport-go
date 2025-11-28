package flight

import (
	"context"

	"github.com/hugr-lab/airport-go/internal/txcontext"
)

// withTransaction wraps a DML operation with automatic transaction commit/rollback.
// If no transaction ID is in context or no TransactionManager is configured,
// the operation executes without transaction coordination.
//
// On success: commits the transaction (if present)
// On failure: rolls back the transaction (if present)
func (s *Server) withTransaction(ctx context.Context, fn func(context.Context) error) error {
	// Extract and store transaction ID in context
	ctx = txcontext.ExtractAndStoreTransactionID(ctx)

	txID, _ := txcontext.TransactionIDFromContext(ctx)
	if txID == "" || s.txManager == nil {
		// No transaction - execute directly
		return fn(ctx)
	}

	// Execute with automatic commit/rollback
	err := fn(ctx)

	if err != nil {
		// Rollback on error (log but don't fail if rollback fails)
		if rbErr := s.txManager.RollbackTransaction(ctx, txID); rbErr != nil {
			s.logger.Error("transaction rollback failed",
				"tx_id", txID,
				"error", rbErr)
		}
		return err
	}

	// Commit on success
	if cmErr := s.txManager.CommitTransaction(ctx, txID); cmErr != nil {
		s.logger.Error("transaction commit failed",
			"tx_id", txID,
			"error", cmErr)
		return cmErr
	}

	return nil
}

// getTransactionContext extracts transaction ID from gRPC metadata and returns
// a context with the transaction ID stored for downstream use.
func (s *Server) getTransactionContext(ctx context.Context) context.Context {
	return txcontext.ExtractAndStoreTransactionID(ctx)
}
