// Package contracts defines the new interface signatures for batch table operations.
// This file is for documentation purposes - actual implementation is in catalog/table.go.
package contracts

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/hugr-lab/airport-go/catalog"
)

// UpdatableBatchTable defines the new Update signature with arrow.RecordBatch.
// This replaces the previous signature that used array.RecordReader.
type UpdatableBatchTable interface {
	catalog.Table

	// Update modifies existing rows using data from the RecordBatch.
	// The rows RecordBatch contains both the rowid column and new column values.
	// Implementations MUST return an error if any rowid value is null.
	// Caller MUST call rows.Release() after Update returns.
	Update(ctx context.Context, rows arrow.RecordBatch, opts *catalog.DMLOptions) (*catalog.DMLResult, error)
}

// DeletableBatchTable defines the new Delete signature with arrow.RecordBatch.
// This replaces the previous signature that used array.RecordReader.
type DeletableBatchTable interface {
	catalog.Table

	// Delete removes rows identified by rowid values in the RecordBatch.
	// Implementations MUST return an error if any rowid value is null.
	// Caller MUST call rows.Release() after Delete returns.
	Delete(ctx context.Context, rows arrow.RecordBatch, opts *catalog.DMLOptions) (*catalog.DMLResult, error)
}
