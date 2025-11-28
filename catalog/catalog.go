// Package catalog provides interfaces for defining Flight server catalogs, schemas, and tables.
//
// The catalog package follows an interface-based design to support both static and dynamic implementations:
//   - Static catalogs: Built using NewCatalogBuilder() fluent API (immutable, fast lookup)
//   - Dynamic catalogs: Custom implementations that can reflect live database state
//
// All interfaces are goroutine-safe and support context-based cancellation.
package catalog

import (
	"context"
)

// Catalog represents the top-level metadata container.
// Implementations can be static (from builder) or dynamic (user-provided).
// All methods MUST be goroutine-safe.
type Catalog interface {
	// Schemas returns all schemas visible in this catalog.
	// Context may contain auth info for permission-based filtering.
	// Returns empty slice (not nil) if no schemas available.
	// MUST respect context cancellation and deadlines.
	Schemas(ctx context.Context) ([]Schema, error)

	// Schema returns a specific schema by name.
	// Returns (nil, nil) if schema doesn't exist (not an error).
	// Returns (nil, err) if lookup fails for other reasons.
	Schema(ctx context.Context, name string) (Schema, error)
}

// Schema represents a database schema containing tables and functions.
// Implementations MUST be goroutine-safe.
type Schema interface {
	// Name returns the schema name (e.g., "main", "information_schema").
	// MUST return non-empty string.
	Name() string

	// Comment returns optional schema documentation.
	// Returns empty string if no comment provided.
	Comment() string

	// Tables returns all tables in this schema.
	// Context may contain auth info for permission filtering.
	// Returns empty slice (not nil) if no tables available.
	// MUST respect context cancellation.
	Tables(ctx context.Context) ([]Table, error)

	// Table returns a specific table by name.
	// Returns (nil, nil) if table doesn't exist (not an error).
	// Returns (nil, err) if lookup fails for other reasons.
	Table(ctx context.Context, name string) (Table, error)

	// ScalarFunctions returns all scalar functions in this schema.
	// Returns empty slice (not nil) if no functions available.
	// MUST respect context cancellation.
	ScalarFunctions(ctx context.Context) ([]ScalarFunction, error)

	// TableFunctions returns all table-valued functions in this schema.
	// Returns empty slice (not nil) if no table functions available.
	// MUST respect context cancellation.
	TableFunctions(ctx context.Context) ([]TableFunction, error)

	// TableFunctionsInOut returns all table functions that accept row sets as input.
	// Returns empty slice (not nil) if no in-out table functions available.
	// MUST respect context cancellation.
	TableFunctionsInOut(ctx context.Context) ([]TableFunctionInOut, error)
}
