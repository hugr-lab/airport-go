package flight

import "errors"

// Error types for multi-catalog operations.

var (
	// ErrCatalogExists is returned when adding a catalog with a name that already exists.
	ErrCatalogExists = errors.New("catalog already exists")
	// ErrCatalogNotFound is returned when a requested catalog does not exist.
	ErrCatalogNotFound = errors.New("catalog not found")
	// ErrNilCatalog is returned when attempting to add a nil catalog.
	ErrNilCatalog = errors.New("catalog cannot be nil")
	// ErrNoCatalogs is returned when creating server with empty catalog list.
	ErrNoCatalogs = errors.New("at least one catalog is required")
)

// ErrDuplicateCatalog is returned during server creation if catalogs have duplicate names.
type ErrDuplicateCatalog struct {
	Name string
}

func (e ErrDuplicateCatalog) Error() string {
	if e.Name == "" {
		return "duplicate default catalog"
	}
	return "duplicate catalog name: " + e.Name
}
