package catalog

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
)

// Sentinel errors for DDL operations.
var (
	// ErrAlreadyExists is returned when creating an object that already exists.
	ErrAlreadyExists = errors.New("already exists")

	// ErrNotFound is returned when an object doesn't exist.
	ErrNotFound = errors.New("not found")

	// ErrSchemaNotEmpty is returned when dropping a schema that contains tables.
	ErrSchemaNotEmpty = errors.New("schema contains tables")
)

// OnConflict specifies behavior when an object already exists.
type OnConflict string

const (
	// OnConflictError returns an error if the object exists.
	OnConflictError OnConflict = "error"

	// OnConflictIgnore silently succeeds if the object exists.
	OnConflictIgnore OnConflict = "ignore"

	// OnConflictReplace drops and recreates the object.
	OnConflictReplace OnConflict = "replace"
)

// CreateSchemaOptions configures schema creation behavior.
type CreateSchemaOptions struct {
	// Comment is optional documentation for the schema.
	Comment string

	// Tags are optional key-value metadata pairs.
	Tags map[string]string
}

// DropSchemaOptions configures schema deletion behavior.
type DropSchemaOptions struct {
	// IgnoreNotFound suppresses error if schema doesn't exist.
	IgnoreNotFound bool
}

// CreateTableOptions configures table creation behavior.
type CreateTableOptions struct {
	// OnConflict specifies behavior when table already exists.
	// Default is OnConflictError.
	OnConflict OnConflict

	// Comment is optional documentation for the table.
	Comment string

	// NotNullConstraints lists column indices that cannot be null.
	NotNullConstraints []uint64

	// UniqueConstraints lists column indices that must be unique.
	UniqueConstraints []uint64

	// CheckConstraints lists SQL check constraint expressions.
	CheckConstraints []string
}

// DropTableOptions configures table deletion behavior.
type DropTableOptions struct {
	// IgnoreNotFound suppresses error if table doesn't exist.
	IgnoreNotFound bool
}

// AddColumnOptions configures column addition behavior.
type AddColumnOptions struct {
	// IfColumnNotExists suppresses error if column already exists.
	IfColumnNotExists bool

	// IgnoreNotFound suppresses error if table doesn't exist.
	IgnoreNotFound bool
}

// RemoveColumnOptions configures column removal behavior.
type RemoveColumnOptions struct {
	// IfColumnExists suppresses error if column doesn't exist.
	IfColumnExists bool

	// IgnoreNotFound suppresses error if table doesn't exist.
	IgnoreNotFound bool

	// Cascade removes dependent objects along with the column.
	Cascade bool
}

// RenameColumnOptions configures column renaming behavior.
type RenameColumnOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool
}

// RenameTableOptions configures table renaming behavior.
type RenameTableOptions struct {
	// IgnoreNotFound suppresses error if table doesn't exist.
	IgnoreNotFound bool
}

// ChangeColumnTypeOptions configures column type change behavior.
type ChangeColumnTypeOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool
}

// SetNotNullOptions configures adding NOT NULL constraint behavior.
type SetNotNullOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool
}

// DropNotNullOptions configures dropping NOT NULL constraint behavior.
type DropNotNullOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool
}

// SetDefaultOptions configures setting column default value behavior.
type SetDefaultOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool
}

// AddFieldOptions configures field addition to struct columns.
type AddFieldOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool

	// IfFieldNotExists suppresses error if field already exists.
	IfFieldNotExists bool
}

// RenameFieldOptions configures field renaming in struct columns.
type RenameFieldOptions struct {
	// IgnoreNotFound suppresses error if table, column, or field doesn't exist.
	IgnoreNotFound bool
}

// RemoveFieldOptions configures field removal from struct columns.
type RemoveFieldOptions struct {
	// IgnoreNotFound suppresses error if table or column doesn't exist.
	IgnoreNotFound bool
}

// CatalogVersion contains the version information for a catalog.
type CatalogVersion struct {
	// Version is the current version number of the catalog.
	// When this changes, DuckDB refreshes its cached schema data.
	Version uint64

	// IsFixed indicates whether the version is fixed for the session.
	// When true, DuckDB caches this version and won't query again.
	IsFixed bool
}

// VersionedCatalog extends Catalog with version tracking.
// Implementations MUST be goroutine-safe.
type VersionedCatalog interface {
	Catalog

	// CatalogVersion returns the current version of the catalog.
	// When the version changes, clients should refresh their cached schema.
	CatalogVersion(ctx context.Context) (CatalogVersion, error)
}

// DynamicCatalog extends Catalog with schema management operations.
// Implementations MUST be goroutine-safe.
type DynamicCatalog interface {
	Catalog

	// CreateSchema creates a new schema in the catalog.
	// Returns the created schema or error if creation fails.
	// Returns ErrAlreadyExists if schema exists.
	CreateSchema(ctx context.Context, name string, opts CreateSchemaOptions) (Schema, error)

	// DropSchema removes a schema from the catalog.
	// Returns ErrNotFound if schema doesn't exist and IgnoreNotFound is false.
	// Returns ErrSchemaNotEmpty if schema contains tables.
	DropSchema(ctx context.Context, name string, opts DropSchemaOptions) error
}

// DynamicSchema extends Schema with table management operations.
// Implementations MUST be goroutine-safe.
type DynamicSchema interface {
	Schema

	// CreateTable creates a new table in the schema.
	// Returns the created table or error if creation fails.
	// Returns ErrAlreadyExists if table exists and OnConflict is OnConflictError.
	CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts CreateTableOptions) (Table, error)

	// DropTable removes a table from the schema.
	// Returns ErrNotFound if table doesn't exist and IgnoreNotFound is false.
	DropTable(ctx context.Context, name string, opts DropTableOptions) error

	// RenameTable renames a table in the schema.
	// Returns ErrNotFound if table doesn't exist and IgnoreNotFound is false.
	// Returns ErrAlreadyExists if newName already exists.
	RenameTable(ctx context.Context, oldName, newName string, opts RenameTableOptions) error
}

// DynamicTable extends Table with column management operations.
// Implementations MUST be goroutine-safe.
type DynamicTable interface {
	Table

	// AddColumn adds a new column to the table.
	// The columnSchema should contain a single field defining the column.
	// Returns ErrAlreadyExists if column exists and IfColumnNotExists is false.
	AddColumn(ctx context.Context, columnSchema *arrow.Schema, opts AddColumnOptions) error

	// RemoveColumn removes a column from the table.
	// Returns ErrNotFound if column doesn't exist and IfColumnExists is false.
	RemoveColumn(ctx context.Context, name string, opts RemoveColumnOptions) error

	// RenameColumn renames a column in the table.
	// Returns ErrNotFound if column doesn't exist and IgnoreNotFound is false.
	// Returns ErrAlreadyExists if newName already exists.
	RenameColumn(ctx context.Context, oldName, newName string, opts RenameColumnOptions) error

	// ChangeColumnType changes the type of a column.
	// The columnSchema should contain a single field with the new type.
	// The expression is a SQL expression for type conversion.
	// Returns ErrNotFound if column doesn't exist and IgnoreNotFound is false.
	ChangeColumnType(ctx context.Context, columnSchema *arrow.Schema, expression string, opts ChangeColumnTypeOptions) error

	// SetNotNull adds a NOT NULL constraint to a column.
	// Returns ErrNotFound if column doesn't exist and IgnoreNotFound is false.
	SetNotNull(ctx context.Context, columnName string, opts SetNotNullOptions) error

	// DropNotNull removes a NOT NULL constraint from a column.
	// Returns ErrNotFound if column doesn't exist and IgnoreNotFound is false.
	DropNotNull(ctx context.Context, columnName string, opts DropNotNullOptions) error

	// SetDefault sets or changes the default value of a column.
	// The expression is a SQL expression for the default value.
	// Returns ErrNotFound if column doesn't exist and IgnoreNotFound is false.
	SetDefault(ctx context.Context, columnName, expression string, opts SetDefaultOptions) error

	// AddField adds a field to a struct-typed column.
	// The columnPath is the path to the struct column (e.g., ["col", "nested"]).
	// The fieldSchema should contain a single field defining the new field.
	// Returns ErrNotFound if column path doesn't exist and IgnoreNotFound is false.
	// Returns ErrAlreadyExists if field exists and IfFieldNotExists is false.
	AddField(ctx context.Context, columnSchema *arrow.Schema, opts AddFieldOptions) error

	// RenameField renames a field in a struct-typed column.
	// The columnPath is the path to the field (e.g., ["col", "nested", "field"]).
	// Returns ErrNotFound if column path doesn't exist and IgnoreNotFound is false.
	RenameField(ctx context.Context, columnPath []string, newName string, opts RenameFieldOptions) error

	// RemoveField removes a field from a struct-typed column.
	// The columnPath is the path to the field (e.g., ["col", "nested", "field"]).
	// Returns ErrNotFound if column path doesn't exist and IfFieldExists is false.
	RemoveField(ctx context.Context, columnPath []string, opts RemoveFieldOptions) error
}
