# Data Model: DDL Operations

**Feature**: 003-ddl-operations
**Date**: 2025-11-29

## Entity Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        catalog.Catalog                          │
│  (existing interface - read-only catalog operations)            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   catalog.DynamicCatalog                        │
│  (new interface - extends Catalog with schema management)       │
│  + CreateSchema(ctx, name, opts) (Schema, error)                │
│  + DropSchema(ctx, name, opts) error                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        catalog.Schema                           │
│  (existing interface - read-only schema operations)             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    catalog.DynamicSchema                        │
│  (new interface - extends Schema with table management)         │
│  + CreateTable(ctx, name, schema, opts) (Table, error)          │
│  + DropTable(ctx, name, opts) error                             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        catalog.Table                            │
│  (existing interface - read-only table operations)              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    catalog.DynamicTable                         │
│  (new interface - extends Table with column management)         │
│  + AddColumn(ctx, schema, opts) error                           │
│  + RemoveColumn(ctx, name, opts) error                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Interfaces

### DynamicCatalog

Extends `Catalog` to support runtime schema creation and deletion.

```go
// DynamicCatalog extends Catalog with schema management operations.
// Implementations MUST be goroutine-safe.
type DynamicCatalog interface {
    Catalog

    // CreateSchema creates a new schema in the catalog.
    // Returns the created schema or error if creation fails.
    // Returns ErrAlreadyExists if schema exists and OnConflict is Error.
    CreateSchema(ctx context.Context, name string, opts CreateSchemaOptions) (Schema, error)

    // DropSchema removes a schema from the catalog.
    // Returns ErrNotFound if schema doesn't exist and IgnoreNotFound is false.
    // Returns ErrSchemaNotEmpty if schema contains tables.
    DropSchema(ctx context.Context, name string, opts DropSchemaOptions) error
}
```

### DynamicSchema

Extends `Schema` to support runtime table creation and deletion.

```go
// DynamicSchema extends Schema with table management operations.
// Implementations MUST be goroutine-safe.
type DynamicSchema interface {
    Schema

    // CreateTable creates a new table in the schema.
    // Returns the created table or error if creation fails.
    // Returns ErrAlreadyExists if table exists and OnConflict is Error.
    CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts CreateTableOptions) (Table, error)

    // DropTable removes a table from the schema.
    // Returns ErrNotFound if table doesn't exist and IgnoreNotFound is false.
    DropTable(ctx context.Context, name string, opts DropTableOptions) error
}
```

### DynamicTable

Extends `Table` to support runtime column modification.

```go
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
}
```

---

## Option Structs

### CreateSchemaOptions

```go
// CreateSchemaOptions configures schema creation behavior.
type CreateSchemaOptions struct {
    // Comment is optional documentation for the schema.
    Comment string

    // Tags are optional key-value metadata pairs.
    Tags map[string]string
}
```

### DropSchemaOptions

```go
// DropSchemaOptions configures schema deletion behavior.
type DropSchemaOptions struct {
    // IgnoreNotFound suppresses error if schema doesn't exist.
    IgnoreNotFound bool
}
```

### CreateTableOptions

```go
// OnConflict specifies behavior when table already exists.
type OnConflict string

const (
    // OnConflictError returns an error if the table exists.
    OnConflictError OnConflict = "error"

    // OnConflictIgnore silently succeeds if the table exists.
    OnConflictIgnore OnConflict = "ignore"

    // OnConflictReplace drops and recreates the table.
    OnConflictReplace OnConflict = "replace"
)

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
```

### DropTableOptions

```go
// DropTableOptions configures table deletion behavior.
type DropTableOptions struct {
    // IgnoreNotFound suppresses error if table doesn't exist.
    IgnoreNotFound bool
}
```

### AddColumnOptions

```go
// AddColumnOptions configures column addition behavior.
type AddColumnOptions struct {
    // IfColumnNotExists suppresses error if column already exists.
    IfColumnNotExists bool

    // IgnoreNotFound suppresses error if table doesn't exist.
    IgnoreNotFound bool
}
```

### RemoveColumnOptions

```go
// RemoveColumnOptions configures column removal behavior.
type RemoveColumnOptions struct {
    // IfColumnExists suppresses error if column doesn't exist.
    IfColumnExists bool

    // IgnoreNotFound suppresses error if table doesn't exist.
    IgnoreNotFound bool

    // Cascade removes dependent objects along with the column.
    Cascade bool
}
```

---

## Sentinel Errors

```go
var (
    // ErrAlreadyExists is returned when creating an object that already exists.
    ErrAlreadyExists = errors.New("already exists")

    // ErrNotFound is returned when an object doesn't exist.
    ErrNotFound = errors.New("not found")

    // ErrSchemaNotEmpty is returned when dropping a schema that contains tables.
    ErrSchemaNotEmpty = errors.New("schema contains tables")
)
```

---

## Request/Response Structures

### Msgpack Request Parameters

```go
// CreateSchemaParams for create_schema action.
type CreateSchemaParams struct {
    CatalogName string            `msgpack:"catalog_name"`
    Schema      string            `msgpack:"schema"`
    Comment     *string           `msgpack:"comment,omitempty"`
    Tags        map[string]string `msgpack:"tags,omitempty"`
}

// DropSchemaParams for drop_schema action.
type DropSchemaParams struct {
    Type           string `msgpack:"type"`           // Always "schema"
    CatalogName    string `msgpack:"catalog_name"`
    SchemaName     string `msgpack:"schema_name"`
    Name           string `msgpack:"name"`
    IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// CreateTableParams for create_table action.
type CreateTableParams struct {
    CatalogName        string   `msgpack:"catalog_name"`
    SchemaName         string   `msgpack:"schema_name"`
    TableName          string   `msgpack:"table_name"`
    ArrowSchema        string   `msgpack:"arrow_schema"`
    OnConflict         string   `msgpack:"on_conflict"`
    NotNullConstraints []uint64 `msgpack:"not_null_constraints"`
    UniqueConstraints  []uint64 `msgpack:"unique_constraints"`
    CheckConstraints   []string `msgpack:"check_constraints"`
}

// DropTableParams for drop_table action.
type DropTableParams struct {
    Type           string `msgpack:"type"`           // Always "table"
    CatalogName    string `msgpack:"catalog_name"`
    SchemaName     string `msgpack:"schema_name"`
    Name           string `msgpack:"name"`
    IgnoreNotFound bool   `msgpack:"ignore_not_found"`
}

// AddColumnParams for add_column action.
type AddColumnParams struct {
    Catalog           string `msgpack:"catalog"`
    Schema            string `msgpack:"schema"`
    Name              string `msgpack:"name"`
    ColumnSchema      string `msgpack:"column_schema"`
    IgnoreNotFound    bool   `msgpack:"ignore_not_found"`
    IfColumnNotExists bool   `msgpack:"if_column_not_exists"`
}

// RemoveColumnParams for remove_column action.
type RemoveColumnParams struct {
    Catalog        string `msgpack:"catalog"`
    Schema         string `msgpack:"schema"`
    Name           string `msgpack:"name"`
    RemovedColumn  string `msgpack:"removed_column"`
    IgnoreNotFound bool   `msgpack:"ignore_not_found"`
    IfColumnExists bool   `msgpack:"if_column_exists"`
    Cascade        bool   `msgpack:"cascade"`
}
```

---

## State Transitions

### Schema Lifecycle

```
                    ┌─────────────┐
                    │   (none)    │
                    └─────────────┘
                          │
                          ▼ create_schema
                    ┌─────────────┐
                    │   ACTIVE    │ ◄──── Tables can be added
                    └─────────────┘
                          │
                          ▼ drop_schema (no tables)
                    ┌─────────────┐
                    │  (deleted)  │
                    └─────────────┘
```

### Table Lifecycle

```
                    ┌─────────────┐
                    │   (none)    │
                    └─────────────┘
                          │
                          ▼ create_table
                    ┌─────────────┐
                    │   ACTIVE    │ ◄──── Columns can be modified
                    └─────────────┘
                          │
                          ▼ drop_table
                    ┌─────────────┐
                    │  (deleted)  │
                    └─────────────┘
```

---

## Validation Rules

| Entity | Rule | Error |
|--------|------|-------|
| Schema | Name must be non-empty | InvalidArgument |
| Schema | Name must not conflict (when on_conflict=error) | AlreadyExists |
| Schema | Must be empty before drop | SchemaNotEmpty |
| Table | Name must be non-empty | InvalidArgument |
| Table | Schema must be valid Arrow schema | InvalidArgument |
| Table | Name must not conflict (when on_conflict=error) | AlreadyExists |
| Column | Schema must contain exactly one field | InvalidArgument |
| Column | Name must not conflict (when if_column_not_exists=false) | AlreadyExists |
