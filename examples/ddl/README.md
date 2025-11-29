# DDL Operations Example

This example demonstrates DDL (Data Definition Language) support in an Airport Flight server, enabling schema and table management via SQL statements like CREATE SCHEMA, DROP SCHEMA, CREATE TABLE, ALTER TABLE, etc.

## Features

- **Schema Management**: CREATE SCHEMA, DROP SCHEMA
- **Table Management**: CREATE TABLE, DROP TABLE, RENAME TABLE
- **Column Operations**: ADD COLUMN, DROP COLUMN, RENAME COLUMN
- **CREATE TABLE AS SELECT**: Copy data from one table to another

## Prerequisites

- Go 1.25+
- DuckDB 1.4+ (for client testing)
- Airport extension for DuckDB

## Running the Server

Start the DDL-enabled Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` and output:
```
Airport DDL server listening on :50051

Example catalog structure:
  - Schema: main
    - Table: sample (id INTEGER, name VARCHAR)

Test with DuckDB CLI:
  ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
```

## Testing with DuckDB

Start DuckDB and connect to the server:

```bash
duckdb
```

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the DDL server
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');

-- View existing schema and table
SELECT * FROM demo.main.sample;
```

### Schema Operations

```sql
-- Create a new schema
CREATE SCHEMA demo.analytics;

-- Drop a schema (must be empty)
DROP SCHEMA demo.analytics;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS demo.reports;
```

### Table Operations

```sql
-- Create a new table
CREATE TABLE demo.main.users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR
);

-- View the table structure
DESCRIBE demo.main.users;

-- Drop a table
DROP TABLE demo.main.users;

-- Drop table if exists
DROP TABLE IF EXISTS demo.main.users;
```

### Column Operations (ALTER TABLE)

```sql
-- Create a table to alter
CREATE TABLE demo.main.products (
    id INTEGER,
    name VARCHAR
);

-- Add a new column
ALTER TABLE demo.main.products ADD COLUMN price DOUBLE;

-- Rename a column
ALTER TABLE demo.main.products RENAME COLUMN name TO product_name;

-- Drop a column
ALTER TABLE demo.main.products DROP COLUMN price;

-- View changes
DESCRIBE demo.main.products;
```

### CREATE TABLE AS SELECT

```sql
-- Create source table
CREATE TABLE demo.main.source (id INTEGER, value VARCHAR);

-- Insert data (requires InsertableTable implementation)
INSERT INTO demo.main.source VALUES (1, 'one'), (2, 'two');

-- Create table from query
CREATE TABLE demo.main.backup AS SELECT * FROM demo.main.source;

-- Verify data was copied
SELECT * FROM demo.main.backup;
```

## Implementation Details

This example implements three interfaces from the `catalog` package:

### DynamicCatalog

```go
type DynamicCatalog interface {
    Catalog
    CreateSchema(ctx context.Context, name string, opts CreateSchemaOptions) (Schema, error)
    DropSchema(ctx context.Context, name string, opts DropSchemaOptions) error
}
```

### DynamicSchema

```go
type DynamicSchema interface {
    Schema
    CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts CreateTableOptions) (Table, error)
    DropTable(ctx context.Context, name string, opts DropTableOptions) error
    RenameTable(ctx context.Context, oldName, newName string, opts RenameTableOptions) error
}
```

### DynamicTable

```go
type DynamicTable interface {
    Table
    AddColumn(ctx context.Context, columnSchema *arrow.Schema, opts AddColumnOptions) error
    RemoveColumn(ctx context.Context, name string, opts RemoveColumnOptions) error
    RenameColumn(ctx context.Context, oldName, newName string, opts RenameColumnOptions) error
    // ... additional methods for constraints and struct fields
}
```

## Error Handling

The server returns appropriate errors for common DDL failures:

- `catalog.ErrAlreadyExists` - When creating a schema/table/column that already exists
- `catalog.ErrNotFound` - When dropping/altering a non-existent object
- `catalog.ErrSchemaNotEmpty` - When dropping a schema that contains tables

## Next Steps

- Try the [dml example](../dml/) for INSERT/UPDATE/DELETE operations
- Try the [basic example](../basic/) for simple read-only tables
- Read the [main README](../../README.md) for more advanced usage
