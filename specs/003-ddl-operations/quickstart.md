# Quickstart: DDL Operations

**Feature**: 003-ddl-operations

This guide shows how to implement a dynamic catalog that supports DDL operations.

## Overview

The airport-go package supports two types of catalogs:
1. **Static catalogs** - Built with `NewCatalogBuilder()`, immutable after creation
2. **Dynamic catalogs** - Implement `DynamicCatalog`, `DynamicSchema`, `DynamicTable` interfaces for runtime modifications

## Implementing a Dynamic Catalog

### Step 1: Implement DynamicCatalog

```go
package mydb

import (
    "context"
    "sync"

    "github.com/hugr-lab/airport-go/catalog"
)

type MyCatalog struct {
    mu      sync.RWMutex
    schemas map[string]*MySchema
}

// Implement catalog.Catalog interface
func (c *MyCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    schemas := make([]catalog.Schema, 0, len(c.schemas))
    for _, s := range c.schemas {
        schemas = append(schemas, s)
    }
    return schemas, nil
}

func (c *MyCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if s, ok := c.schemas[name]; ok {
        return s, nil
    }
    return nil, nil // Not found (not an error)
}

// Implement catalog.DynamicCatalog interface
func (c *MyCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if _, exists := c.schemas[name]; exists {
        return nil, catalog.ErrAlreadyExists
    }

    schema := &MySchema{
        name:    name,
        comment: opts.Comment,
        tables:  make(map[string]*MyTable),
    }
    c.schemas[name] = schema
    return schema, nil
}

func (c *MyCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    schema, exists := c.schemas[name]
    if !exists {
        if opts.IgnoreNotFound {
            return nil
        }
        return catalog.ErrNotFound
    }

    // Check for tables (FR-016)
    if len(schema.tables) > 0 {
        return catalog.ErrSchemaNotEmpty
    }

    delete(c.schemas, name)
    return nil
}
```

### Step 2: Implement DynamicSchema

```go
type MySchema struct {
    mu      sync.RWMutex
    name    string
    comment string
    tables  map[string]*MyTable
}

// Implement catalog.Schema interface
func (s *MySchema) Name() string    { return s.name }
func (s *MySchema) Comment() string { return s.comment }

func (s *MySchema) Tables(ctx context.Context) ([]catalog.Table, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    tables := make([]catalog.Table, 0, len(s.tables))
    for _, t := range s.tables {
        tables = append(tables, t)
    }
    return tables, nil
}

func (s *MySchema) Table(ctx context.Context, name string) (catalog.Table, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    if t, ok := s.tables[name]; ok {
        return t, nil
    }
    return nil, nil
}

// Functions (return empty for this example)
func (s *MySchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
    return nil, nil
}
func (s *MySchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
    return nil, nil
}
func (s *MySchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
    return nil, nil
}

// Implement catalog.DynamicSchema interface
func (s *MySchema) CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts catalog.CreateTableOptions) (catalog.Table, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if existing, exists := s.tables[name]; exists {
        switch opts.OnConflict {
        case catalog.OnConflictIgnore:
            return existing, nil
        case catalog.OnConflictReplace:
            // Fall through to create new table
        default: // OnConflictError
            return nil, catalog.ErrAlreadyExists
        }
    }

    table := &MyTable{
        name:    name,
        schema:  schema,
        comment: opts.Comment,
    }
    s.tables[name] = table
    return table, nil
}

func (s *MySchema) DropTable(ctx context.Context, name string, opts catalog.DropTableOptions) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, exists := s.tables[name]; !exists {
        if opts.IgnoreNotFound {
            return nil
        }
        return catalog.ErrNotFound
    }

    delete(s.tables, name)
    return nil
}
```

### Step 3: Implement DynamicTable

```go
type MyTable struct {
    mu      sync.RWMutex
    name    string
    schema  *arrow.Schema
    comment string
    data    [][]any // Your storage implementation
}

// Implement catalog.Table interface
func (t *MyTable) Name() string                         { return t.name }
func (t *MyTable) Comment() string                      { return t.comment }
func (t *MyTable) ArrowSchema(cols []string) *arrow.Schema {
    return catalog.ProjectSchema(t.schema, cols)
}

func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // Your data retrieval implementation
    // ...
}

// Implement catalog.DynamicTable interface
func (t *MyTable) AddColumn(ctx context.Context, columnSchema *arrow.Schema, opts catalog.AddColumnOptions) error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if columnSchema.NumFields() != 1 {
        return errors.New("column schema must contain exactly one field")
    }

    newCol := columnSchema.Field(0)

    // Check if column exists
    for i := 0; i < t.schema.NumFields(); i++ {
        if t.schema.Field(i).Name == newCol.Name {
            if opts.IfColumnNotExists {
                return nil
            }
            return catalog.ErrAlreadyExists
        }
    }

    // Add column to schema
    fields := make([]arrow.Field, t.schema.NumFields()+1)
    for i := 0; i < t.schema.NumFields(); i++ {
        fields[i] = t.schema.Field(i)
    }
    fields[len(fields)-1] = newCol

    meta := t.schema.Metadata()
    t.schema = arrow.NewSchema(fields, &meta)

    // Add NULL values to existing data
    for i := range t.data {
        t.data[i] = append(t.data[i], nil)
    }

    return nil
}

func (t *MyTable) RemoveColumn(ctx context.Context, name string, opts catalog.RemoveColumnOptions) error {
    t.mu.Lock()
    defer t.mu.Unlock()

    // Find column index
    colIdx := -1
    for i := 0; i < t.schema.NumFields(); i++ {
        if t.schema.Field(i).Name == name {
            colIdx = i
            break
        }
    }

    if colIdx < 0 {
        if opts.IfColumnExists {
            return nil
        }
        return catalog.ErrNotFound
    }

    // Remove column from schema
    fields := make([]arrow.Field, 0, t.schema.NumFields()-1)
    for i := 0; i < t.schema.NumFields(); i++ {
        if i != colIdx {
            fields = append(fields, t.schema.Field(i))
        }
    }

    meta := t.schema.Metadata()
    t.schema = arrow.NewSchema(fields, &meta)

    // Remove column data
    for i := range t.data {
        t.data[i] = append(t.data[i][:colIdx], t.data[i][colIdx+1:]...)
    }

    return nil
}
```

### Step 4: Register with Airport Server

```go
package main

import (
    "net"

    "google.golang.org/grpc"
    "github.com/hugr-lab/airport-go"
)

func main() {
    // Create your dynamic catalog
    myCatalog := &mydb.MyCatalog{
        schemas: make(map[string]*mydb.MySchema),
    }

    // Create a default schema
    myCatalog.CreateSchema(context.Background(), "main", catalog.CreateSchemaOptions{
        Comment: "Default schema",
    })

    // Configure server
    config := airport.ServerConfig{
        Catalog: myCatalog,
        Address: "localhost:50051",
    }

    // Create gRPC server
    lis, _ := net.Listen("tcp", config.Address)
    grpcServer := grpc.NewServer(airport.ServerOptions(config)...)

    // Register Airport service
    airport.NewServer(grpcServer, config)

    // Start serving
    grpcServer.Serve(lis)
}
```

## Using DDL from DuckDB

Once your dynamic catalog server is running, connect from DuckDB:

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Attach to your server
ATTACH '' AS my_db (TYPE airport, LOCATION 'grpc://localhost:50051');

-- Create a schema
CREATE SCHEMA my_db.new_schema;

-- Create a table
CREATE TABLE my_db.new_schema.users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR
);

-- Add a column
ALTER TABLE my_db.new_schema.users ADD COLUMN created_at TIMESTAMP;

-- Drop the column
ALTER TABLE my_db.new_schema.users DROP COLUMN created_at;

-- Drop the table
DROP TABLE my_db.new_schema.users;

-- Drop the schema
DROP SCHEMA my_db.new_schema;
```

## Thread Safety Notes

All dynamic catalog implementations MUST be thread-safe:

1. Use `sync.RWMutex` for read/write operations
2. Lock for writes (CreateSchema, DropSchema, CreateTable, etc.)
3. RLock for reads (Schemas, Schema, Tables, Table)
4. The server may call these methods concurrently from multiple goroutines

## Error Handling

Return standard sentinel errors from catalog package:

```go
import "github.com/hugr-lab/airport-go/catalog"

// When object already exists
return nil, catalog.ErrAlreadyExists

// When object not found
return catalog.ErrNotFound

// When schema contains tables
return catalog.ErrSchemaNotEmpty
```

The server maps these to appropriate gRPC status codes automatically.
