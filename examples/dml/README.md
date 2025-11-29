# DML Operations Example

This example demonstrates DML (Data Manipulation Language) support in an Airport Flight server, enabling data modification via INSERT, UPDATE, and DELETE statements with full transaction support including ROLLBACK.

## Features

- **INSERT**: Add new rows to tables
- **UPDATE**: Modify existing rows by row ID
- **DELETE**: Remove rows by row ID
- **Transactions**: BEGIN, COMMIT, ROLLBACK support
- **Row IDs**: Automatic row identification for UPDATE/DELETE

## Prerequisites

- Go 1.25+
- DuckDB 1.4+ (for client testing)
- Airport extension for DuckDB

## Running the Server

Start the DML-enabled Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` and output:
```
Airport DML server listening on :50051
Example catalog contains:
  - Schema: main
    - Table: users (writable with transaction support)
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

-- Connect to the DML server
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
```

### Basic DML Operations

```sql
-- Insert data
INSERT INTO demo.main.users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com');

-- View the data
SELECT * FROM demo.main.users;

-- Update a row
UPDATE demo.main.users SET name = 'Alicia' WHERE id = 1;

-- Delete a row
DELETE FROM demo.main.users WHERE id = 3;

-- Verify changes
SELECT * FROM demo.main.users;
```

### Transaction Support

```sql
-- Start a transaction
BEGIN TRANSACTION;

-- Insert data within transaction
INSERT INTO demo.main.users (id, name, email) VALUES (100, 'TxUser', 'tx@example.com');

-- Data is visible within the transaction
SELECT * FROM demo.main.users;

-- Rollback discards changes
ROLLBACK;

-- TxUser is gone
SELECT * FROM demo.main.users;
```

### Transaction with Commit

```sql
-- Start a transaction
BEGIN TRANSACTION;

-- Make changes
INSERT INTO demo.main.users (id, name, email) VALUES (200, 'Committed', 'commit@example.com');

-- Commit persists changes
COMMIT;

-- Data is permanent
SELECT * FROM demo.main.users;
```

## Implementation Details

This example implements DML interfaces from the `catalog` package:

### InsertableTable

```go
type InsertableTable interface {
    Table
    Insert(ctx context.Context, rows array.RecordReader) (*DMLResult, error)
}
```

### UpdatableTable

```go
type UpdatableTable interface {
    Table
    Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*DMLResult, error)
}
```

### DeletableTable

```go
type DeletableTable interface {
    Table
    Delete(ctx context.Context, rowIDs []int64) (*DMLResult, error)
}
```

### TransactionManager

```go
type TransactionManager interface {
    BeginTransaction(ctx context.Context) (string, error)
    CommitTransaction(ctx context.Context, txID string) error
    RollbackTransaction(ctx context.Context, txID string) error
    GetTransactionStatus(ctx context.Context, txID string) (TransactionState, bool)
}
```

## Row ID Support

For UPDATE and DELETE operations, tables must expose a row ID column:

```go
// Schema with rowid - required for UPDATE/DELETE
rowidMeta := arrow.NewMetadata([]string{"is_rowid"}, []string{"true"})
schema := arrow.NewSchema([]arrow.Field{
    {Name: "rowid", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: rowidMeta},
    {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
    {Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)
```

The `is_rowid` metadata tells DuckDB which column to use for identifying rows during UPDATE/DELETE.

## DMLResult

DML operations return a result with affected row count:

```go
type DMLResult struct {
    AffectedRows  int64
    ReturningData array.RecordReader  // For INSERT...RETURNING
}
```

## Next Steps

- Try the [ddl example](../ddl/) for CREATE/DROP/ALTER operations
- Try the [basic example](../basic/) for simple read-only tables
- Read the [main README](../../README.md) for more advanced usage
