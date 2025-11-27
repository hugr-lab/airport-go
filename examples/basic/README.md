# Basic Airport Example

This example demonstrates a simple Apache Arrow Flight server using the Airport Go package.

## Prerequisites

- Go 1.25+
- DuckDB 1.4+ (for client testing)
- Airport extension for DuckDB

## Installation

### Install DuckDB

**macOS (Homebrew)**:
```bash
brew install duckdb
```

**Linux/Other**:
Download from https://duckdb.org/docs/installation/

### Install Airport Extension

Start DuckDB and run:
```sql
INSTALL airport FROM community;
```

## Running the Server

Start the Airport Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` and output:
```
Airport server listening on :50051
Example catalog contains:
  - Schema: main
    - Table: users (3 rows)
```

## Testing with DuckDB Client

In a separate terminal, run the DuckDB client:

```bash
duckdb < client.sql
```

Or start DuckDB interactively:

```bash
duckdb
```

Then run the commands from `client.sql`:

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the local Airport server
CREATE SECRET airport_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:50051'
);

-- Query the users table
SELECT * FROM airport_catalog.main.users;
```

## Expected Output

```
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ Alice   │
│     2 │ Bob     │
│     3 │ Charlie │
└───────┴─────────┘
```

## What's Happening

1. **Server**: The Go server creates an in-memory catalog with a "users" table
2. **Connection**: DuckDB connects to the Flight server via the Airport extension
3. **Query**: DuckDB sends a Flight `DoGet` request to retrieve table data
4. **Response**: The server streams Arrow record batches back to DuckDB
5. **Display**: DuckDB renders the results

## Next Steps

- Try the [auth example](../auth/) for authenticated connections
- Try the [dynamic example](../dynamic/) for live data sources
- Read the [main README](../../README.md) for more advanced usage
