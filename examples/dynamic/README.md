# Dynamic Catalog Example

This example demonstrates a dynamic catalog that reflects live data sources.

## Prerequisites

- Go 1.25+
- DuckDB 1.4+ (for client testing)
- Airport extension for DuckDB

## Installation

### Install DuckDB and Airport Extension

See the [basic example README](../basic/README.md) for installation instructions.

## Running the Server

Start the dynamic catalog Airport Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` with a live data source that updates every second.

## Testing with DuckDB Client

Run the DuckDB client:

```bash
duckdb < client.sql
```

Or manually:

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the server
CREATE SECRET airport_dynamic_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:50051'
);

-- Query live data
SELECT * FROM airport_catalog.main.live_data;
```

## Dynamic Catalog Features

This example demonstrates:

1. **Live Schema**: The catalog schema can change at runtime
2. **Fresh Data**: Each query returns current data from the source
3. **No Caching**: Data is generated on-demand for each request
4. **Time-Based Data**: The `live_data` table shows current timestamp

## Use Cases

Dynamic catalogs are useful for:

- **Database Reflection**: Query live database schemas
- **API Gateways**: Expose REST APIs as SQL tables
- **Monitoring Systems**: Query metrics and logs in real-time
- **Data Pipelines**: Stream processing with SQL interface
- **Multi-Tenant Systems**: Schema varies per user/tenant

## Implementation Pattern

The dynamic catalog implements the `Catalog` interface:

```go
type DynamicCatalog struct {
    // Your data source connections
}

func (c *DynamicCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    // Return current schemas from your data source
}

func (c *DynamicCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
    // Return specific schema with current state
}
```

## Performance Tips

1. **Cache Metadata**: Cache schema/table definitions if they don't change often
2. **Connection Pooling**: Reuse connections to your data sources
3. **Lazy Loading**: Only fetch table lists when accessed
4. **Pagination**: Use scan options to limit result set sizes

## Next Steps

- Implement your own dynamic catalog for your data source
- Add caching layers for frequently accessed metadata
- See [catalog package docs](../../catalog/) for interface details
