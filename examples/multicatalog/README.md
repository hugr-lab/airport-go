# Multi-Catalog Server Example

This example demonstrates a multi-catalog Airport Flight server that serves multiple named catalogs from a single endpoint. Clients select the target catalog via the `airport-catalog` gRPC metadata header.

## Features

- **Multiple Catalogs**: Serve "sales" and "analytics" catalogs from one server
- **Dynamic Management**: Add and remove catalogs at runtime
- **Named Catalogs**: Each catalog has a unique name for routing
- **Catalog Routing**: Clients specify the target catalog via gRPC metadata

## Prerequisites

- Go 1.26+
- DuckDB 1.5+ (for client testing)
- Airport extension for DuckDB

## Running the Server

Start the multi-catalog Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` and output:
```
Multi-Catalog Airport server listening on :50051

Initial catalogs:
  - sales (sales_data.orders table)
  - analytics (analytics_data.metrics table)

Dynamic catalog 'inventory' will be added after 5 seconds
```

After 5 seconds, the server dynamically adds an "inventory" catalog, and removes it 10 seconds later.

## Testing with DuckDB Client

In a separate terminal, start DuckDB:

```bash
duckdb
```

### Query the Sales Catalog

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Attach the sales catalog
ATTACH 'sales' AS sales (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

-- Query orders
SELECT * FROM sales.sales_data.orders;
```

Expected output:
```
┌──────────┬─────────────┬─────────┐
│ order_id │  customer   │ amount  │
│  int64   │   varchar   │ double  │
├──────────┼─────────────┼─────────┤
│     1001 │ Acme Corp   │  1500.0 │
│     1002 │ Widgets Inc │  2300.5 │
│     1003 │ TechStart   │  890.75 │
└──────────┴─────────────┴─────────┘
```

### Query the Analytics Catalog

```sql
-- Attach the analytics catalog
ATTACH 'analytics' AS analytics (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

-- Query metrics
SELECT * FROM analytics.analytics_data.metrics;
```

### Query the Dynamic Inventory Catalog

After the server adds the inventory catalog (5 seconds after startup):

```sql
-- Attach the inventory catalog
ATTACH 'inventory' AS inventory (TYPE AIRPORT, LOCATION 'grpc://localhost:50051');

-- Query products
SELECT * FROM inventory.inventory_data.products;
```

## Implementation Details

### Multi-Catalog Setup

```go
config := airport.MultiCatalogServerConfig{
    Catalogs: []catalog.Catalog{salesCatalog, analyticsCatalog},
}

opts := airport.MultiCatalogServerOptions(config)
grpcServer := grpc.NewServer(opts...)

mcs, err := airport.NewMultiCatalogServer(grpcServer, config)
```

### Named Catalogs

Each catalog must implement the `NamedCatalog` interface:

```go
type namedCatalog struct {
    catalog.Catalog
    name string
}

func (c *namedCatalog) Name() string {
    return c.name
}
```

### Dynamic Catalog Management

Add and remove catalogs at runtime:

```go
// Add a catalog
mcs.AddCatalog(inventoryCatalog)

// Remove a catalog by name
mcs.RemoveCatalog("inventory")
```

## Catalog Structure

```
Server (grpc://localhost:50051)
├── sales/                         (catalog: "sales")
│   └── sales_data/                (schema)
│       └── orders                 (table)
│           ├── order_id (int64)
│           ├── customer (string)
│           └── amount (float64)
├── analytics/                     (catalog: "analytics")
│   └── analytics_data/            (schema)
│       └── metrics                (table)
│           ├── metric_name (string)
│           ├── value (float64)
│           └── timestamp (string)
└── inventory/                     (catalog: "inventory", added dynamically)
    └── inventory_data/            (schema)
        └── products               (table)
            ├── sku (string)
            ├── name (string)
            └── quantity (int64)
```

## Next Steps

- Try the [auth example](../auth/) for authenticated connections
- Try the [dynamic example](../dynamic/) for permission-based schema filtering
- Read the [main README](../../README.md) for more features
