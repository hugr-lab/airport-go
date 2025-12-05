# Dynamic Catalog Example

This example demonstrates a dynamic Airport Flight catalog with:
- **Runtime schema changes**: Add/remove schemas and tables at runtime
- **Permission-based filtering**: Different users see different schemas
- **Live data**: Tables that return current data on each query
- **Bearer token authentication**: Secure access with token validation

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
cd examples/dynamic
go run main.go
```

The server will start on `localhost:50053` with:
- `public` schema - accessible to all authenticated users
- `admin` schema - only accessible to users with "admin" identity

## Testing with DuckDB Client

### As a Regular User

Connect with user token (sees only public schema):

```sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect as regular user
CREATE OR REPLACE SECRET demo_secret (
    TYPE airport,
    LOCATION 'grpc://localhost:50053',
    auth_token 'user-token'
);

ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50053');

-- List schemas (only sees 'public')
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'demo';

-- Query live metrics
SELECT * FROM demo.public.metrics;

DETACH demo;
```

### As an Admin User

Connect with admin token (sees both schemas):

```sql
-- Connect as admin
CREATE OR REPLACE SECRET demo_secret (
    TYPE airport,
    LOCATION 'grpc://localhost:50053',
    auth_token 'admin-token'
);

ATTACH '' AS admin_demo (TYPE airport, LOCATION 'grpc://localhost:50053');

-- List schemas (sees 'public' and 'admin')
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'admin_demo';

-- Query admin settings
SELECT * FROM admin_demo.admin.settings;

-- Query public metrics
SELECT * FROM admin_demo.public.metrics;
```

## Catalog Structure

```
dynamic_catalog/
├── public/                    (accessible to all users)
│   └── metrics               (live server metrics)
│       ├── timestamp (int64)
│       ├── metric (string)
│       └── value (int64)
└── admin/                     (admin users only)
    └── settings              (server configuration)
        ├── setting (string)
        └── value (string)
```

## Valid Tokens

| Token | Identity | Access |
|-------|----------|--------|
| `admin-token` | admin | public + admin schemas |
| `user-token` | user | public schema only |

## Key Features Demonstrated

### 1. Dynamic Schema Management

```go
// Add schema at runtime
cat.AddSchema("new_schema", schema)

// Add table to schema at runtime
schema.AddTable(table)
```

### 2. Permission-Based Filtering

```go
func (s *DynamicSchema) canAccess(identity string) bool {
    if len(s.allowedUsers) == 0 {
        return true // No restrictions
    }
    for _, allowed := range s.allowedUsers {
        if allowed == identity {
            return true
        }
    }
    return false
}
```

### 3. Live Data Tables

```go
metricsTable := &LiveTable{
    name:   "metrics",
    schema: metricsSchema,
    getData: func() [][]interface{} {
        // Return current metrics on each query
        return [][]interface{}{
            {time.Now().Unix(), "uptime_seconds", int64(elapsed)},
        }
    },
}
```

### 4. Bearer Token Authentication

```go
config := airport.ServerConfig{
    Catalog: cat,
    Auth: airport.BearerAuth(func(token string) (string, error) {
        validTokens := map[string]string{
            "admin-token": "admin",
            "user-token":  "user",
        }
        if identity, ok := validTokens[token]; ok {
            return identity, nil
        }
        return "", airport.ErrUnauthorized
    }),
}
```

## Use Cases

Dynamic catalogs are useful for:

- **Multi-Tenant Systems**: Different tenants see different schemas/tables
- **Role-Based Access**: Filter data based on user permissions
- **Database Reflection**: Query live database schemas
- **API Gateways**: Expose REST APIs as SQL tables
- **Monitoring Systems**: Query metrics and logs in real-time
- **Data Pipelines**: Stream processing with SQL interface

## Next Steps

- See [auth example](../auth/) for more authentication patterns
- See [DML example](../dml/) for writable tables
- See [DDL example](../ddl/) for schema modification operations
