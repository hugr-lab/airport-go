-- Dynamic Catalog Example - DuckDB Client
-- This script demonstrates connecting to an Airport server with dynamic catalog

-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the local Airport server
CREATE OR REPLACE SECRET airport_dynamic_secret (
    TYPE AIRPORT,
    auth_token 'admin-token',
    scope 'grpc://localhost:50053'
);

ATTACH '' AS airport_catalog (TYPE airport, SECRET airport_dynamic_secret, LOCATION 'grpc://localhost:50053');

-- List available schemas
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 'airport_catalog';

-- Query the live data table
SELECT * FROM airport_catalog.main.live_data;

-- Show table structure
DESCRIBE SELECT * FROM airport_catalog.main.live_data;

-- Example: Query with filtering
SELECT * FROM airport_catalog.main.live_data
WHERE timestamp > current_timestamp - INTERVAL '5 minutes';
