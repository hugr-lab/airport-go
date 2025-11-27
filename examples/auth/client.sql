-- Authentication Example - DuckDB Client
-- This script demonstrates connecting to an authenticated Airport Flight server

-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the Airport server with bearer token authentication
CREATE OR REPLACE SECRET airport_auth_secret (
    TYPE AIRPORT,
    auth_token 'secret-api-key',
    scope 'grpc://localhost:50051'
);

ATTACH '' AS airport_catalog (TYPE airport, SECRET airport_auth_secret, LOCATION 'grpc://localhost:50051');

-- Query protected data using the authenticated connection
SELECT * FROM airport_catalog.main.users;

-- Verify authentication is working
SELECT COUNT(*) as total_users FROM airport_catalog.main.users;

-- Example: Query specific user
SELECT * FROM airport_catalog.main.users WHERE id = 1;
