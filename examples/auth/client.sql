-- Authentication Example - DuckDB Client
-- This script demonstrates connecting to an authenticated Airport Flight server

-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Create a persistent secret with auth_token scoped to the server URL
-- DuckDB will automatically use this secret for matching scopes
-- Valid tokens: secret-admin-token, secret-user1-token, secret-user2-token, secret-guest-token
CREATE PERSISTENT SECRET airport_auth_secret (
    TYPE airport,
    auth_token 'secret-admin-token',
    scope 'grpc://localhost:50052'
);

-- Attach the server (secret applies automatically via scope)
ATTACH 'airport_catalog' AS airport_catalog (
    TYPE AIRPORT,
    location 'grpc://localhost:50052'
);

-- Query protected data using the authenticated connection
SELECT * FROM airport_catalog.main.users;

-- Verify authentication is working
SELECT COUNT(*) as total_users FROM airport_catalog.main.users;

-- Example: Query specific user
SELECT * FROM airport_catalog.main.users WHERE id = 1;
