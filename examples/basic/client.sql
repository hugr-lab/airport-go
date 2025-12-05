-- Basic Airport Example - DuckDB Client
-- This script demonstrates connecting to the Airport Flight server and querying data

-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the local Airport server
CREATE OR REPLACE SECRET airport_secret (
    TYPE AIRPORT,
    scope 'grpc://localhost:50051'
);

ATTACH '' AS airport_catalog (TYPE airport, SECRET airport_secret, LOCATION 'grpc://localhost:50051');

-- Query the users table
SELECT * FROM airport_catalog.demo.users;

-- Display results with column information
DESCRIBE SELECT * FROM airport_catalog.demo.users;

-- Count rows
SELECT COUNT(*) as total_users FROM airport_catalog.demo.users;
