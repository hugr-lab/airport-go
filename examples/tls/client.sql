-- TLS Example Client SQL
-- Run the server first: go run main.go
-- Then execute this SQL in DuckDB

-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Method 1: Direct attach with TLS (no authentication)
ATTACH 'grpc+tls://localhost:50051' AS secure_data (TYPE airport);

-- Query the secure data
SELECT * FROM secure_data.secure.messages;

-- Method 2: Using SECRET for authentication and/or client certificates
CREATE OR REPLACE SECRET airport_tls_secret (
    TYPE AIRPORT,
    auth_token 'your-bearer-token-here',  -- Optional bearer token
    tls_cert_path 'client-cert.pem',      -- For mutual TLS
    tls_key_path 'client-key.pem',        -- For mutual TLS
    tls_ca_path 'ca-cert.pem',            -- CA certificate
    scope 'grpc+tls://localhost:50051'
);

ATTACH '' AS secure_data_auth (
    TYPE airport,
    SECRET airport_tls_secret,
    LOCATION 'grpc+tls://localhost:50051'
);

-- Query with authenticated connection
SELECT * FROM secure_data_auth.secure.messages;

-- List schemas
SELECT schema_name
FROM duckdb_schemas()
WHERE database_name = 'secure_data';

-- List tables
SELECT table_name
FROM duckdb_tables()
WHERE schema_name = 'secure'
  AND database_name = 'secure_data';

-- Cleanup
DETACH secure_data;
DETACH secure_data_auth;
