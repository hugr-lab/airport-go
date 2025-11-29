-- Functions Example Client SQL
-- Run the server first: cd examples/functions && go run main.go
-- Then execute this SQL in DuckDB

-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Connect to the functions demo server
ATTACH '' AS funcs (TYPE airport, LOCATION 'grpc://localhost:50051');

-- ========================================
-- Scalar Function Examples
-- ========================================

-- Note: Scalar function execution via Airport is not yet fully supported by DuckDB.
-- These examples show the intended usage once support is added.

-- Multiply values in the users table
-- SELECT id, name, value, funcs.functions_demo.MULTIPLY(value, 2) as doubled
-- FROM funcs.functions_demo.users;

-- ========================================
-- Table Function Examples
-- ========================================

-- Example 1: GENERATE_SERIES with default step (1)
-- Generates: 1, 2, 3, 4, 5
SELECT * FROM funcs.functions_demo.GENERATE_SERIES(1, 5);

-- Example 2: GENERATE_SERIES with custom step
-- Generates: 0, 10, 20, 30, 40, 50
SELECT * FROM funcs.functions_demo.GENERATE_SERIES(0, 50, 10);

-- Example 3: GENERATE_SERIES with negative step
-- Generates: 10, 8, 6, 4, 2
SELECT * FROM funcs.functions_demo.GENERATE_SERIES(10, 1, -2);

-- Example 4: GENERATE_RANGE with 3 columns
-- Creates a table with columns: col1, col2, col3
-- Each row i has values: i*1, i*2, i*3
SELECT * FROM funcs.functions_demo.GENERATE_RANGE(1, 5, 3);

-- Example 5: GENERATE_RANGE with single column
-- Creates a table with just col1
SELECT * FROM funcs.functions_demo.GENERATE_RANGE(1, 10, 1);

-- Example 6: GENERATE_RANGE with many columns
-- Creates a table with 5 columns
SELECT * FROM funcs.functions_demo.GENERATE_RANGE(1, 3, 5);

-- ========================================
-- Combining Table Functions with Regular Tables
-- ========================================

-- Join generated series with user data
SELECT
    s.value as series_value,
    u.name,
    u.value as user_value
FROM funcs.functions_demo.GENERATE_SERIES(1, 3) s
LEFT JOIN funcs.functions_demo.users u ON s.value = u.id;

-- Use generated series for filtering
SELECT *
FROM funcs.functions_demo.users
WHERE id IN (
    SELECT value FROM funcs.functions_demo.GENERATE_SERIES(1, 2)
);

-- ========================================
-- Schema Discovery
-- ========================================

-- List all tables and functions in the schema
SELECT table_name, table_type
FROM duckdb_tables()
WHERE schema_name = 'functions_demo'
  AND database_name = 'funcs';

-- Describe the dynamic schema (will show different columns based on parameters)
DESCRIBE SELECT * FROM funcs.functions_demo.GENERATE_RANGE(1, 1, 4);

-- ========================================
-- Cleanup
-- ========================================
DETACH funcs;
