# Research: DML RETURNING Clause Column Selection

**Date**: 2025-11-30
**Feature**: 006-returning-optimization

## Key Finding: RETURNING Columns Not Communicated via Protocol

### Investigation Summary

After investigating the DuckDB Airport extension source code and protocol documentation, I discovered that **the DuckDB Airport extension does NOT communicate RETURNING column names to the server**.

### Evidence

1. **Airport Extension Source Code** (`airport_exchange.cpp`):
   - The `destination_chunk_column_names` parameter is used **client-side only** for mapping received columns to output positions
   - It is **never serialized or transmitted** in headers, metadata, or schema information
   - The only column-related communication is the input schema (columns being inserted/updated)

2. **Airport Extension Source Code** (`airport_insert.cpp`):
   - Returns **all table columns** via `AirportGetInsertColumns()`:
     ```cpp
     // Since we are supporting default values now, we want to end all
     // columns of the table rather than just the columns the user has specified.
     for (auto &col : entry.GetColumns().Logical()) {
       column_types.push_back(col.GetType());
       column_names.push_back(col.GetName());
     }
     ```

3. **Protocol Headers Sent**:
   - `airport-operation`: "insert", "update", or "delete"
   - `airport-flight-path`: "schema/table"
   - `return-chunks`: "1" or "0" (boolean flag only)
   - `airport-transaction-id`: (if applicable)

   **No header for RETURNING column names exists.**

4. **Current airport-go Implementation**:
   - Uses `inputSchema` (incoming data schema) for RETURNING results
   - The test `INSERT ... RETURNING id` only returns `id` because the test table implementation builds RETURNING data from `inputSchema` columns

### Decision: Server-Side Column Filtering

**Decision**: The server receives all columns from the table implementation's RETURNING data and relies on DuckDB to filter on the client side.

**Rationale**:
1. The Airport protocol does not support passing RETURNING column names to the server
2. DuckDB handles column projection client-side after receiving server response
3. Changing the protocol would require coordination with DuckDB Airport extension maintainers
4. Current approach is functional - the server returns what the table can provide

**Alternatives Considered**:

| Alternative | Rejected Because |
|-------------|------------------|
| Add custom header for RETURNING columns | Non-standard; requires DuckDB extension changes |
| Parse SQL to extract column names | Not possible - server only receives Arrow data, not SQL |
| Use input schema as RETURNING hint | Input schema may not match desired RETURNING columns |

### Implications for Implementation

Since the protocol doesn't communicate RETURNING column names:

1. **DMLOptions.ReturningColumns** should still be added for:
   - Future protocol extensions
   - Server-side implementations that want to optimize data retrieval
   - Documentation of intended behavior

2. **The server** should:
   - Pass `DMLOptions` to table implementations
   - Let implementations decide what to return
   - If `ReturningColumns` is empty/nil, return all available columns (current behavior)
   - If `ReturningColumns` is specified, filter or optimize accordingly

3. **Table implementations** can:
   - Ignore `ReturningColumns` and return all data (DuckDB filters client-side)
   - Use `ReturningColumns` to optimize database queries (e.g., SELECT only needed columns)
   - Return only specified columns for efficiency

### How Current Tests Work

The existing `TestDMLInsertReturning` test works because:
1. The test table's `buildReturningReader()` method builds data from `inputSchema`
2. `inputSchema` is the schema of data sent by DuckDB for INSERT
3. When executing `INSERT INTO table (id, name, email) VALUES (...) RETURNING id`:
   - DuckDB sends `id, name, email` columns
   - Server returns data with those columns
   - DuckDB filters to only `id` on client side

### Recommended Approach

```go
// DMLOptions carries options for DML operations
type DMLOptions struct {
    // ReturningColumns specifies which columns to include in RETURNING results.
    // If nil or empty, implementation decides what to return (typically all columns).
    // This is a hint for optimization - implementations MAY ignore it.
    // Note: DuckDB Airport extension does not currently send this information;
    // this field is for future protocol extensions and server-side optimization.
    ReturningColumns []string
}
```

The implementation should:
1. Add `DMLOptions` parameter to DML interface methods
2. Pass empty `ReturningColumns` (since DuckDB doesn't provide it)
3. Allow implementations to use `ReturningColumns` if they populate it internally
4. Document that this is for future use and optimization
