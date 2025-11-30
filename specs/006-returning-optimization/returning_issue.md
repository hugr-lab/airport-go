# Feature Request: Communicate RETURNING Column Names to Server

## Summary

When executing DML statements with a RETURNING clause (e.g., `INSERT ... RETURNING id`), the server currently has no way to know which specific columns are requested in the RETURNING clause. This prevents server-side optimization of RETURNING data.

## Current Behavior

When DuckDB executes:
```sql
INSERT INTO remote_table (name, email) VALUES ('Alice', 'alice@example.com') RETURNING id
```

The Airport extension sends:
- `airport-operation: insert` header
- `return-chunks: 1` header (boolean flag indicating RETURNING is requested)
- Input data schema: `[name, email]`

**Missing**: The specific columns requested in the RETURNING clause (`id` in this example).

## Investigation

We investigated the Airport extension source code to understand how RETURNING columns are handled:

### Findings from `airport_exchange.cpp`

The `AirportExchangeGetGlobalSinkState` function accepts a `destination_chunk_column_names` parameter:

```cpp
void AirportExchangeGetGlobalSinkState(
    ClientContext &context,
    const TableCatalogEntry &table,
    const AirportTableEntry &airport_table,
    AirportExchangeGlobalState *global_state,
    const ArrowSchema &send_schema,
    const bool return_chunk,
    const string exchange_operation,
    const vector<string> destination_chunk_column_names,  // <-- RETURNING columns
    const std::optional<string> transaction_id)
```

However, this parameter is **only used client-side** for mapping received columns to output positions:

```cpp
for (size_t output_index = 0; output_index < destination_chunk_column_names.size(); output_index++) {
    auto found_index = findIndex(reading_arrow_column_names, destination_chunk_column_names[output_index]);
    // Maps destination column names to arrow schema indices
}
```

The column names are **never transmitted** to the server via headers, metadata, or schema information.

### Findings from `airport_insert.cpp`

The RETURNING column names are tracked separately:

```cpp
vector<string> returning_column_names;
returning_column_names.reserve(table->GetColumns().LogicalColumnCount());
for (auto &cd : table->GetColumns().Logical()) {
    returning_column_names.push_back(cd.GetName());
}
```

These are passed to `AirportExchangeGetGlobalSinkState` but only for client-side column projection after receiving server response.

### Current Protocol Headers

The only headers sent for DML operations are:
- `airport-operation`: "insert", "update", or "delete"
- `airport-flight-path`: "schema/table"
- `return-chunks`: "1" or "0" (boolean flag only)
- `airport-transaction-id`: (if applicable)

**No header exists for RETURNING column names.**

## Impact

Without knowing which columns are requested:

1. **Server returns all columns** - The server must return all available columns, even when only a subset is needed
2. **Increased network bandwidth** - Unnecessary data transfer for large tables with many columns
3. **No server-side query optimization** - Database implementations cannot optimize queries like `SELECT id FROM ... RETURNING id` vs `SELECT * FROM ...`

## Proposed Solution

Add a new header to communicate RETURNING column names:

```
airport-returning-columns: id,created_at
```

Or via Arrow schema metadata on the output schema message.

### Backward Compatibility

- Servers that don't recognize the header continue with current behavior (return all columns)
- Clients that don't send the header get all columns (current behavior)

## Workaround

Currently, servers must return all available columns and rely on DuckDB to filter client-side. This works correctly but is not optimal for:
- Tables with many columns
- Large result sets
- Network-constrained environments

## Environment

- DuckDB Airport Extension: community version
- Protocol: Arrow Flight gRPC

## Related Code

- `airport_exchange.cpp`: `AirportExchangeGetGlobalSinkState()` - handles bidirectional exchange
- `airport_insert.cpp`: `AirportInsertGlobalState` - tracks returning columns
- `airport_update.cpp`: Similar pattern for UPDATE
- `airport_delete.cpp`: Similar pattern for DELETE

## Questions

1. Is there a technical reason why `destination_chunk_column_names` is not transmitted to the server?
2. Would adding an `airport-returning-columns` header be acceptable?
3. Are there alternative approaches being considered for server-side RETURNING optimization?
