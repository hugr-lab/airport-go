# DoAction Contract: column_statistics

**Protocol**: Arrow Flight RPC
**Action Type**: `column_statistics`

## Request

### Action Structure

```protobuf
message Action {
  string type = 1;  // "column_statistics"
  bytes body = 2;   // msgpack-encoded ColumnStatisticsParams
}
```

### Request Body (msgpack)

```go
type ColumnStatisticsParams struct {
    FlightDescriptor []byte `msgpack:"flight_descriptor"` // Serialized Arrow FlightDescriptor
    ColumnName       string `msgpack:"column_name"`       // Column name
    Type             string `msgpack:"type"`              // DuckDB type name
}
```

**Example** (conceptual):
```json
{
  "flight_descriptor": "<protobuf bytes>",
  "column_name": "id",
  "type": "INTEGER"
}
```

## Response

### Success Response

Single `Result` message containing Arrow IPC-serialized RecordBatch.

```protobuf
message Result {
  bytes body = 1;  // Arrow IPC serialized RecordBatch
}
```

### RecordBatch Schema

| Column | Arrow Type | Description |
|--------|------------|-------------|
| `has_not_null` | `Boolean` (nullable) | Non-null values exist |
| `has_null` | `Boolean` (nullable) | Null values exist |
| `distinct_count` | `Uint64` (nullable) | Unique value count |
| `min` | *varies* (nullable) | Minimum value |
| `max` | *varies* (nullable) | Maximum value |
| `max_string_length` | `Uint64` (nullable) | Max string length |
| `contains_unicode` | `Boolean` (nullable) | Unicode presence |

**Note**: `min` and `max` types match the queried column's Arrow type.

### Error Responses

| Condition | gRPC Code | Message Pattern |
|-----------|-----------|-----------------|
| Table doesn't support statistics | `UNIMPLEMENTED` | "table does not support statistics" |
| Column not found | `NOT_FOUND` | "column \"{name}\" not found" |
| Invalid request | `INVALID_ARGUMENT` | "invalid column_statistics payload: ..." |
| Schema not found | `NOT_FOUND` | "schema \"{name}\" not found" |
| Table not found | `NOT_FOUND` | "table \"{name}\" not found" |
| Internal error | `INTERNAL` | "failed to compute statistics: ..." |

## Examples

### Successful Request Flow

```
Client → Server: DoAction(type="column_statistics", body=<msgpack params>)
Server → Client: Result(body=<Arrow IPC RecordBatch>)
```

### Error Flow (Unimplemented)

```
Client → Server: DoAction(type="column_statistics", body=<msgpack params>)
Server → Client: Error(code=UNIMPLEMENTED, message="table does not support statistics")
```

## DuckDB Integration

DuckDB calls this action during query planning:

```sql
-- This query may trigger column_statistics for 'id' column
SELECT * FROM demo.main.users WHERE id > 100;
```

The optimizer uses statistics to:
1. Estimate row counts for filter predicates
2. Choose optimal join algorithms
3. Determine index usage
