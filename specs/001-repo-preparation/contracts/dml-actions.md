# DML Operations Contract

**Version**: 1.0.0 | **Date**: 2025-11-26
**Protocol**: Apache Arrow Flight RPC (DoPut for INSERT/UPDATE, DoAction for DELETE)

## Overview

DML (Data Manipulation Language) operations modify table data using Arrow RecordBatch streaming for efficient columnar data transfer.

## Operation: INSERT

**Purpose**: Insert rows into a table

**Protocol**: Flight DoPut RPC

**Descriptor**:
```json
{
  "type": "CMD",
  "cmd": "{\"action\": \"insert\", \"schema_name\": \"string\", \"table_name\": \"string\"}"
}
```

**Data Format**: Arrow RecordBatch stream
- Schema must match table schema exactly
- Multiple batches supported for streaming large datasets
- Batch size recommendation: 1000-10000 rows

**Response**:
```json
{
  "rows_inserted": "integer"
}
```

**Error Codes**:
- `InvalidArgument`: Schema mismatch, invalid data types
- `NotFound`: Table doesn't exist
- `Internal`: Storage error

**Example**:
```go
// Create FlightDescriptor
descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorCMD,
    Cmd:  []byte(`{"action":"insert","schema_name":"analytics","table_name":"events"}`),
}

// Create record batch with data
schema := arrow.NewSchema([]arrow.Field{
    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
    {Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
    {Name: "event_type", Type: arrow.BinaryTypes.String},
}, nil)

builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
// ... append other fields

record := builder.NewRecord()
defer record.Release()

// Send via DoPut
stream, err := client.DoPut(ctx)
err = stream.Send(&flight.FlightData{
    FlightDescriptor: descriptor,
    DataBody:         flight.SerializeRecord(record, memory.DefaultAllocator),
})
putResult, err := stream.CloseAndRecv()
```

---

## Operation: UPDATE

**Purpose**: Update existing rows in a table

**Protocol**: Flight DoPut RPC

**Descriptor**:
```json
{
  "type": "CMD",
  "cmd": "{\"action\": \"update\", \"schema_name\": \"string\", \"table_name\": \"string\", \"row_ids\": [int64...]}"
}
```

**Data Format**: Arrow RecordBatch stream
- Schema contains columns to update (partial updates supported)
- Number of rows in batch must match length of `row_ids` array
- Row order corresponds to `row_ids` order

**Response**:
```json
{
  "rows_updated": "integer"
}
```

**Error Codes**:
- `InvalidArgument`: Schema mismatch, row count mismatch with row_ids
- `NotFound`: Invalid rowid in row_ids array
- `Internal`: Storage error

**Example**:
```go
descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorCMD,
    Cmd:  []byte(`{"action":"update","schema_name":"analytics","table_name":"events","row_ids":[1,2,3]}`),
}

// Create batch with updated column values
schema := arrow.NewSchema([]arrow.Field{
    {Name: "event_type", Type: arrow.BinaryTypes.String},
}, nil)

builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
builder.Field(0).(*array.StringBuilder).AppendValues([]string{"click", "view", "purchase"}, nil)

record := builder.NewRecord()
defer record.Release()

stream, err := client.DoPut(ctx)
err = stream.Send(&flight.FlightData{
    FlightDescriptor: descriptor,
    DataBody:         flight.SerializeRecord(record, memory.DefaultAllocator),
})
putResult, err := stream.CloseAndRecv()
```

---

## Operation: DELETE

**Purpose**: Delete rows from a table

**Protocol**: Flight DoAction RPC

**Action Type**: `"Delete"`

**Request Payload**:
```json
{
  "schema_name": "string (required)",
  "table_name": "string (required)",
  "row_ids": "array of int64 (required)"
}
```

**Response**:
```json
{
  "status": "success",
  "rows_deleted": "integer"
}
```

**Error Codes**:
- `InvalidArgument`: Invalid JSON, missing fields, empty row_ids
- `NotFound`: Invalid rowid (optional strict mode)
- `Internal`: Storage error

**Example**:
```go
action := &flight.Action{
    Type: "Delete",
    Body: []byte(`{
        "schema_name": "analytics",
        "table_name": "events",
        "row_ids": [1, 2, 3, 4, 5]
    }`),
}
stream, err := client.DoAction(ctx, action)
```

---

## Row ID (rowid) Pseudocolumn

All tables have an implicit `rowid` pseudocolumn for identifying rows:

- **Type**: int64
- **Uniqueness**: Unique within table (not globally unique)
- **Stability**: Stable for row lifetime (survives updates)
- **Generation**: Implementation-defined (auto-increment, UUID, etc.)
- **Visibility**: Queryable via SELECT but not part of table schema

**Querying rowid**:
```sql
-- DuckDB Airport extension
SELECT rowid, * FROM airport_catalog.analytics.events WHERE event_type = 'click';
```

---

## Schema Compatibility Rules

### INSERT
- RecordBatch schema MUST match table schema exactly
- All non-nullable columns MUST be present
- Column order can differ (matched by name)
- Extra columns in batch: Error
- Missing required columns: Error

### UPDATE
- RecordBatch schema can be partial (subset of table columns)
- Only specified columns are updated
- Unspecified columns retain existing values
- Column types must match table schema
- Extra columns in batch: Error

### Type Compatibility
- Exact type match required (no implicit conversions)
- Nullable columns accept NULL values
- Non-nullable columns reject NULL values

---

## Performance Considerations

### Batch Sizing
- **Small batches** (100-1000 rows): Lower latency, more RPC overhead
- **Medium batches** (1000-10000 rows): Balanced (recommended)
- **Large batches** (10000-100000 rows): Higher throughput, more memory

### Streaming
- DoPut supports streaming multiple batches
- Client should stream large datasets instead of single large batch
- Server processes batches incrementally

### Memory Management
- Always Release() Arrow objects after use
- Use `memory.CheckedAllocator` in tests to detect leaks

**Example streaming insert**:
```go
stream, err := client.DoPut(ctx)

for batch := range dataSource {
    record := buildRecord(batch)
    defer record.Release()

    err := stream.Send(&flight.FlightData{
        FlightDescriptor: descriptor,
        DataBody:         flight.SerializeRecord(record, allocator),
    })
    if err != nil {
        return err
    }
}

result, err := stream.CloseAndRecv()
fmt.Printf("Inserted %d rows\n", result.RowsInserted)
```

---

## Testing

Each DML operation requires:
1. **Unit tests**: Descriptor parsing, schema validation
2. **Integration tests**: End-to-end DoPut/DoAction with data verification
3. **Scale tests**: 100M row operations (benchmark, not in regular CI)
4. **Memory tests**: No leaks with `memory.CheckedAllocator`

**Example Integration Test**:
```go
func TestInsertOperation(t *testing.T) {
    server := startTestServer(t)
    defer server.Stop()

    client := connectFlightClient(t, server.Address())
    ctx := context.Background()

    // Create table first
    createTable(t, client, "test", "users")

    // Insert data
    descriptor := &flight.FlightDescriptor{
        Type: flight.DescriptorCMD,
        Cmd:  []byte(`{"action":"insert","schema_name":"test","table_name":"users"}`),
    }

    schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
    }, nil)

    builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
    builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
    builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

    record := builder.NewRecord()
    defer record.Release()

    stream, err := client.DoPut(ctx)
    require.NoError(t, err)

    err = stream.Send(&flight.FlightData{
        FlightDescriptor: descriptor,
        DataBody:         flight.SerializeRecord(record, memory.DefaultAllocator),
    })
    require.NoError(t, err)

    result, err := stream.CloseAndRecv()
    require.NoError(t, err)
    assert.Equal(t, int64(3), result.RowsInserted)

    // Verify data
    verifyTableData(t, client, "test", "users", 3)
}
```

## Compatibility

This contract aligns with DuckDB Airport extension DML operations. Clients using DuckDB 1.4+ can generate these operations automatically via SQL INSERT/UPDATE/DELETE statements.

