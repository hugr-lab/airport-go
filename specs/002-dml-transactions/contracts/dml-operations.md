# DML Operations API Contract

**Feature**: 002-dml-transactions
**Version**: 1.0.0
**Date**: 2025-11-28

## Overview

This contract defines the Arrow Flight RPC interface for DML (Data Manipulation Language) operations: INSERT, UPDATE, and DELETE. Operations use DoPut for INSERT/UPDATE (streaming data) and DoAction for DELETE (no streaming required).

---

## INSERT Operation

### Protocol: DoPut

**Flight Descriptor** (CMD type, JSON-encoded):
```json
{
    "operation": "insert",
    "schema_name": "analytics",
    "table_name": "events",
    "returning": false
}
```

**Request Flow**:
1. Client sends FlightDescriptor in first FlightData message
2. Client streams Arrow RecordBatch messages with rows to insert
3. Server validates schema matches table
4. Server inserts rows via `InsertableTable.Insert()`
5. Server returns PutResult with MessagePack-encoded response

**PutResult Response** (MessagePack-encoded app_metadata):
```json
{
    "status": "success",
    "affected_rows": 1000,
    "returning_data": null
}
```

**With RETURNING Clause**:
```json
{
    "operation": "insert",
    "schema_name": "analytics",
    "table_name": "events",
    "returning": true
}
```

Response includes serialized Arrow IPC RecordBatch:
```json
{
    "status": "success",
    "affected_rows": 1000,
    "returning_data": "<base64-encoded Arrow IPC bytes>"
}
```

### Error Responses

| Condition | gRPC Code | Example Message |
|-----------|-----------|-----------------|
| Schema not found | NOT_FOUND | "schema 'analytics' not found" |
| Table not found | NOT_FOUND | "table 'analytics.events' not found" |
| Table read-only | FAILED_PRECONDITION | "table 'events' does not support INSERT operations" |
| Schema mismatch | INVALID_ARGUMENT | "column mismatch: table has 5 columns, input has 4" |

---

## UPDATE Operation

### Protocol: DoPut

**Flight Descriptor** (CMD type, JSON-encoded):
```json
{
    "operation": "update",
    "schema_name": "analytics",
    "table_name": "events",
    "row_ids": [1, 5, 12, 99],
    "returning": false
}
```

**Request Flow**:
1. Client sends FlightDescriptor with row_ids identifying rows to update
2. Client streams Arrow RecordBatch messages with replacement data
3. Row count in RecordBatch must match row_ids count
4. Server validates table implements `UpdatableTable`
5. Server updates rows via `UpdatableTable.Update()`
6. Server returns PutResult with response

**PutResult Response**:
```json
{
    "status": "success",
    "affected_rows": 4,
    "returning_data": null
}
```

**With RETURNING Clause**:
```json
{
    "operation": "update",
    "schema_name": "analytics",
    "table_name": "events",
    "row_ids": [1, 5, 12, 99],
    "returning": true
}
```

### Error Responses

| Condition | gRPC Code | Example Message |
|-----------|-----------|-----------------|
| Empty row_ids | INVALID_ARGUMENT | "row_ids cannot be empty for UPDATE" |
| Row count mismatch | INVALID_ARGUMENT | "row_ids count (4) does not match input rows (3)" |
| No rowid support | FAILED_PRECONDITION | "table 'events' does not support UPDATE (no rowid)" |
| Row not found | NOT_FOUND | "row_id 99 not found in table" |

---

## DELETE Operation

### Protocol: DoAction

**Action Type**: `delete`

**Action Body** (JSON-encoded):
```json
{
    "schema_name": "analytics",
    "table_name": "events",
    "row_ids": [1, 5, 12, 99],
    "returning": false
}
```

**Request Flow**:
1. Client sends Action with type="delete" and JSON body
2. Server validates table implements `DeletableTable`
3. Server deletes rows via `DeletableTable.Delete()`
4. Server returns Result stream with response

**Result Response** (JSON-encoded body):
```json
{
    "status": "success",
    "affected_rows": 4
}
```

**With RETURNING Clause**:
```json
{
    "schema_name": "analytics",
    "table_name": "events",
    "row_ids": [1, 5, 12, 99],
    "returning": true
}
```

Response includes serialized Arrow IPC:
```json
{
    "status": "success",
    "affected_rows": 4,
    "returning_data": "<base64-encoded Arrow IPC bytes>"
}
```

### Error Responses

| Condition | gRPC Code | Example Message |
|-----------|-----------|-----------------|
| Empty row_ids | INVALID_ARGUMENT | "row_ids cannot be empty for DELETE" |
| No rowid support | FAILED_PRECONDITION | "table 'events' does not support DELETE (no rowid)" |

---

## Common Headers

All DML operations support optional transaction context via gRPC metadata:

| Header | Type | Description |
|--------|------|-------------|
| x-transaction-id | string | Transaction ID from create_transaction action |

**Example** (gRPC metadata):
```
x-transaction-id: 550e8400-e29b-41d4-a716-446655440000
```

When present:
- Operation executes within transaction scope
- Success triggers automatic commit (if configured)
- Failure triggers automatic rollback (if configured)

---

## Schema Requirements

### INSERT
- Input RecordBatch schema must match target table schema exactly
- Column names and types must align
- Nullable columns can accept null values

### UPDATE
- Input RecordBatch schema must match target table schema (excluding internal rowid)
- Row count must equal row_ids count
- Rows are matched by position: row_ids[0] receives data from row 0, etc.

### DELETE
- No data schema requirements (row_ids only)
- row_ids reference internal rowid pseudocolumn

---

## Go Client Example

```go
import (
    "context"
    "encoding/json"

    "github.com/apache/arrow-go/v18/arrow/flight"
    "github.com/apache/arrow-go/v18/arrow/ipc"
    "google.golang.org/grpc/metadata"
)

// INSERT example
func insertRows(ctx context.Context, client flight.FlightServiceClient,
    schema, table string, data arrow.RecordBatch) (int64, error) {

    // Build descriptor
    desc := map[string]interface{}{
        "operation": "insert",
        "schema_name": schema,
        "table_name": table,
    }
    descBytes, _ := json.Marshal(desc)

    // Create DoPut stream
    stream, err := client.DoPut(ctx)
    if err != nil {
        return 0, err
    }

    // Send descriptor in first message
    writer := flight.NewRecordBatchStreamWriter(stream, ipc.WithSchema(data.Schema()))
    defer writer.Close()

    // Write data
    if err := writer.Write(data); err != nil {
        return 0, err
    }

    // Get response
    result, err := stream.CloseAndRecv()
    if err != nil {
        return 0, err
    }

    // Parse result
    var response struct {
        Status       string `msgpack:"status"`
        AffectedRows int64  `msgpack:"affected_rows"`
    }
    msgpack.Decode(result.AppMetadata, &response)
    return response.AffectedRows, nil
}

// DELETE example with transaction
func deleteWithTx(ctx context.Context, client flight.FlightServiceClient,
    txID, schema, table string, rowIDs []int64) (int64, error) {

    // Add transaction header
    ctx = metadata.AppendToOutgoingContext(ctx, "x-transaction-id", txID)

    // Build action
    action := &flight.Action{
        Type: "delete",
        Body: mustJSON(map[string]interface{}{
            "schema_name": schema,
            "table_name": table,
            "row_ids": rowIDs,
        }),
    }

    // Execute
    stream, err := client.DoAction(ctx, action)
    if err != nil {
        return 0, err
    }

    // Get result
    result, err := stream.Recv()
    if err != nil {
        return 0, err
    }

    var response struct {
        Status       string `json:"status"`
        AffectedRows int64  `json:"affected_rows"`
    }
    json.Unmarshal(result.Body, &response)
    return response.AffectedRows, nil
}
```
