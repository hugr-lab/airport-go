# Transaction Management API Contract

**Feature**: 002-dml-transactions
**Version**: 1.0.0
**Date**: 2025-11-28

## Overview

This contract defines the Arrow Flight RPC interface for transaction coordination. Transactions are OPTIONAL - servers operate normally without a TransactionManager configured. When configured, transactions enable coordinated commit/rollback across multiple DML operations.

---

## create_transaction Action

### Protocol: DoAction

**Action Type**: `create_transaction`

**Action Body**: Empty or `{}`

**Request Flow**:
1. Client sends Action with type="create_transaction"
2. Server validates TransactionManager is configured
3. Server creates new transaction via `TransactionManager.BeginTransaction()`
4. Server returns Result with transaction ID

**Result Response** (JSON-encoded body):
```json
{
    "status": "success",
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Error Responses

| Condition | gRPC Code | Example Message |
|-----------|-----------|-----------------|
| No tx manager | UNIMPLEMENTED | "transaction manager not configured" |
| Creation failed | INTERNAL | "failed to create transaction: {details}" |

---

## Using Transactions with DML Operations

### Transaction Header

Include transaction ID in gRPC metadata for all operations within the transaction:

**Header Name**: `x-transaction-id`
**Header Value**: Transaction ID string from create_transaction

### Request Flow with Transaction

1. Client calls `create_transaction` to get transaction ID
2. Client includes `x-transaction-id` header in subsequent DML operations
3. Each DML operation:
   - Extracts transaction ID from context
   - Validates transaction is active
   - Executes operation within transaction scope
   - On success: calls `TransactionManager.CommitTransaction()`
   - On failure: calls `TransactionManager.RollbackTransaction()`

**Note**: In the current implementation, each operation auto-commits on success. Multi-statement transactions (where commit is deferred) are out of scope per spec.

---

## Transaction Status Check

### Protocol: DoAction

**Action Type**: `get_transaction_status`

**Action Body** (JSON-encoded):
```json
{
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Result Response**:
```json
{
    "status": "success",
    "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
    "transaction_status": "active"
}
```

**Transaction Status Values**:
| Status | Description |
|--------|-------------|
| active | Transaction open, awaiting operations |
| committed | Transaction successfully completed |
| aborted | Transaction rolled back |

### Error Responses

| Condition | gRPC Code | Example Message |
|-----------|-----------|-----------------|
| No tx manager | UNIMPLEMENTED | "transaction manager not configured" |
| TX not found | NOT_FOUND | "transaction '550e8400...' not found" |

---

## Transaction Lifecycle

```
Client                          Server (Flight)                 TransactionManager
   │                                  │                                  │
   │  DoAction(create_transaction)    │                                  │
   ├─────────────────────────────────►│                                  │
   │                                  │  BeginTransaction()              │
   │                                  ├─────────────────────────────────►│
   │                                  │                    txID          │
   │                                  │◄─────────────────────────────────┤
   │       {transaction_id: txID}     │                                  │
   │◄─────────────────────────────────┤                                  │
   │                                  │                                  │
   │  DoPut(INSERT) + x-tx-id: txID   │                                  │
   ├─────────────────────────────────►│                                  │
   │                                  │  [extract tx from context]       │
   │                                  │  [execute INSERT]                │
   │                                  │                                  │
   │                                  │  CommitTransaction(txID)         │
   │                                  ├─────────────────────────────────►│
   │                                  │◄─────────────────────────────────┤
   │       {affected_rows: N}         │                                  │
   │◄─────────────────────────────────┤                                  │
   │                                  │                                  │
```

### Error Case (Rollback)

```
Client                          Server (Flight)                 TransactionManager
   │                                  │                                  │
   │  DoPut(INSERT) + x-tx-id: txID   │                                  │
   ├─────────────────────────────────►│                                  │
   │                                  │  [execute INSERT]                │
   │                                  │  [ERROR: schema mismatch]        │
   │                                  │                                  │
   │                                  │  RollbackTransaction(txID)       │
   │                                  ├─────────────────────────────────►│
   │                                  │◄─────────────────────────────────┤
   │       INVALID_ARGUMENT           │                                  │
   │◄─────────────────────────────────┤                                  │
   │                                  │                                  │
```

---

## Go Client Example

```go
import (
    "context"
    "encoding/json"

    "github.com/apache/arrow-go/v18/arrow/flight"
    "google.golang.org/grpc/metadata"
)

// Create a new transaction
func createTransaction(ctx context.Context, client flight.FlightServiceClient) (string, error) {
    action := &flight.Action{
        Type: "create_transaction",
        Body: []byte("{}"),
    }

    stream, err := client.DoAction(ctx, action)
    if err != nil {
        return "", err
    }

    result, err := stream.Recv()
    if err != nil {
        return "", err
    }

    var response struct {
        Status        string `json:"status"`
        TransactionID string `json:"transaction_id"`
    }
    json.Unmarshal(result.Body, &response)

    return response.TransactionID, nil
}

// Execute operation within transaction
func executeInTransaction(ctx context.Context, txID string, fn func(context.Context) error) error {
    // Add transaction header to context
    ctx = metadata.AppendToOutgoingContext(ctx, "x-transaction-id", txID)
    return fn(ctx)
}

// Check transaction status
func getTransactionStatus(ctx context.Context, client flight.FlightServiceClient,
    txID string) (string, error) {

    action := &flight.Action{
        Type: "get_transaction_status",
        Body: mustJSON(map[string]string{"transaction_id": txID}),
    }

    stream, err := client.DoAction(ctx, action)
    if err != nil {
        return "", err
    }

    result, err := stream.Recv()
    if err != nil {
        return "", err
    }

    var response struct {
        TransactionStatus string `json:"transaction_status"`
    }
    json.Unmarshal(result.Body, &response)

    return response.TransactionStatus, nil
}

// Full example: coordinated operations
func coordinatedOperations(ctx context.Context, client flight.FlightServiceClient) error {
    // Start transaction
    txID, err := createTransaction(ctx, client)
    if err != nil {
        return fmt.Errorf("failed to create transaction: %w", err)
    }

    // Execute operations with transaction context
    err = executeInTransaction(ctx, txID, func(txCtx context.Context) error {
        // INSERT operation
        if _, err := insertRows(txCtx, client, "schema", "table", data); err != nil {
            return err
        }

        // DELETE operation
        if _, err := deleteRows(txCtx, client, "schema", "table", rowIDs); err != nil {
            return err
        }

        return nil
    })

    if err != nil {
        // Transaction was automatically rolled back by server
        return fmt.Errorf("transaction failed: %w", err)
    }

    // Verify transaction committed
    status, _ := getTransactionStatus(ctx, client, txID)
    if status != "committed" {
        return fmt.Errorf("unexpected transaction status: %s", status)
    }

    return nil
}
```

---

## Server Implementation Notes

### TransactionManager Configuration

```go
// In server configuration
config := airport.ServerConfig{
    Catalog: myCatalog,
    TransactionManager: myTransactionManager, // Optional
}
```

### Context Helpers

Application code can check transaction state:

```go
import "github.com/hugr-lab/airport-go/catalog"

func myHandler(ctx context.Context) error {
    // Check if running in transaction
    txID, hasTx := catalog.TransactionIDFromContext(ctx)
    if hasTx {
        log.Printf("Operating within transaction: %s", txID)
    }

    // ... operation logic ...
}
```

---

## Limitations

Per feature spec (Out of Scope):
- Multi-statement transaction batching (each operation auto-commits)
- Transaction isolation level configuration
- Savepoint support
- Distributed transactions across servers
- Transaction timeout enforcement (delegated to TransactionManager implementation)
