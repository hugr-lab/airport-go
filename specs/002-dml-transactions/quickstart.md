# Quickstart: DML Operations and Transaction Management

**Feature**: 002-dml-transactions
**Date**: 2025-11-28

## Prerequisites

- Go 1.25+
- airport-go library installed
- Basic understanding of Arrow Flight RPC

## Implementing DML-Capable Tables

### Step 1: Define a Table with INSERT Support

```go
package main

import (
    "context"
    "sync"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/hugr-lab/airport-go/catalog"
)

// MyTable implements catalog.Table and catalog.InsertableTable
type MyTable struct {
    name   string
    schema *arrow.Schema
    data   []arrow.RecordBatch
    mu     sync.RWMutex
    alloc  memory.Allocator
}

// Table interface methods
func (t *MyTable) Name() string           { return t.name }
func (t *MyTable) Comment() string        { return "A writable table" }
func (t *MyTable) ArrowSchema() *arrow.Schema { return t.schema }

func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    t.mu.RLock()
    defer t.mu.RUnlock()
    // Return a reader over stored data
    return array.NewRecordSliceReader(t.schema, t.data), nil
}

// InsertableTable interface method
func (t *MyTable) Insert(ctx context.Context, rows array.RecordReader) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    var totalRows int64
    for rows.Next() {
        record := rows.RecordBatch()
        record.Retain() // Keep reference
        t.data = append(t.data, record)
        totalRows += record.NumRows()
    }

    if err := rows.Err(); err != nil {
        return nil, err
    }

    return &catalog.DMLResult{AffectedRows: totalRows}, nil
}
```

### Step 2: Add UPDATE and DELETE Support

```go
// UpdatableTable interface method
func (t *MyTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    // Build a map of rowID -> new data
    updates := make(map[int64]arrow.RecordBatch)
    idx := 0
    for rows.Next() {
        if idx >= len(rowIDs) {
            return nil, fmt.Errorf("more rows than row_ids")
        }
        record := rows.RecordBatch()
        record.Retain()
        updates[rowIDs[idx]] = record
        idx++
    }

    // Apply updates (simplified - real implementation tracks rowids)
    affected := int64(len(updates))

    return &catalog.DMLResult{AffectedRows: affected}, nil
}

// DeletableTable interface method
func (t *MyTable) Delete(ctx context.Context, rowIDs []int64) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    // Remove rows by rowid (simplified)
    rowSet := make(map[int64]bool)
    for _, id := range rowIDs {
        rowSet[id] = true
    }

    affected := int64(len(rowIDs))

    return &catalog.DMLResult{AffectedRows: affected}, nil
}
```

### Step 3: Register Table in Catalog

```go
func main() {
    // Create table schema
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
        {Name: "value", Type: arrow.PrimitiveTypes.Float64},
    }, nil)

    // Create writable table
    myTable := &MyTable{
        name:   "events",
        schema: schema,
        data:   make([]arrow.RecordBatch, 0),
        alloc:  memory.DefaultAllocator,
    }

    // Build catalog with writable table
    cat := catalog.NewStaticCatalogBuilder().
        WithSchema("analytics", "Analytics schema").
        WithTable("analytics", myTable).
        Build()

    // Create server
    grpcServer := grpc.NewServer()
    airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog: cat,
    })

    // Start serving
    lis, _ := net.Listen("tcp", ":8815")
    grpcServer.Serve(lis)
}
```

---

## Implementing Transaction Manager

### Step 1: Create TransactionManager Implementation

```go
package main

import (
    "context"
    "sync"

    "github.com/google/uuid"
    "github.com/hugr-lab/airport-go/catalog"
)

type InMemoryTxManager struct {
    transactions map[string]catalog.TransactionState
    mu           sync.RWMutex
}

func NewInMemoryTxManager() *InMemoryTxManager {
    return &InMemoryTxManager{
        transactions: make(map[string]catalog.TransactionState),
    }
}

func (m *InMemoryTxManager) BeginTransaction(ctx context.Context) (string, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    txID := uuid.New().String()
    m.transactions[txID] = catalog.TransactionActive
    return txID, nil
}

func (m *InMemoryTxManager) CommitTransaction(ctx context.Context, txID string) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if state, exists := m.transactions[txID]; !exists || state != catalog.TransactionActive {
        return nil // Idempotent
    }
    m.transactions[txID] = catalog.TransactionCommitted
    return nil
}

func (m *InMemoryTxManager) RollbackTransaction(ctx context.Context, txID string) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if state, exists := m.transactions[txID]; !exists || state != catalog.TransactionActive {
        return nil // Idempotent
    }
    m.transactions[txID] = catalog.TransactionAborted
    return nil
}

func (m *InMemoryTxManager) GetTransactionStatus(ctx context.Context, txID string) (catalog.TransactionState, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    state, exists := m.transactions[txID]
    return state, exists
}
```

### Step 2: Configure Server with TransactionManager

```go
func main() {
    cat := buildCatalog() // Your catalog setup
    txManager := NewInMemoryTxManager()

    grpcServer := grpc.NewServer()
    airport.NewServer(grpcServer, airport.ServerConfig{
        Catalog:            cat,
        TransactionManager: txManager, // Enable transaction support
    })

    lis, _ := net.Listen("tcp", ":8815")
    grpcServer.Serve(lis)
}
```

---

## Client Usage

### INSERT Operation

```go
import (
    "context"
    "encoding/json"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/flight"
    "github.com/vmihailenco/msgpack/v5"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func insertData(ctx context.Context, addr string) error {
    // Connect
    conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return err
    }
    defer conn.Close()
    client := flight.NewFlightServiceClient(conn)

    // Build INSERT descriptor
    desc := map[string]any{
        "operation":   "insert",
        "schema_name": "analytics",
        "table_name":  "events",
    }
    descBytes, _ := json.Marshal(desc)

    // Create DoPut stream
    stream, err := client.DoPut(ctx)
    if err != nil {
        return err
    }

    // Send descriptor
    err = stream.Send(&flight.FlightData{
        FlightDescriptor: &flight.FlightDescriptor{
            Type: flight.DescriptorCMD,
            Cmd:  descBytes,
        },
    })
    if err != nil {
        return err
    }

    // Send data batches (example with Arrow record)
    // ... write your RecordBatch data ...

    // Get result
    result, err := stream.CloseAndRecv()
    if err != nil {
        return err
    }

    // Parse response
    var response struct {
        Status       string `msgpack:"status"`
        AffectedRows int64  `msgpack:"affected_rows"`
    }
    msgpack.Unmarshal(result.AppMetadata, &response)

    fmt.Printf("Inserted %d rows\n", response.AffectedRows)
    return nil
}
```

### DELETE Operation

```go
func deleteData(ctx context.Context, addr string, rowIDs []int64) error {
    conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    defer conn.Close()
    client := flight.NewFlightServiceClient(conn)

    // Build DELETE action
    body, _ := json.Marshal(map[string]any{
        "schema_name": "analytics",
        "table_name":  "events",
        "row_ids":     rowIDs,
    })

    action := &flight.Action{
        Type: "delete",
        Body: body,
    }

    // Execute
    stream, err := client.DoAction(ctx, action)
    if err != nil {
        return err
    }

    result, err := stream.Recv()
    if err != nil {
        return err
    }

    var response struct {
        Status       string `json:"status"`
        AffectedRows int64  `json:"affected_rows"`
    }
    json.Unmarshal(result.Body, &response)

    fmt.Printf("Deleted %d rows\n", response.AffectedRows)
    return nil
}
```

### Using Transactions

```go
func transactionalOperations(ctx context.Context, addr string) error {
    conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    defer conn.Close()
    client := flight.NewFlightServiceClient(conn)

    // 1. Create transaction
    txAction := &flight.Action{
        Type: "create_transaction",
        Body: []byte("{}"),
    }

    stream, err := client.DoAction(ctx, txAction)
    if err != nil {
        return err
    }

    result, _ := stream.Recv()
    var txResponse struct {
        TransactionID string `json:"transaction_id"`
    }
    json.Unmarshal(result.Body, &txResponse)

    txID := txResponse.TransactionID
    fmt.Printf("Created transaction: %s\n", txID)

    // 2. Execute operations with transaction context
    ctx = metadata.AppendToOutgoingContext(ctx, "x-transaction-id", txID)

    // INSERT with transaction
    if err := insertData(ctx, addr); err != nil {
        // Transaction auto-rolled back on error
        return fmt.Errorf("insert failed: %w", err)
    }

    // DELETE with transaction
    if err := deleteData(ctx, addr, []int64{1, 2, 3}); err != nil {
        return fmt.Errorf("delete failed: %w", err)
    }

    // 3. Check transaction status
    statusAction := &flight.Action{
        Type: "get_transaction_status",
        Body: mustJSON(map[string]string{"transaction_id": txID}),
    }
    stream, _ = client.DoAction(ctx, statusAction)
    result, _ = stream.Recv()

    var statusResponse struct {
        TransactionStatus string `json:"transaction_status"`
    }
    json.Unmarshal(result.Body, &statusResponse)

    fmt.Printf("Transaction status: %s\n", statusResponse.TransactionStatus)
    return nil
}
```

---

## Error Handling

```go
import (
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

func handleDMLError(err error) {
    st, ok := status.FromError(err)
    if !ok {
        fmt.Printf("Unknown error: %v\n", err)
        return
    }

    switch st.Code() {
    case codes.NotFound:
        fmt.Printf("Resource not found: %s\n", st.Message())
    case codes.FailedPrecondition:
        fmt.Printf("Operation not supported: %s\n", st.Message())
    case codes.InvalidArgument:
        fmt.Printf("Invalid input: %s\n", st.Message())
    case codes.Unimplemented:
        fmt.Printf("Feature not available: %s\n", st.Message())
    default:
        fmt.Printf("Error (%s): %s\n", st.Code(), st.Message())
    }
}
```

---

## Checking Table Capabilities

```go
// From within your Table implementation or catalog code
func checkCapabilities(table catalog.Table) {
    fmt.Printf("Table: %s\n", table.Name())

    if _, ok := table.(catalog.InsertableTable); ok {
        fmt.Println("  - Supports INSERT")
    }

    if _, ok := table.(catalog.UpdatableTable); ok {
        fmt.Println("  - Supports UPDATE")
    }

    if _, ok := table.(catalog.DeletableTable); ok {
        fmt.Println("  - Supports DELETE")
    }
}
```

---

## Next Steps

1. See [DML Operations Contract](./contracts/dml-operations.md) for full API details
2. See [Transaction Management Contract](./contracts/transaction-management.md) for transaction API
3. Check `tests/integration/dml_test.go` for comprehensive test examples
