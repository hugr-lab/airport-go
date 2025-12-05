// Package main demonstrates an Airport Flight server with DML (INSERT, UPDATE, DELETE) support
// and transaction management with rollback capability.
//
// To test with DuckDB CLI:
//
//	duckdb
//	INSTALL airport FROM community;
//	LOAD airport;
//	ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
//
//	-- Basic DML operations:
//	INSERT INTO demo.test.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
//	SELECT * FROM demo.test.users;
//	UPDATE demo.test.users SET name = 'Alicia' WHERE id = 1;
//	DELETE FROM demo.test.users WHERE id = 1;
//
//	-- Transaction with rollback (changes are discarded on error):
//	BEGIN TRANSACTION;
//	INSERT INTO demo.test.users (id, name, email) VALUES (100, 'TxUser', 'tx@example.com');
//	SELECT * FROM demo.test.users;  -- Shows TxUser
//	ROLLBACK;
//	SELECT * FROM demo.test.users;  -- TxUser is gone
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Create a writable in-memory table with transaction support
	table := NewUsersTable()

	// Create transaction manager for rollback support
	txManager := NewInMemoryTransactionManager(table)

	// Build catalog with the DML-capable table
	cat, err := airport.NewCatalogBuilder().
		Schema("test").
		Table(table).
		Build()
	if err != nil {
		log.Fatalf("Failed to build catalog: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register Airport handlers with transaction manager
	debugLevel := slog.LevelDebug
	err = airport.NewServer(grpcServer, airport.ServerConfig{
		Catalog:            cat,
		TransactionManager: txManager,
		LogLevel:           &debugLevel,
	})
	if err != nil {
		log.Fatalf("Failed to register Airport server: %v", err)
	}

	// Start serving
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Airport DML server listening on :50051")
	log.Println("Example catalog contains:")
	log.Println("  - Schema: test")
	log.Println("    - Table: users (writable with transaction support)")
	log.Println("")
	log.Println("Test with DuckDB CLI:")
	log.Println("  ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');")
	log.Println("")
	log.Println("  -- Basic DML:")
	log.Println("  INSERT INTO demo.test.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');")
	log.Println("  SELECT * FROM demo.test.users;")
	log.Println("")
	log.Println("  -- Transaction with rollback:")
	log.Println("  BEGIN TRANSACTION;")
	log.Println("  INSERT INTO demo.test.users (id, name, email) VALUES (100, 'TxUser', 'tx@example.com');")
	log.Println("  ROLLBACK;  -- Changes are discarded")
	log.Println("  SELECT * FROM demo.test.users;  -- TxUser is gone")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// =============================================================================
// Transaction Manager Implementation
// =============================================================================

// InMemoryTransactionManager implements catalog.TransactionManager with rollback support.
// It tracks pending changes per transaction and applies/discards them on commit/rollback.
type InMemoryTransactionManager struct {
	mu           sync.RWMutex
	transactions map[string]*txState
	table        *UsersTable
}

type txState struct {
	status   catalog.TransactionState
	snapshot [][]any // Copy of table data at transaction start
}

func NewInMemoryTransactionManager(table *UsersTable) *InMemoryTransactionManager {
	tm := &InMemoryTransactionManager{
		transactions: make(map[string]*txState),
		table:        table,
	}
	table.txManager = tm

	return tm
}

func (m *InMemoryTransactionManager) BeginTransaction(_ context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txID := uuid.New().String()

	// Take snapshot of current table state for rollback
	m.table.mu.RLock()
	snapshot := make([][]any, len(m.table.data))
	for i, row := range m.table.data {
		snapshot[i] = make([]any, len(row))
		copy(snapshot[i], row)
	}
	m.table.mu.RUnlock()

	m.transactions[txID] = &txState{
		status:   catalog.TransactionActive,
		snapshot: snapshot,
	}

	fmt.Printf("[TxManager] BEGIN transaction %s (snapshot: %d rows)\n", txID[:8], len(snapshot))
	return txID, nil
}

func (m *InMemoryTransactionManager) CommitTransaction(_ context.Context, txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.transactions[txID]
	if !ok {
		return errors.New("transaction not found")
	}

	if tx.status != catalog.TransactionActive {
		return nil // Already committed or rolled back
	}

	tx.status = catalog.TransactionCommitted
	tx.snapshot = nil // Release snapshot memory

	fmt.Printf("[TxManager] COMMIT transaction %s\n", txID[:8])
	return nil
}

func (m *InMemoryTransactionManager) RollbackTransaction(_ context.Context, txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.transactions[txID]
	if !ok {
		return errors.New("transaction not found")
	}

	if tx.status != catalog.TransactionActive {
		return nil // Already committed or rolled back
	}

	// Restore table to snapshot state
	m.table.mu.Lock()
	m.table.data = tx.snapshot
	m.table.mu.Unlock()

	tx.status = catalog.TransactionAborted
	tx.snapshot = nil

	fmt.Printf("[TxManager] ROLLBACK transaction %s (restored snapshot)\n", txID[:8])
	return nil
}

func (m *InMemoryTransactionManager) GetTransactionStatus(_ context.Context, txID string) (catalog.TransactionState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tx, ok := m.transactions[txID]
	if !ok {
		return "", false
	}
	return tx.status, true
}

// =============================================================================
// UsersTable Implementation
// =============================================================================

// UsersTable is an in-memory table that supports full DML operations.
// It implements catalog.Table, catalog.InsertableTable, catalog.UpdatableTable,
// and catalog.DeletableTable interfaces.
type UsersTable struct {
	schema    *arrow.Schema
	alloc     memory.Allocator
	mu        sync.RWMutex
	data      [][]any // Each row: [rowid, id, name, email]
	nextRowID int64

	txManager catalog.TransactionManager
}

// NewUsersTable creates a new writable users table.
func NewUsersTable() *UsersTable {
	// Schema with rowid pseudocolumn - required for UPDATE/DELETE.
	// The is_rowid metadata tells DuckDB this column is the row identifier.
	rowidMeta := arrow.NewMetadata([]string{"is_rowid"}, []string{"true"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "rowid", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: rowidMeta},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	return &UsersTable{
		schema:    schema,
		alloc:     memory.DefaultAllocator,
		data:      make([][]any, 0),
		nextRowID: 1,
	}
}

// Table interface implementation

func (t *UsersTable) Name() string    { return "users" }
func (t *UsersTable) Comment() string { return "Writable users table with DML and transaction support" }
func (t *UsersTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

func (t *UsersTable) Scan(_ context.Context, _ *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.data) == 0 {
		return array.NewRecordReader(t.schema, nil)
	}

	record := t.buildRecord()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

// InsertableTable interface implementation

func (t *UsersTable) Insert(ctx context.Context, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if running in transaction context
	txID, inTx := catalog.TransactionIDFromContext(ctx)
	if inTx {
		fmt.Printf("[UsersTable] INSERT in transaction %s\n", txID[:8])
	}

	// Log RETURNING info if requested
	if opts != nil && opts.Returning {
		fmt.Printf("[UsersTable] INSERT with RETURNING requested, columns: %v\n", opts.ReturningColumns)
	}

	var totalRows int64

	for rows.Next() {
		batch := rows.RecordBatch()
		for rowIdx := int64(0); rowIdx < batch.NumRows(); rowIdx++ {
			// Assign rowid and extract values from input
			// Input schema: [id, name, email] (no rowid)
			// Storage: [rowid, id, name, email]
			row := make([]any, batch.NumCols()+1)
			row[0] = t.nextRowID // Auto-assign rowid

			for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
				col := batch.Column(colIdx)
				row[colIdx+1] = extractValue(col, int(rowIdx))
			}
			if t.checkIDExists(row[1].(int64)) {
				return nil, fmt.Errorf("id %d already exists", row[1].(int64))
			}
			t.data = append(t.data, row)
			t.nextRowID++
			totalRows++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	fmt.Printf("[UsersTable] Inserted %d rows (total: %d)\n", totalRows, len(t.data))
	return &catalog.DMLResult{AffectedRows: totalRows}, nil
}

// UpdatableTable interface implementation

func (t *UsersTable) Update(ctx context.Context, rowIDs []int64, rows array.RecordReader, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if running in transaction context
	if txID, ok := catalog.TransactionIDFromContext(ctx); ok {
		fmt.Printf("[UsersTable] UPDATE in transaction %s\n", txID[:8])
	}

	// Log RETURNING info if requested
	if opts != nil && opts.Returning {
		fmt.Printf("[UsersTable] UPDATE with RETURNING requested, columns: %v\n", opts.ReturningColumns)
	}

	// Build rowid to index mapping
	rowIndex := make(map[int64]int)
	for i, row := range t.data {
		if rowid, ok := row[0].(int64); ok {
			rowIndex[rowid] = i
		}
	}

	// Get input schema to map columns
	inputSchema := rows.Schema()
	colMapping := t.buildColumnMapping(inputSchema)

	var totalRows int64
	rowIDIdx := 0

	for rows.Next() {
		batch := rows.RecordBatch()
		for rowIdx := int64(0); rowIdx < batch.NumRows(); rowIdx++ {
			if rowIDIdx >= len(rowIDs) {
				break
			}
			targetRowID := rowIDs[rowIDIdx]
			rowIDIdx++

			dataIdx, found := rowIndex[targetRowID]
			if !found {
				continue // Row doesn't exist
			}

			// Update only the columns provided in input
			for inputCol, tableCol := range colMapping {
				col := batch.Column(inputCol)
				t.data[dataIdx][tableCol] = extractValue(col, int(rowIdx))
			}
			totalRows++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	fmt.Printf("[UsersTable] Updated %d rows\n", totalRows)
	return &catalog.DMLResult{AffectedRows: totalRows}, nil
}

// DeletableTable interface implementation

func (t *UsersTable) Delete(ctx context.Context, rowIDs []int64, opts *catalog.DMLOptions) (*catalog.DMLResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if running in transaction context
	if txID, ok := catalog.TransactionIDFromContext(ctx); ok {
		fmt.Printf("[UsersTable] DELETE in transaction %s\n", txID[:8])
	}

	// Log RETURNING info if requested
	if opts != nil && opts.Returning {
		fmt.Printf("[UsersTable] DELETE with RETURNING requested, columns: %v\n", opts.ReturningColumns)
	}

	// Build set of rowids to delete
	toDelete := make(map[int64]bool)
	for _, id := range rowIDs {
		toDelete[id] = true
	}

	// Filter out deleted rows
	newData := make([][]any, 0, len(t.data))
	var deletedCount int64
	for _, row := range t.data {
		if rowid, ok := row[0].(int64); ok && toDelete[rowid] {
			deletedCount++
			continue
		}
		newData = append(newData, row)
	}
	t.data = newData

	fmt.Printf("[UsersTable] Deleted %d rows (remaining: %d)\n", deletedCount, len(t.data))
	return &catalog.DMLResult{AffectedRows: deletedCount}, nil
}

// Helper methods

func (t *UsersTable) buildRecord() arrow.RecordBatch {
	builder := array.NewRecordBuilder(t.alloc, t.schema)
	defer builder.Release()

	for _, row := range t.data {
		builder.Field(0).(*array.Int64Builder).Append(row[0].(int64))
		builder.Field(1).(*array.Int64Builder).Append(row[1].(int64))
		builder.Field(2).(*array.StringBuilder).Append(row[2].(string))
		builder.Field(3).(*array.StringBuilder).Append(row[3].(string))
	}

	return builder.NewRecordBatch()
}

func (t *UsersTable) buildColumnMapping(inputSchema *arrow.Schema) map[int]int {
	mapping := make(map[int]int)
	for i := 0; i < inputSchema.NumFields(); i++ {
		inputColName := inputSchema.Field(i).Name
		// Find in table schema (skip rowid at index 0)
		for j := 1; j < t.schema.NumFields(); j++ {
			if t.schema.Field(j).Name == inputColName {
				mapping[i] = j
				break
			}
		}
	}
	return mapping
}

func (t *UsersTable) checkIDExists(rowID int64) bool {
	for _, row := range t.data {
		if rid, ok := row[1].(int64); ok && rid == rowID {
			return true
		}
	}
	return false
}

func extractValue(col arrow.Array, idx int) any {
	if col.IsNull(idx) {
		return nil
	}
	switch arr := col.(type) {
	case *array.Int64:
		return arr.Value(idx)
	case *array.String:
		return arr.Value(idx)
	default:
		return nil
	}
}
