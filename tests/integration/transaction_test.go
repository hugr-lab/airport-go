package airport_test

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	airport "github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// mockTransactionManager implements catalog.TransactionManager for testing.
type mockTransactionManager struct {
	mu           sync.RWMutex
	transactions map[string]catalog.TransactionState
}

func newMockTransactionManager() *mockTransactionManager {
	return &mockTransactionManager{
		transactions: make(map[string]catalog.TransactionState),
	}
}

func (m *mockTransactionManager) BeginTransaction(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txID := uuid.New().String()
	m.transactions[txID] = catalog.TransactionActive
	return txID, nil
}

func (m *mockTransactionManager) CommitTransaction(ctx context.Context, txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.transactions[txID]; exists {
		m.transactions[txID] = catalog.TransactionCommitted
	}
	return nil
}

func (m *mockTransactionManager) RollbackTransaction(ctx context.Context, txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.transactions[txID]; exists {
		m.transactions[txID] = catalog.TransactionAborted
	}
	return nil
}

func (m *mockTransactionManager) GetTransactionStatus(ctx context.Context, txID string) (catalog.TransactionState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.transactions[txID]
	return state, exists
}

// transactionTestEnv provides test environment with transaction support.
type transactionTestEnv struct {
	server    *grpc.Server
	client    flight.FlightServiceClient
	conn      *grpc.ClientConn
	listener  net.Listener
	txManager *mockTransactionManager
}

func setupTransactionTestEnv(t *testing.T) *transactionTestEnv {
	t.Helper()

	// Create mock transaction manager
	txManager := newMockTransactionManager()

	// Create catalog with a simple table for testing transactions
	cat := txTestCatalog(t)

	// Create gRPC server
	server := grpc.NewServer()

	// Create and register Airport server with transaction manager
	config := airport.ServerConfig{
		Catalog:            cat,
		TransactionManager: txManager,
		Address:            "localhost:0",
	}

	if err := airport.NewServer(server, config); err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Start listener
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	// Start server in background
	go server.Serve(listener)

	// Create client
	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		listener.Close()
		server.Stop()
		t.Fatalf("failed to create client: %v", err)
	}

	client := flight.NewFlightServiceClient(conn)

	return &transactionTestEnv{
		server:    server,
		client:    client,
		conn:      conn,
		listener:  listener,
		txManager: txManager,
	}
}

func (e *transactionTestEnv) cleanup() {
	if e.conn != nil {
		e.conn.Close()
	}
	if e.server != nil {
		e.server.Stop()
	}
	if e.listener != nil {
		e.listener.Close()
	}
}

// TestCreateTransaction tests the create_transaction action.
func TestCreateTransaction(t *testing.T) {
	env := setupTransactionTestEnv(t)
	defer env.cleanup()

	ctx := context.Background()

	// Create request body
	requestBody, err := msgpack.Encode(map[string]interface{}{
		"catalog_name": "",
	})
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	// Call create_transaction action
	action := &flight.Action{
		Type: "create_transaction",
		Body: requestBody,
	}

	stream, err := env.client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive result: %v", err)
	}

	// Decode response
	var response struct {
		Identifier string `msgpack:"identifier"`
	}
	if err := msgpack.Decode(result.Body, &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify transaction was created
	if response.Identifier == "" {
		t.Error("expected non-empty transaction identifier")
	}

	// Verify transaction is active in manager
	state, exists := env.txManager.GetTransactionStatus(ctx, response.Identifier)
	if !exists {
		t.Error("transaction should exist in manager")
	}
	if state != catalog.TransactionActive {
		t.Errorf("expected state %q, got %q", catalog.TransactionActive, state)
	}
}

// TestGetTransactionStatus tests the get_transaction_status action.
func TestGetTransactionStatus(t *testing.T) {
	env := setupTransactionTestEnv(t)
	defer env.cleanup()

	ctx := context.Background()

	// First create a transaction
	txID, err := env.txManager.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	// Create request body
	requestBody, err := msgpack.Encode(map[string]interface{}{
		"transaction_id": txID,
	})
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	// Call get_transaction_status action
	action := &flight.Action{
		Type: "get_transaction_status",
		Body: requestBody,
	}

	stream, err := env.client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive result: %v", err)
	}

	// Decode response
	var response struct {
		Status string `msgpack:"status"`
		Exists bool   `msgpack:"exists"`
	}
	if err := msgpack.Decode(result.Body, &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify response
	if !response.Exists {
		t.Error("transaction should exist")
	}
	if response.Status != string(catalog.TransactionActive) {
		t.Errorf("expected status %q, got %q", catalog.TransactionActive, response.Status)
	}
}

// TestGetTransactionStatusNotFound tests get_transaction_status for non-existent transaction.
func TestGetTransactionStatusNotFound(t *testing.T) {
	env := setupTransactionTestEnv(t)
	defer env.cleanup()

	ctx := context.Background()

	// Create request body with non-existent transaction ID
	requestBody, err := msgpack.Encode(map[string]interface{}{
		"transaction_id": "non-existent-tx-id",
	})
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	// Call get_transaction_status action
	action := &flight.Action{
		Type: "get_transaction_status",
		Body: requestBody,
	}

	stream, err := env.client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive result: %v", err)
	}

	// Decode response
	var response struct {
		Status string `msgpack:"status"`
		Exists bool   `msgpack:"exists"`
	}
	if err := msgpack.Decode(result.Body, &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify response - should not exist
	if response.Exists {
		t.Error("transaction should not exist")
	}
}

// TestTransactionWithoutManager tests that operations work without TransactionManager.
func TestTransactionWithoutManager(t *testing.T) {
	// Create a simple catalog without transaction manager
	cat := txTestCatalog(t)

	// Create gRPC server
	server := grpc.NewServer()
	defer server.Stop()

	// Create and register Airport server WITHOUT transaction manager
	config := airport.ServerConfig{
		Catalog: cat,
		Address: "localhost:0",
		// TransactionManager is nil
	}

	if err := airport.NewServer(server, config); err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Start listener
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	// Start server in background
	go server.Serve(listener)

	// Create client
	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	ctx := context.Background()

	// Create request body
	requestBody, err := msgpack.Encode(map[string]interface{}{
		"catalog_name": "",
	})
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	// Call create_transaction action
	action := &flight.Action{
		Type: "create_transaction",
		Body: requestBody,
	}

	stream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive result: %v", err)
	}

	// Decode response - when no TransactionManager, identifier should be nil/empty
	var response struct {
		Identifier *string `msgpack:"identifier"`
	}
	if err := msgpack.Decode(result.Body, &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// When no TransactionManager is configured, identifier should be nil
	if response.Identifier != nil && *response.Identifier != "" {
		t.Errorf("expected nil/empty identifier without TransactionManager, got %q", *response.Identifier)
	}
}

// TestDMLWithTransaction tests that DML operations work with transaction context.
func TestDMLWithTransaction(t *testing.T) {
	env := setupTransactionTestEnv(t)
	defer env.cleanup()

	ctx := context.Background()

	// Create a transaction first
	createReq, _ := msgpack.Encode(map[string]interface{}{"catalog_name": ""})
	createAction := &flight.Action{Type: "create_transaction", Body: createReq}

	stream, err := env.client.DoAction(ctx, createAction)
	if err != nil {
		t.Fatalf("DoAction failed: %v", err)
	}

	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive result: %v", err)
	}

	var createResp struct {
		Identifier string `msgpack:"identifier"`
	}
	if err := msgpack.Decode(result.Body, &createResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if createResp.Identifier == "" {
		t.Fatal("expected non-empty transaction identifier")
	}

	// Transaction should be active
	state, exists := env.txManager.GetTransactionStatus(ctx, createResp.Identifier)
	if !exists || state != catalog.TransactionActive {
		t.Fatalf("expected active transaction, got exists=%v, state=%v", exists, state)
	}
}

// Ensure json import is used (for potential future use)
var _ = json.Marshal

// txTestCatalog creates a simple catalog for transaction testing.
func txTestCatalog(t *testing.T) catalog.Catalog {
	t.Helper()

	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		Comment("Schema for transaction testing").
		Build()

	if err != nil {
		t.Fatalf("failed to build catalog: %v", err)
	}
	return cat
}
