package airport

import (
	"context"
	"errors"
	"testing"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/flight"
)

// mockCatalog implements catalog.NamedCatalog for testing
type mockCatalog struct {
	name string
}

func (m *mockCatalog) Name() string { return m.name }
func (m *mockCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	return nil, nil
}
func (m *mockCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	return nil, nil
}

func TestValidateMultiCatalogConfig_Success(t *testing.T) {
	config := MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{
			&mockCatalog{name: "sales"},
			&mockCatalog{name: "analytics"},
		},
	}

	err := validateMultiCatalogConfig(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateMultiCatalogConfig_EmptyCatalogs(t *testing.T) {
	// Empty catalog list is allowed - catalogs can be added at runtime
	config := MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{},
	}

	err := validateMultiCatalogConfig(config)
	if err != nil {
		t.Fatalf("unexpected error for empty catalog list: %v", err)
	}
}

func TestValidateMultiCatalogConfig_NilCatalog(t *testing.T) {
	config := MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{
			&mockCatalog{name: "sales"},
			nil,
		},
	}

	err := validateMultiCatalogConfig(config)
	if err == nil {
		t.Fatal("expected error for nil catalog")
	}
	if !errors.Is(err, flight.ErrNilCatalog) {
		t.Errorf("expected ErrNilCatalog, got %T", err)
	}
}

func TestValidateMultiCatalogConfig_DuplicateNames(t *testing.T) {
	config := MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{
			&mockCatalog{name: "sales"},
			&mockCatalog{name: "sales"}, // duplicate
		},
	}

	err := validateMultiCatalogConfig(config)
	if err == nil {
		t.Fatal("expected error for duplicate catalog names")
	}
	var dupErr flight.ErrDuplicateCatalog
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected ErrDuplicateCatalog, got %T", err)
	}
	if dupErr.Name != "sales" {
		t.Errorf("expected duplicate name 'sales', got '%s'", dupErr.Name)
	}
}

func TestValidateMultiCatalogConfig_DefaultCatalog(t *testing.T) {
	config := MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{
			&mockCatalog{name: ""}, // default catalog
			&mockCatalog{name: "analytics"},
		},
	}

	err := validateMultiCatalogConfig(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateMultiCatalogConfig_DuplicateDefaultCatalogs(t *testing.T) {
	config := MultiCatalogServerConfig{
		Catalogs: []catalog.Catalog{
			&mockCatalog{name: ""}, // default catalog
			&mockCatalog{name: ""}, // another default - duplicate
		},
	}

	err := validateMultiCatalogConfig(config)
	if err == nil {
		t.Fatal("expected error for duplicate default catalogs")
	}
	var dupErr flight.ErrDuplicateCatalog
	if !errors.As(err, &dupErr) {
		t.Errorf("expected ErrDuplicateCatalog, got %T", err)
	}
	if dupErr.Name != "" {
		t.Errorf("expected duplicate name '', got '%s'", dupErr.Name)
	}
}

func TestGetCatalogName(t *testing.T) {
	tests := []struct {
		name     string
		catalog  catalog.Catalog
		expected string
	}{
		{
			name:     "named catalog",
			catalog:  &mockCatalog{name: "sales"},
			expected: "sales",
		},
		{
			name:     "empty name (default)",
			catalog:  &mockCatalog{name: ""},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getCatalogName(tt.catalog)
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestCatalogTxManagerAdapter(t *testing.T) {
	// Create a mock CatalogTransactionManager
	mockTxMgr := &mockCatalogTxManager{
		txCatalogs: make(map[string]string),
	}

	adapter := &catalogTxManagerAdapter{
		ctm:         mockTxMgr,
		catalogName: "sales",
	}

	// Test BeginTransaction passes catalog name
	txID, err := adapter.BeginTransaction(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if txID == "" {
		t.Fatal("expected non-empty txID")
	}

	// Verify catalog was recorded
	if mockTxMgr.txCatalogs[txID] != "sales" {
		t.Errorf("expected catalog 'sales', got '%s'", mockTxMgr.txCatalogs[txID])
	}

	// Test CommitTransaction
	if err := adapter.CommitTransaction(context.Background(), txID); err != nil {
		t.Fatalf("unexpected error on commit: %v", err)
	}

	// Test GetTransactionStatus
	state, exists := adapter.GetTransactionStatus(context.Background(), txID)
	if !exists {
		t.Fatal("expected transaction to exist")
	}
	if state != catalog.TransactionCommitted {
		t.Errorf("expected committed state, got %v", state)
	}
}

// mockCatalogTxManager implements catalog.CatalogTransactionManager for testing
type mockCatalogTxManager struct {
	txCatalogs map[string]string
	txStates   map[string]catalog.TransactionState
	nextID     int
}

func (m *mockCatalogTxManager) BeginTransaction(ctx context.Context, catalogName string) (string, error) {
	m.nextID++
	txID := "tx-" + string(rune('0'+m.nextID))
	m.txCatalogs[txID] = catalogName
	if m.txStates == nil {
		m.txStates = make(map[string]catalog.TransactionState)
	}
	m.txStates[txID] = catalog.TransactionActive
	return txID, nil
}

func (m *mockCatalogTxManager) CommitTransaction(ctx context.Context, txID string) error {
	m.txStates[txID] = catalog.TransactionCommitted
	return nil
}

func (m *mockCatalogTxManager) RollbackTransaction(ctx context.Context, txID string) error {
	m.txStates[txID] = catalog.TransactionAborted
	return nil
}

func (m *mockCatalogTxManager) GetTransactionStatus(ctx context.Context, txID string) (catalog.TransactionState, string, bool) {
	state, exists := m.txStates[txID]
	if !exists {
		return "", "", false
	}
	return state, m.txCatalogs[txID], true
}
