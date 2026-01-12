package flight

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/catalog"
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

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestNewMultiCatalogServerInternal_Success(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	cat2 := &mockCatalog{name: "analytics"}

	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")
	srv2 := NewServer(cat2, memory.DefaultAllocator, testLogger(), "")

	mcs, err := NewMultiCatalogServerInternal(testLogger(), srv1, srv2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	catalogs := mcs.Catalogs()
	if len(catalogs) != 2 {
		t.Errorf("expected 2 catalogs, got %d", len(catalogs))
	}
}

func TestNewMultiCatalogServerInternal_NilServer(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	_, err := NewMultiCatalogServerInternal(testLogger(), srv1, nil)
	if err == nil {
		t.Fatal("expected error for nil server")
	}
	if !errors.Is(err, ErrNilCatalog) {
		t.Errorf("expected ErrNilCatalog, got %T", err)
	}
}

func TestNewMultiCatalogServerInternal_DuplicateNames(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	cat2 := &mockCatalog{name: "sales"} // duplicate

	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")
	srv2 := NewServer(cat2, memory.DefaultAllocator, testLogger(), "")

	_, err := NewMultiCatalogServerInternal(testLogger(), srv1, srv2)
	if err == nil {
		t.Fatal("expected error for duplicate catalog names")
	}
	var dupErr ErrDuplicateCatalog
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected ErrDuplicateCatalog, got %T", err)
	}
	if dupErr.Name != "sales" {
		t.Errorf("expected duplicate name 'sales', got '%s'", dupErr.Name)
	}
}

func TestNewMultiCatalogServerInternal_DefaultCatalog(t *testing.T) {
	cat1 := &mockCatalog{name: ""} // default catalog
	cat2 := &mockCatalog{name: "analytics"}

	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")
	srv2 := NewServer(cat2, memory.DefaultAllocator, testLogger(), "")

	mcs, err := NewMultiCatalogServerInternal(testLogger(), srv1, srv2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	catalogs := mcs.Catalogs()
	if len(catalogs) != 2 {
		t.Errorf("expected 2 catalogs, got %d", len(catalogs))
	}
}

func TestAddCatalog_Success(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	mcs, err := NewMultiCatalogServerInternal(testLogger(), srv1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cat2 := &mockCatalog{name: "analytics"}
	srv2 := NewServer(cat2, memory.DefaultAllocator, testLogger(), "")

	if err := mcs.AddCatalog(srv2); err != nil {
		t.Fatalf("unexpected error adding catalog: %v", err)
	}

	catalogs := mcs.Catalogs()
	if len(catalogs) != 2 {
		t.Errorf("expected 2 catalogs, got %d", len(catalogs))
	}
}

func TestAddCatalog_Nil(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	mcs, _ := NewMultiCatalogServerInternal(testLogger(), srv1)

	err := mcs.AddCatalog(nil)
	if err == nil {
		t.Fatal("expected error for nil catalog")
	}
	if !errors.Is(err, ErrNilCatalog) {
		t.Errorf("expected ErrNilCatalog, got %T", err)
	}
}

func TestAddCatalog_Duplicate(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	mcs, _ := NewMultiCatalogServerInternal(testLogger(), srv1)

	cat2 := &mockCatalog{name: "sales"} // duplicate
	srv2 := NewServer(cat2, memory.DefaultAllocator, testLogger(), "")

	err := mcs.AddCatalog(srv2)
	if err == nil {
		t.Fatal("expected error for duplicate catalog")
	}
	if !errors.Is(err, ErrCatalogExists) {
		t.Errorf("expected ErrCatalogExists, got %T", err)
	}
}

func TestRemoveCatalog_Success(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	cat2 := &mockCatalog{name: "analytics"}

	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")
	srv2 := NewServer(cat2, memory.DefaultAllocator, testLogger(), "")

	mcs, _ := NewMultiCatalogServerInternal(testLogger(), srv1, srv2)

	if err := mcs.RemoveCatalog("analytics"); err != nil {
		t.Fatalf("unexpected error removing catalog: %v", err)
	}

	catalogs := mcs.Catalogs()
	if len(catalogs) != 1 {
		t.Errorf("expected 1 catalog, got %d", len(catalogs))
	}
}

func TestRemoveCatalog_NotFound(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	mcs, _ := NewMultiCatalogServerInternal(testLogger(), srv1)

	err := mcs.RemoveCatalog("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent catalog")
	}
	if !errors.Is(err, ErrCatalogNotFound) {
		t.Errorf("expected ErrCatalogNotFound, got %T", err)
	}
}

func TestGetCatalogServer_Success(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	mcs, _ := NewMultiCatalogServerInternal(testLogger(), srv1)

	srv, err := mcs.catalogServer("sales")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv != srv1 {
		t.Error("returned wrong server")
	}
}

func TestGetCatalogServer_NotFound(t *testing.T) {
	cat1 := &mockCatalog{name: "sales"}
	srv1 := NewServer(cat1, memory.DefaultAllocator, testLogger(), "")

	mcs, _ := NewMultiCatalogServerInternal(testLogger(), srv1)

	_, err := mcs.catalogServer("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent catalog")
	}

	// Verify it's a gRPC NotFound error
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", st.Code())
	}
}
