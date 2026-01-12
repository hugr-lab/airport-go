// Package main demonstrates an Airport Flight server with geometry (GeoArrow) support.
// This example shows how to create tables with geometry columns using the geoarrow.wkb
// extension type, compatible with DuckDB's spatial extension.
//
// IMPORTANT: To query geometry data from DuckDB, you must:
//  1. Install and load the spatial extension
//  2. Register GeoArrow extensions using register_geoarrow_extensions()
//
// To test with DuckDB CLI:
//
//	duckdb
//	INSTALL airport FROM community;
//	LOAD airport;
//
//	-- Required for geometry support
//	INSTALL spatial;
//	LOAD spatial;
//	FROM register_geoarrow_extensions();
//
//	-- Connect to the server
//	ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
//
//	-- Query locations with geometry
//	SELECT * FROM demo.geo.locations;
//
//	-- Spatial queries (requires spatial extension)
//	SELECT name, ST_AsText(geom) as wkt FROM demo.geo.locations;
//	SELECT name FROM demo.geo.locations WHERE ST_X(geom) < 0;
//	SELECT a.name, b.name, ST_Distance(a.geom, b.geom) as distance
//	FROM demo.geo.locations a, demo.geo.locations b
//	WHERE a.id < b.id;
package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
	"google.golang.org/grpc"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

func main() {
	// Create locations table with geometry column
	table := NewLocationsTable()

	// Build catalog
	cat, err := airport.NewCatalogBuilder().
		Schema("geo").
		Table(table).
		Build()
	if err != nil {
		log.Fatalf("Failed to build catalog: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register Airport handlers
	debugLevel := slog.LevelDebug
	err = airport.NewServer(grpcServer, airport.ServerConfig{
		Catalog:  cat,
		LogLevel: &debugLevel,
	})
	if err != nil {
		log.Fatalf("Failed to register Airport server: %v", err)
	}

	// Start serving
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Airport Geometry server listening on :50051")
	log.Println("")
	log.Println("Example catalog contains:")
	log.Println("  - Schema: geo")
	log.Println("    - Table: locations (with geometry column)")
	log.Println("")
	log.Println("Test with DuckDB CLI:")
	log.Println("  -- Install and load required extensions")
	log.Println("  INSTALL airport FROM community;")
	log.Println("  LOAD airport;")
	log.Println("")
	log.Println("  -- IMPORTANT: Required for geometry support")
	log.Println("  INSTALL spatial;")
	log.Println("  LOAD spatial;")
	log.Println("  FROM register_geoarrow_extensions();")
	log.Println("")
	log.Println("  -- Connect and query")
	log.Println("  ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');")
	log.Println("  SELECT * FROM demo.geo.locations;")
	log.Println("")
	log.Println("  -- Spatial queries")
	log.Println("  SELECT name, ST_AsText(geom) as wkt FROM demo.geo.locations;")
	log.Println("  SELECT name FROM demo.geo.locations WHERE ST_X(geom) < 0;")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// =============================================================================
// LocationsTable Implementation with Geometry Column
// =============================================================================

// LocationsTable is an in-memory table with geometry data.
type LocationsTable struct {
	schema *arrow.Schema
	alloc  memory.Allocator
	mu     sync.RWMutex
	data   []Location
}

// Location represents a named location with a point geometry.
type Location struct {
	ID       int64
	Name     string
	Category string
	Point    orb.Point // Longitude, Latitude
}

// NewLocationsTable creates a new locations table with sample data.
func NewLocationsTable() *LocationsTable {
	// Create schema with geometry field using catalog.NewGeometryField
	// This creates a proper geoarrow.wkb extension type with CRS metadata
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},
		catalog.NewGeometryField("geom", true, 4326, "Point"), // EPSG:4326 (WGS84)
	}, nil)

	return &LocationsTable{
		schema: schema,
		alloc:  memory.DefaultAllocator,
		data: []Location{
			// Major cities (lon, lat format as per GeoJSON spec)
			{ID: 1, Name: "San Francisco", Category: "city", Point: orb.Point{-122.4194, 37.7749}},
			{ID: 2, Name: "New York", Category: "city", Point: orb.Point{-73.9857, 40.7484}},
			{ID: 3, Name: "London", Category: "city", Point: orb.Point{-0.1276, 51.5074}},
			{ID: 4, Name: "Tokyo", Category: "city", Point: orb.Point{139.6917, 35.6895}},
			{ID: 5, Name: "Sydney", Category: "city", Point: orb.Point{151.2093, -33.8688}},
			// Landmarks
			{ID: 6, Name: "Eiffel Tower", Category: "landmark", Point: orb.Point{2.2945, 48.8584}},
			{ID: 7, Name: "Statue of Liberty", Category: "landmark", Point: orb.Point{-74.0445, 40.6892}},
			{ID: 8, Name: "Golden Gate Bridge", Category: "landmark", Point: orb.Point{-122.4783, 37.8199}},
			{ID: 9, Name: "Big Ben", Category: "landmark", Point: orb.Point{-0.1246, 51.5007}},
			{ID: 10, Name: "Sydney Opera House", Category: "landmark", Point: orb.Point{151.2153, -33.8568}},
		},
	}
}

// Table interface implementation

func (t *LocationsTable) Name() string    { return "locations" }
func (t *LocationsTable) Comment() string { return "Locations with point geometry (WGS84/EPSG:4326)" }
func (t *LocationsTable) ArrowSchema(columns []string) *arrow.Schema {
	return catalog.ProjectSchema(t.schema, columns)
}

// Scan implements catalog.Table. It returns all locations with their geometries.
func (t *LocationsTable) Scan(_ context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	record := t.buildRecord()
	return array.NewRecordReader(t.schema, []arrow.RecordBatch{record})
}

// buildRecord creates an Arrow record from in-memory data using GeometryBuilder.
func (t *LocationsTable) buildRecord() arrow.RecordBatch {
	builder := array.NewRecordBuilder(t.alloc, t.schema)
	defer builder.Release()

	// Get typed builders for each column
	idBuilder := builder.Field(0).(*array.Int64Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	categoryBuilder := builder.Field(2).(*array.StringBuilder)

	// GeometryBuilder is automatically used for geometry columns when using
	// array.NewRecordBuilder with a schema that has a GeometryExtensionType field.
	// This is because GeometryExtensionType implements array.CustomExtensionBuilder.
	geomBuilder := builder.Field(3).(*catalog.GeometryBuilder)

	for _, loc := range t.data {
		idBuilder.Append(loc.ID)
		nameBuilder.Append(loc.Name)
		categoryBuilder.Append(loc.Category)

		// Append geometry using the convenient Append method
		// This automatically encodes the orb.Geometry to WKB
		if err := geomBuilder.Append(loc.Point); err != nil {
			// In production, handle this error appropriately
			log.Printf("Warning: failed to encode geometry for %s: %v", loc.Name, err)
			geomBuilder.AppendNull()
		}
	}

	return builder.NewRecordBatch()
}
