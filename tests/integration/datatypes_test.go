package airport_test

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// TestAllDataTypes verifies that all DuckDB data types are correctly handled
// through the Flight server, including DuckDB-specific Arrow extensions.
func TestAllDataTypes(t *testing.T) {
	cat := allTypesTestCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test querying the all_types table
	t.Run("AllTypes", func(t *testing.T) {
		query := "SELECT * FROM " + attachName + ".some_schema.all_types LIMIT 1"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Failed to query all_types: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected at least one row")
		}

		// Get column types
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		t.Logf("Found %d columns", len(columnTypes))
		for _, col := range columnTypes {
			t.Logf("Column: %s, Type: %s", col.Name(), col.DatabaseTypeName())
		}
	})
}

// TestGeometryTypes verifies geometry type handling with geoarrow.wkb extension
func TestGeometryTypes(t *testing.T) {
	cat := geometryTestCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	// Install and load spatial extension for ST_AsText function
	if _, err := db.Exec("INSTALL spatial"); err != nil {
		t.Skipf("Spatial extension not available: %v", err)
	}
	if _, err := db.Exec("LOAD spatial"); err != nil {
		t.Skipf("Failed to load spatial extension: %v", err)
	}

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Check column types in DuckDB catalog
	t.Run("ColumnTypes", func(t *testing.T) {
		query := `SELECT column_name, data_type
		          FROM duckdb_columns()
		          WHERE table_name = 'geometries'
		          AND schema_name = 'some_schema'
		          AND database_name = ?
		          ORDER BY column_name`

		rows, err := db.Query(query, attachName)
		if err != nil {
			t.Fatalf("Failed to query column types: %v", err)
		}
		defer rows.Close()

		expectedTypes := map[string]string{
			"geom":    "BLOB", // WKB geometry stored as BLOB/BINARY
			"geom_id": "BIGINT",
		}

		foundColumns := make(map[string]string)
		for rows.Next() {
			var colName, dataType string
			if err := rows.Scan(&colName, &dataType); err != nil {
				t.Fatalf("Failed to scan column type: %v", err)
			}
			foundColumns[colName] = dataType
			t.Logf("Column %s: %s", colName, dataType)
		}

		// Verify all expected columns exist with correct types
		for colName, expectedType := range expectedTypes {
			if actualType, ok := foundColumns[colName]; !ok {
				t.Errorf("Column %q not found in catalog", colName)
			} else if actualType != expectedType {
				t.Errorf("Column %q: expected type %s, got %s", colName, expectedType, actualType)
			}
		}

		if len(foundColumns) != len(expectedTypes) {
			t.Errorf("Expected %d columns, found %d", len(expectedTypes), len(foundColumns))
		}
	})

	// Test 2: Query geometry data
	t.Run("GeometryWKB", func(t *testing.T) {
		query := "SELECT geom_id, ST_AsText(ST_GeomFromWKB(geom)) as geom_text FROM " + attachName + ".some_schema.geometries"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Failed to query geometries: %v", err)
		}
		defer rows.Close()

		expectedGeometries := map[int64]string{
			1: "POINT (1 2)",
			2: "LINESTRING (0 0, 1 1, 2 2)",
			3: "POLYGON ((0 0, 4 0, 4 3, 0 0))",
		}

		count := 0
		for rows.Next() {
			var id int64
			var geomText string
			if err := rows.Scan(&id, &geomText); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("Geometry %d: %s", id, geomText)

			// Verify geometry matches expected value
			if expected, ok := expectedGeometries[id]; ok {
				if geomText != expected {
					t.Errorf("Geometry %d: expected %q, got %q", id, expected, geomText)
				}
			}
			count++
		}

		if count != 3 {
			t.Errorf("Expected 3 geometries, got %d", count)
		}
	})
}

// TestUUIDType verifies UUID type with arrow.uuid extension
func TestUUIDType(t *testing.T) {
	cat := uuidTestCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("UUID", func(t *testing.T) {
		query := "SELECT id, uuid_val FROM " + attachName + ".some_schema.uuids"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Failed to query uuids: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int64
			var uuidVal string
			if err := rows.Scan(&id, &uuidVal); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("UUID %d: %s", id, uuidVal)
			count++
		}

		if count == 0 {
			t.Error("Expected at least one UUID")
		}
	})
}

// TestJSONType verifies JSON type with arrow.json extension
func TestJSONType(t *testing.T) {
	cat := jsonTestCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("JSON", func(t *testing.T) {
		query := "SELECT id, CAST(json_val AS VARCHAR) as json_str FROM " + attachName + ".some_schema.json_data"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Failed to query json_data: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int64
			var jsonVal string
			if err := rows.Scan(&id, &jsonVal); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("JSON %d: %s", id, jsonVal)
			count++
		}

		if count == 0 {
			t.Error("Expected at least one JSON value")
		}
	})
}

// TestHugeIntTypes verifies HUGEINT and UHUGEINT with DuckDB extensions
func TestHugeIntTypes(t *testing.T) {
	cat := hugeintTestCatalog()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("HugeInt", func(t *testing.T) {
		query := "SELECT id, CAST(huge_val AS VARCHAR) as huge_str, CAST(uhuge_val AS VARCHAR) as uhuge_str FROM " + attachName + ".some_schema.huge_numbers"
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Failed to query huge_numbers: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int64
			var hugeVal, uhugeVal string
			if err := rows.Scan(&id, &hugeVal, &uhugeVal); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("HugeInt %d: huge=%s, uhuge=%s", id, hugeVal, uhugeVal)
			count++
		}

		if count == 0 {
			t.Error("Expected at least one huge number")
		}
	})
}

// allTypesTestCatalog creates a catalog with a table containing all common types
func allTypesTestCatalog() catalog.Catalog {
	// Create schema with all common data types
	allTypesSchema := arrow.NewSchema([]arrow.Field{
		{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "int8_col", Type: arrow.PrimitiveTypes.Int8},
		{Name: "int16_col", Type: arrow.PrimitiveTypes.Int16},
		{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32},
		{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64},
		{Name: "uint8_col", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "uint16_col", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "uint32_col", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "uint64_col", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "float32_col", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64},
		{Name: "string_col", Type: arrow.BinaryTypes.String},
		{Name: "binary_col", Type: arrow.BinaryTypes.Binary},
		{Name: "date32_col", Type: arrow.FixedWidthTypes.Date32},
		{Name: "timestamp_col", Type: arrow.FixedWidthTypes.Timestamp_us},
	}, nil)

	allTypesData := [][]interface{}{
		{
			true, int8(127), int16(32767), int32(2147483647), int64(9223372036854775807),
			uint8(255), uint16(65535), uint32(4294967295), uint64(18446744073709551615),
			float32(3.14), float64(2.718281828),
			"test string", []byte{0x01, 0x02, 0x03, 0x04},
			int32(18000), int64(1609459200000000), // 2021-01-01
		},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("Test schema with all types").
		SimpleTable(airport.SimpleTableDef{
			Name:     "all_types",
			Comment:  "Table with all common data types",
			Schema:   allTypesSchema,
			ScanFunc: makeScanFunc(allTypesSchema, allTypesData),
		}).
		Build()

	if err != nil {
		panic(err)
	}

	return cat
}

// geometryTestCatalog creates a catalog with geometry data (WKB format)
func geometryTestCatalog() catalog.Catalog {
	// Create schema with geometry column
	// Use BINARY type for WKB data - DuckDB will interpret with geoarrow.wkb extension
	geomSchema := arrow.NewSchema([]arrow.Field{
		{Name: "geom_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: arrow.MetadataFrom(map[string]string{
			"ARROW:extension:name":     "geoarrow.wkb",
			"ARROW:extension:metadata": "{}",
		})},
	}, nil)

	// Create geometries using orb and encode to WKB
	point := orb.Point{1.0, 2.0}
	pointWKB, err := wkb.Marshal(point)
	if err != nil {
		panic(err)
	}

	linestring := orb.LineString{
		{0.0, 0.0},
		{1.0, 1.0},
		{2.0, 2.0},
	}
	linestringWKB, err := wkb.Marshal(linestring)
	if err != nil {
		panic(err)
	}

	// Create a simple triangle polygon
	polygon := orb.Polygon{
		{
			{0.0, 0.0},
			{4.0, 0.0},
			{4.0, 3.0},
			{0.0, 0.0}, // Close the ring
		},
	}
	polygonWKB, err := wkb.Marshal(polygon)
	if err != nil {
		panic(err)
	}

	geomData := [][]interface{}{
		{int64(1), pointWKB},
		{int64(2), linestringWKB},
		{int64(3), polygonWKB},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("Geometry test schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "geometries",
			Comment:  "Table with geometry data (WKB format)",
			Schema:   geomSchema,
			ScanFunc: makeScanFunc(geomSchema, geomData),
		}).
		Build()

	if err != nil {
		panic(err)
	}

	return cat
}

// uuidTestCatalog creates a catalog with UUID data
func uuidTestCatalog() catalog.Catalog {
	// Create schema with UUID column (FixedSizeBinary[16] with arrow.uuid extension)
	uuidSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "uuid_val", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.MetadataFrom(map[string]string{
			"ARROW:extension:name": "arrow.uuid",
		})},
	}, nil)

	// UUID bytes for: 550e8400-e29b-41d4-a716-446655440000
	uuidBytes := []byte{
		0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	}

	uuidData := [][]interface{}{
		{int64(1), uuidBytes},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("UUID test schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "uuids",
			Comment:  "Table with UUID data",
			Schema:   uuidSchema,
			ScanFunc: makeScanFunc(uuidSchema, uuidData),
		}).
		Build()

	if err != nil {
		panic(err)
	}

	return cat
}

// jsonTestCatalog creates a catalog with JSON data
func jsonTestCatalog() catalog.Catalog {
	// Create schema with JSON column (String with arrow.json extension)
	jsonSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "json_val", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"ARROW:extension:name": "arrow.json",
		})},
	}, nil)

	jsonData := [][]interface{}{
		{int64(1), `{"name": "Alice", "age": 30, "city": "NYC"}`},
		{int64(2), `{"name": "Bob", "age": 25, "city": "LA"}`},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("JSON test schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "json_data",
			Comment:  "Table with JSON data",
			Schema:   jsonSchema,
			ScanFunc: makeScanFunc(jsonSchema, jsonData),
		}).
		Build()

	if err != nil {
		panic(err)
	}

	return cat
}

// hugeintTestCatalog creates a catalog with HUGEINT/UHUGEINT data
func hugeintTestCatalog() catalog.Catalog {
	// Create schema with HUGEINT columns (FixedSizeBinary[16] with DuckDB extensions)
	hugeintSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "huge_val", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.MetadataFrom(map[string]string{
			"ARROW:extension:name":     "arrow.opaque",
			"ARROW:extension:metadata": `{"type_name":"hugeint","vendor_name":"DuckDB"}`,
		})},
		{Name: "uhuge_val", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.MetadataFrom(map[string]string{
			"ARROW:extension:name":     "arrow.opaque",
			"ARROW:extension:metadata": `{"type_name":"uhugeint","vendor_name":"DuckDB"}`,
		})},
	}, nil)

	// Represent very large numbers as 16-byte fixed binary
	// For simplicity, using small values encoded as 16 bytes (little endian)
	hugeBytes := make([]byte, 16)
	hugeBytes[0] = 0xFF // -1 in two's complement
	for i := 1; i < 16; i++ {
		hugeBytes[i] = 0xFF
	}

	uhugeBytes := make([]byte, 16)
	uhugeBytes[0] = 0x01 // 1
	for i := 1; i < 16; i++ {
		uhugeBytes[i] = 0x00
	}

	hugeintData := [][]interface{}{
		{int64(1), hugeBytes, uhugeBytes},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("HUGEINT test schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "huge_numbers",
			Comment:  "Table with HUGEINT/UHUGEINT data",
			Schema:   hugeintSchema,
			ScanFunc: makeScanFunc(hugeintSchema, hugeintData),
		}).
		Build()

	if err != nil {
		panic(err)
	}

	return cat
}

