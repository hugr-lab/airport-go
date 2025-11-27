package catalog

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/paulmach/orb"
)

func TestGeometryExtensionType(t *testing.T) {
	extType := NewGeometryExtensionType()

	// Test extension name
	if extType.ExtensionName() != "geoarrow.wkb" {
		t.Errorf("expected extension name 'geoarrow.wkb', got '%s'", extType.ExtensionName())
	}

	// Test storage type
	if !arrow.TypeEqual(extType.StorageType(), arrow.BinaryTypes.Binary) {
		t.Errorf("expected Binary storage type, got %s", extType.StorageType())
	}

	// Test string representation
	if extType.String() != "extension<geoarrow.wkb>" {
		t.Errorf("expected 'extension<geoarrow.wkb>', got '%s'", extType.String())
	}
}

func TestGeometryExtensionType_Deserialize(t *testing.T) {
	extType := NewGeometryExtensionType()

	tests := []struct {
		name        string
		storageType arrow.DataType
		data        string
		wantErr     bool
	}{
		{
			name:        "Binary storage",
			storageType: arrow.BinaryTypes.Binary,
			data:        "",
			wantErr:     false,
		},
		{
			name:        "LargeBinary storage",
			storageType: arrow.BinaryTypes.LargeBinary,
			data:        "",
			wantErr:     false,
		},
		{
			name:        "Invalid storage type",
			storageType: arrow.PrimitiveTypes.Int64,
			data:        "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extType.Deserialize(tt.storageType, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Error("Deserialize() returned nil result without error")
			}
		})
	}
}

func TestNewGeometryField(t *testing.T) {
	field := NewGeometryField("location", true, 4326, "Point")

	// Check field name and nullability
	if field.Name != "location" {
		t.Errorf("expected field name 'location', got '%s'", field.Name)
	}
	if !field.Nullable {
		t.Error("expected field to be nullable")
	}

	// Check extension type
	if field.Type.ID() != arrow.EXTENSION {
		t.Errorf("expected EXTENSION type, got %s", field.Type.ID())
	}

	// Check metadata
	extName, _ := field.Metadata.GetValue("ARROW:extension:name")
	if extName != "geoarrow.wkb" {
		t.Errorf("expected extension name 'geoarrow.wkb', got '%s'", extName)
	}

	srid, _ := field.Metadata.GetValue("srid")
	if srid != "4326" {
		t.Errorf("expected SRID '4326', got '%s'", srid)
	}

	geomType, _ := field.Metadata.GetValue("geometry_type")
	if geomType != "Point" {
		t.Errorf("expected geometry_type 'Point', got '%s'", geomType)
	}

	// Check extension metadata JSON
	extMetadata, _ := field.Metadata.GetValue("ARROW:extension:metadata")
	var metadata GeometryMetadata
	if err := json.Unmarshal([]byte(extMetadata), &metadata); err != nil {
		t.Fatalf("failed to parse extension metadata: %v", err)
	}

	if metadata.CRS == nil || metadata.CRS.ID == nil {
		t.Fatal("expected CRS with ID in metadata")
	}
	if metadata.CRS.ID.Authority != "EPSG" {
		t.Errorf("expected CRS authority 'EPSG', got '%s'", metadata.CRS.ID.Authority)
	}
	if metadata.CRS.ID.Code != 4326 {
		t.Errorf("expected CRS code 4326, got %d", metadata.CRS.ID.Code)
	}
	if metadata.Encoding != "WKB" {
		t.Errorf("expected encoding 'WKB', got '%s'", metadata.Encoding)
	}
	if len(metadata.GeometryTypes) != 1 || metadata.GeometryTypes[0] != "Point" {
		t.Errorf("expected geometry_types ['Point'], got %v", metadata.GeometryTypes)
	}
}

func TestEncodeDecodeGeometry_Point(t *testing.T) {
	point := orb.Point{-122.4194, 37.7749} // San Francisco

	// Encode
	wkbBytes, err := EncodeGeometry(point)
	if err != nil {
		t.Fatalf("EncodeGeometry() failed: %v", err)
	}
	if len(wkbBytes) == 0 {
		t.Fatal("EncodeGeometry() returned empty bytes")
	}

	// Decode
	geom, err := DecodeGeometry(wkbBytes)
	if err != nil {
		t.Fatalf("DecodeGeometry() failed: %v", err)
	}

	// Verify type and value
	decodedPoint, ok := geom.(orb.Point)
	if !ok {
		t.Fatalf("expected orb.Point, got %T", geom)
	}
	if !decodedPoint.Equal(point) {
		t.Errorf("round-trip mismatch: got %v, want %v", decodedPoint, point)
	}
}

func TestEncodeDecodeGeometry_LineString(t *testing.T) {
	lineString := orb.LineString{
		orb.Point{-122.4, 37.7},
		orb.Point{-122.5, 37.8},
		orb.Point{-122.6, 37.9},
	}

	wkbBytes, err := EncodeGeometry(lineString)
	if err != nil {
		t.Fatalf("EncodeGeometry() failed: %v", err)
	}

	geom, err := DecodeGeometry(wkbBytes)
	if err != nil {
		t.Fatalf("DecodeGeometry() failed: %v", err)
	}

	decodedLS, ok := geom.(orb.LineString)
	if !ok {
		t.Fatalf("expected orb.LineString, got %T", geom)
	}
	if !decodedLS.Equal(lineString) {
		t.Errorf("round-trip mismatch: got %v, want %v", decodedLS, lineString)
	}
}

func TestEncodeDecodeGeometry_Polygon(t *testing.T) {
	polygon := orb.Polygon{
		orb.Ring{ // Outer ring
			orb.Point{0, 0},
			orb.Point{1, 0},
			orb.Point{1, 1},
			orb.Point{0, 1},
			orb.Point{0, 0}, // Closed
		},
	}

	wkbBytes, err := EncodeGeometry(polygon)
	if err != nil {
		t.Fatalf("EncodeGeometry() failed: %v", err)
	}

	geom, err := DecodeGeometry(wkbBytes)
	if err != nil {
		t.Fatalf("DecodeGeometry() failed: %v", err)
	}

	decodedPoly, ok := geom.(orb.Polygon)
	if !ok {
		t.Fatalf("expected orb.Polygon, got %T", geom)
	}
	if !decodedPoly.Equal(polygon) {
		t.Errorf("round-trip mismatch: got %v, want %v", decodedPoly, polygon)
	}
}

func TestValidateGeometry(t *testing.T) {
	tests := []struct {
		name    string
		geom    orb.Geometry
		wantErr bool
	}{
		{
			name:    "Valid point",
			geom:    orb.Point{-122.4194, 37.7749},
			wantErr: false,
		},
		{
			name:    "Valid linestring",
			geom:    orb.LineString{orb.Point{0, 0}, orb.Point{1, 1}},
			wantErr: false,
		},
		{
			name:    "Invalid linestring (1 point)",
			geom:    orb.LineString{orb.Point{0, 0}},
			wantErr: true,
		},
		{
			name: "Valid polygon",
			geom: orb.Polygon{
				orb.Ring{
					orb.Point{0, 0},
					orb.Point{1, 0},
					orb.Point{1, 1},
					orb.Point{0, 1},
					orb.Point{0, 0},
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid polygon (not closed)",
			geom: orb.Polygon{
				orb.Ring{
					orb.Point{0, 0},
					orb.Point{1, 0},
					orb.Point{1, 1},
					orb.Point{0, 1},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid polygon (too few points)",
			geom: orb.Polygon{
				orb.Ring{
					orb.Point{0, 0},
					orb.Point{1, 1},
					orb.Point{0, 0},
				},
			},
			wantErr: true,
		},
		{
			name:    "Valid multipoint",
			geom:    orb.MultiPoint{orb.Point{0, 0}, orb.Point{1, 1}},
			wantErr: false,
		},
		{
			name:    "Invalid multipoint (empty)",
			geom:    orb.MultiPoint{},
			wantErr: true,
		},
		{
			name:    "Nil geometry",
			geom:    nil,
			wantErr: true,
		},
		{
			name:    "Bound (not serializable)",
			geom:    orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGeometry(tt.geom)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateGeometry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGeometryTypeName(t *testing.T) {
	tests := []struct {
		geom orb.Geometry
		want string
	}{
		{orb.Point{0, 0}, "Point"},
		{orb.MultiPoint{orb.Point{0, 0}}, "MultiPoint"},
		{orb.LineString{orb.Point{0, 0}, orb.Point{1, 1}}, "LineString"},
		{orb.MultiLineString{orb.LineString{orb.Point{0, 0}, orb.Point{1, 1}}}, "MultiLineString"},
		{orb.Polygon{orb.Ring{orb.Point{0, 0}, orb.Point{1, 0}, orb.Point{1, 1}, orb.Point{0, 1}, orb.Point{0, 0}}}, "Polygon"},
		{orb.MultiPolygon{}, "MultiPolygon"},
		{orb.Collection{}, "GeometryCollection"},
		{orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}}, "Bound"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := GeometryTypeName(tt.geom)
			if got != tt.want {
				t.Errorf("GeometryTypeName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGeometryInArrowRecord(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema with binary field (geometry stored as WKB in binary column)
	// We test the storage layer directly since extension types have more complex builder patterns
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "location", Type: arrow.BinaryTypes.Binary}, // WKB stored in binary
	}, nil)

	// Create record builder
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Add test data
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("Golden Gate Bridge")

	point := orb.Point{-122.4783, 37.8199}
	wkbBytes, err := EncodeGeometry(point)
	if err != nil {
		t.Fatalf("EncodeGeometry() failed: %v", err)
	}

	builder.Field(2).(*array.BinaryBuilder).Append(wkbBytes)

	// Create record
	record := builder.NewRecord()
	defer record.Release()

	// Verify record
	if record.NumRows() != 1 {
		t.Errorf("expected 1 row, got %d", record.NumRows())
	}
	if record.NumCols() != 3 {
		t.Errorf("expected 3 columns, got %d", record.NumCols())
	}

	// Verify geometry column
	geomCol := record.Column(2).(*array.Binary)
	if geomCol.Len() != 1 {
		t.Errorf("expected 1 geometry, got %d", geomCol.Len())
	}

	// Decode geometry
	geomBytes := geomCol.Value(0)
	decodedGeom, err := DecodeGeometry(geomBytes)
	if err != nil {
		t.Fatalf("DecodeGeometry() failed: %v", err)
	}

	decodedPoint, ok := decodedGeom.(orb.Point)
	if !ok {
		t.Fatalf("expected orb.Point, got %T", decodedGeom)
	}
	if !decodedPoint.Equal(point) {
		t.Errorf("decoded point mismatch: got %v, want %v", decodedPoint, point)
	}
}

func TestGeometryMetadataRoundTrip(t *testing.T) {
	field := NewGeometryField("geom", true, 4326, "Polygon")

	// Extract extension metadata
	extMetadataStr, _ := field.Metadata.GetValue("ARROW:extension:metadata")
	if extMetadataStr == "" {
		t.Fatal("extension metadata is empty")
	}

	// Parse metadata
	var metadata GeometryMetadata
	if err := json.Unmarshal([]byte(extMetadataStr), &metadata); err != nil {
		t.Fatalf("failed to unmarshal metadata: %v", err)
	}

	// Verify metadata fields
	if metadata.CRS == nil {
		t.Fatal("CRS is nil")
	}
	if metadata.CRS.ID == nil {
		t.Fatal("CRS.ID is nil")
	}
	if metadata.CRS.ID.Authority != "EPSG" {
		t.Errorf("expected EPSG authority, got %s", metadata.CRS.ID.Authority)
	}
	if metadata.CRS.ID.Code != 4326 {
		t.Errorf("expected EPSG:4326, got %d", metadata.CRS.ID.Code)
	}
	if metadata.Encoding != "WKB" {
		t.Errorf("expected WKB encoding, got %s", metadata.Encoding)
	}
	if len(metadata.GeometryTypes) != 1 || metadata.GeometryTypes[0] != "Polygon" {
		t.Errorf("expected ['Polygon'], got %v", metadata.GeometryTypes)
	}
}

func TestEncodeGeometry_Errors(t *testing.T) {
	_, err := EncodeGeometry(nil)
	if err == nil {
		t.Error("expected error encoding nil geometry, got nil")
	}
}

func TestDecodeGeometry_Errors(t *testing.T) {
	_, err := DecodeGeometry([]byte{})
	if err == nil {
		t.Error("expected error decoding empty bytes, got nil")
	}

	_, err = DecodeGeometry([]byte{0x00, 0x01}) // Truncated WKB
	if err == nil {
		t.Error("expected error decoding invalid WKB, got nil")
	}
}
