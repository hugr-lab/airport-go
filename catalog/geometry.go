package catalog

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
)

// GeometryExtensionType implements Arrow extension type for geospatial data.
// Geometries are stored as WKB (Well-Known Binary) in Binary columns.
// Compatible with DuckDB spatial extension and GeoParquet format.
type GeometryExtensionType struct {
	arrow.ExtensionBase
}

// NewGeometryExtensionType creates a new geometry extension type.
func NewGeometryExtensionType() *GeometryExtensionType {
	return &GeometryExtensionType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: arrow.BinaryTypes.Binary,
		},
	}
}

// ArrayType returns the Go type for geometry arrays.
func (g *GeometryExtensionType) ArrayType() reflect.Type {
	return reflect.TypeOf(GeometryArray{})
}

// GeometryArray is an extension array for geometry data stored as WKB.
// It wraps the underlying Binary storage array and provides geometry-specific methods.
type GeometryArray struct {
	array.ExtensionArrayBase
}

// ValueBytes returns the WKB bytes at index i.
func (a *GeometryArray) ValueBytes(i int) []byte {
	return a.Storage().(*array.Binary).Value(i)
}

// Value returns the decoded geometry at index i.
func (a *GeometryArray) Value(i int) (orb.Geometry, error) {
	if a.IsNull(i) {
		return nil, nil
	}
	return DecodeGeometry(a.ValueBytes(i))
}

// String returns a string representation of the array.
func (a *GeometryArray) String() string {
	return fmt.Sprintf("GeometryArray{len=%d}", a.Len())
}

// GetOneForMarshal implements arrow.Array.
func (a *GeometryArray) GetOneForMarshal(i int) any {
	if a.IsNull(i) {
		return nil
	}
	return a.ValueBytes(i)
}

var _ array.ExtensionArray = (*GeometryArray)(nil)

// GeometryBuilder is a builder for GeometryArray that provides convenient methods
// for appending geometry values. It wraps the underlying Binary storage builder.
type GeometryBuilder struct {
	*array.ExtensionBuilder
}

// NewGeometryBuilder creates a new GeometryBuilder with the given memory allocator.
func NewGeometryBuilder(mem memory.Allocator) *GeometryBuilder {
	return &GeometryBuilder{
		ExtensionBuilder: array.NewExtensionBuilder(mem, NewGeometryExtensionType()),
	}
}

// storageBuilder returns the underlying Binary builder.
func (b *GeometryBuilder) storageBuilder() *array.BinaryBuilder {
	return b.Builder.(*array.BinaryBuilder)
}

// Append encodes and appends a geometry value.
func (b *GeometryBuilder) Append(geom orb.Geometry) error {
	wkbBytes, err := EncodeGeometry(geom)
	if err != nil {
		return err
	}
	b.storageBuilder().Append(wkbBytes)
	return nil
}

// AppendWKB appends raw WKB bytes directly.
func (b *GeometryBuilder) AppendWKB(wkbBytes []byte) {
	b.storageBuilder().Append(wkbBytes)
}

// AppendNull appends a null value.
func (b *GeometryBuilder) AppendNull() {
	b.storageBuilder().AppendNull()
}

// AppendValues appends multiple geometry values with optional validity mask.
func (b *GeometryBuilder) AppendValues(geoms []orb.Geometry, valid []bool) error {
	if len(valid) != 0 && len(geoms) != len(valid) {
		return fmt.Errorf("len(geoms) != len(valid): %d != %d", len(geoms), len(valid))
	}

	for i, geom := range geoms {
		if len(valid) > 0 && !valid[i] {
			b.AppendNull()
			continue
		}
		if err := b.Append(geom); err != nil {
			return fmt.Errorf("geometry[%d]: %w", i, err)
		}
	}
	return nil
}

// NewGeometryArray creates a GeometryArray from the accumulated values and resets the builder.
func (b *GeometryBuilder) NewGeometryArray() *GeometryArray {
	return b.NewExtensionArray().(*GeometryArray)
}

// NewBuilder implements array.CustomExtensionBuilder.
// This enables automatic use of GeometryBuilder when creating builders via array.NewBuilder.
func (g *GeometryExtensionType) NewBuilder(mem memory.Allocator) array.Builder {
	return NewGeometryBuilder(mem)
}

var (
	_ array.Builder                = (*GeometryBuilder)(nil)
	_ array.CustomExtensionBuilder = (*GeometryExtensionType)(nil)
)

// ExtensionName returns the extension type identifier.
// Uses "geoarrow.wkb" for maximum compatibility with GeoArrow and DuckDB.
func (g *GeometryExtensionType) ExtensionName() string {
	return "geoarrow.wkb"
}

// String returns a string representation of the type.
func (g *GeometryExtensionType) String() string {
	return "extension<geoarrow.wkb>"
}

// Serialize returns the extension metadata (empty for basic WKB).
func (g *GeometryExtensionType) Serialize() string {
	return ""
}

// Deserialize creates a geometry extension type from metadata.
func (g *GeometryExtensionType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if !arrow.TypeEqual(storageType, arrow.BinaryTypes.Binary) &&
		!arrow.TypeEqual(storageType, arrow.BinaryTypes.LargeBinary) {
		return nil, fmt.Errorf("invalid storage type for geometry: %s (expected Binary or LargeBinary)", storageType)
	}
	return &GeometryExtensionType{
		ExtensionBase: arrow.ExtensionBase{Storage: storageType},
	}, nil
}

// ExtensionEquals checks equality with another extension type.
func (g *GeometryExtensionType) ExtensionEquals(other arrow.ExtensionType) bool {
	otherGeom, ok := other.(*GeometryExtensionType)
	if !ok {
		return false
	}
	return arrow.TypeEqual(g.StorageType(), otherGeom.StorageType())
}

// GeometryMetadata represents CRS and encoding information for geometry columns.
// Stored in Arrow field metadata as JSON.
type GeometryMetadata struct {
	// CRS is the coordinate reference system (PROJJSON format).
	CRS *CRS `json:"crs,omitempty"`

	// Encoding is the geometry encoding format (default: "WKB").
	Encoding string `json:"encoding,omitempty"`

	// GeometryTypes lists allowed geometry types (e.g., ["Point", "Polygon"]).
	// If nil/empty, any geometry type is allowed.
	GeometryTypes []string `json:"geometry_types,omitempty"`

	// Edges indicates edge interpretation ("planar" or "spherical").
	Edges string `json:"edges,omitempty"`

	// BBox is the bounding box [minx, miny, maxx, maxy].
	BBox []float64 `json:"bbox,omitempty"`
}

// CRS represents a coordinate reference system in PROJJSON format.
// Simplified structure for common use cases.
type CRS struct {
	// ID identifies the CRS (e.g., EPSG code).
	ID *CRSID `json:"id,omitempty"`

	// Name is human-readable CRS name.
	Name string `json:"name,omitempty"`

	// Type is the CRS type (e.g., "GeographicCRS", "ProjectedCRS").
	Type string `json:"type,omitempty"`
}

// CRSID represents a CRS identifier (typically EPSG code).
type CRSID struct {
	Authority string `json:"authority"` // e.g., "EPSG"
	Code      int    `json:"code"`      // e.g., 4326
}

// NewGeometryField creates an Arrow field with geometry extension type and metadata.
func NewGeometryField(name string, nullable bool, srid int, geomType string) arrow.Field {
	extType := NewGeometryExtensionType()

	// Create CRS metadata
	metadata := &GeometryMetadata{
		CRS: &CRS{
			ID: &CRSID{
				Authority: "EPSG",
				Code:      srid,
			},
		},
		Encoding: "WKB",
	}

	if geomType != "" && geomType != "GEOMETRY" {
		metadata.GeometryTypes = []string{geomType}
	}

	// Serialize metadata as JSON
	metadataJSON, _ := json.Marshal(metadata)

	// Build field metadata
	fieldMetadata := arrow.MetadataFrom(map[string]string{
		"ARROW:extension:name":     extType.ExtensionName(),
		"ARROW:extension:metadata": string(metadataJSON),
		"srid":                     fmt.Sprintf("%d", srid),
		"geometry_type":            geomType,
		"dimension":                "XY",
	})

	return arrow.Field{
		Name:     name,
		Type:     extType,
		Nullable: nullable,
		Metadata: fieldMetadata,
	}
}

// EncodeGeometry converts an orb.Geometry to WKB bytes for Arrow storage.
func EncodeGeometry(geom orb.Geometry) ([]byte, error) {
	if geom == nil {
		return nil, fmt.Errorf("cannot encode nil geometry")
	}
	return wkb.Marshal(geom)
}

// DecodeGeometry converts WKB bytes from Arrow storage to orb.Geometry.
func DecodeGeometry(wkbBytes []byte) (orb.Geometry, error) {
	if len(wkbBytes) == 0 {
		return nil, fmt.Errorf("cannot decode empty WKB data")
	}
	return wkb.Unmarshal(wkbBytes)
}

// ValidateGeometry checks if a geometry is valid for storage.
func ValidateGeometry(geom orb.Geometry) error {
	if geom == nil {
		return fmt.Errorf("geometry is nil")
	}

	switch g := geom.(type) {
	case orb.Point:
		// Points are always valid
		return nil

	case orb.MultiPoint:
		if len(g) == 0 {
			return fmt.Errorf("multipoint is empty")
		}
		return nil

	case orb.LineString:
		if len(g) < 2 {
			return fmt.Errorf("linestring must have at least 2 points, has %d", len(g))
		}
		return nil

	case orb.MultiLineString:
		if len(g) == 0 {
			return fmt.Errorf("multilinestring is empty")
		}
		for i, ls := range g {
			if len(ls) < 2 {
				return fmt.Errorf("multilinestring[%d] must have at least 2 points, has %d", i, len(ls))
			}
		}
		return nil

	case orb.Polygon:
		if len(g) == 0 {
			return fmt.Errorf("polygon has no rings")
		}
		// Check outer ring
		if len(g[0]) < 4 {
			return fmt.Errorf("polygon outer ring must have at least 4 points, has %d", len(g[0]))
		}
		// Verify outer ring is closed
		if !g[0][0].Equal(g[0][len(g[0])-1]) {
			return fmt.Errorf("polygon outer ring is not closed")
		}
		// Check inner rings (holes)
		for i, ring := range g[1:] {
			if len(ring) < 4 {
				return fmt.Errorf("polygon hole[%d] must have at least 4 points, has %d", i, len(ring))
			}
			if !ring[0].Equal(ring[len(ring)-1]) {
				return fmt.Errorf("polygon hole[%d] is not closed", i)
			}
		}
		return nil

	case orb.MultiPolygon:
		if len(g) == 0 {
			return fmt.Errorf("multipolygon is empty")
		}
		for i, poly := range g {
			if err := ValidateGeometry(poly); err != nil {
				return fmt.Errorf("multipolygon[%d]: %w", i, err)
			}
		}
		return nil

	case orb.Collection:
		if len(g) == 0 {
			return fmt.Errorf("geometry collection is empty")
		}
		for i, geom := range g {
			if err := ValidateGeometry(geom); err != nil {
				return fmt.Errorf("collection[%d]: %w", i, err)
			}
		}
		return nil

	case orb.Bound:
		// Bounds are not directly serializable to WKB
		return fmt.Errorf("bounds cannot be directly stored as WKB (convert to polygon)")

	default:
		return fmt.Errorf("unknown geometry type: %T", geom)
	}
}

// GeometryTypeName returns the WKB type name for a geometry.
func GeometryTypeName(geom orb.Geometry) string {
	switch geom.(type) {
	case orb.Point:
		return "Point"
	case orb.MultiPoint:
		return "MultiPoint"
	case orb.LineString:
		return "LineString"
	case orb.MultiLineString:
		return "MultiLineString"
	case orb.Polygon:
		return "Polygon"
	case orb.MultiPolygon:
		return "MultiPolygon"
	case orb.Collection:
		return "GeometryCollection"
	case orb.Bound:
		return "Bound"
	default:
		return "Unknown"
	}
}

// RegisterGeometryExtension registers the geometry extension type with Arrow.
// Should be called once during package initialization.
func RegisterGeometryExtension() {
	_ = arrow.RegisterExtensionType(&GeometryExtensionType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: arrow.BinaryTypes.Binary,
		},
	})
}

func init() {
	RegisterGeometryExtension()
}
