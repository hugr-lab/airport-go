# Feature Specification: Geometry (GeoArrow) Support

**Feature ID**: 014-geometry-support
**Created**: 2026-01-12
**Status**: Implemented
**Branch**: 001-multicatalog-server

## Overview

This feature adds support for geometry columns using the GeoArrow WKB extension type, enabling spatial data handling compatible with DuckDB's spatial extension.

## Problem Statement

Users need to expose geometry/spatial data through Airport Flight servers. Without proper extension type support, geometry data cannot be correctly interpreted by DuckDB clients with the spatial extension.

## Solution

Implement GeoArrow WKB extension type support in the `catalog` package:

1. **GeometryExtensionType** - Arrow extension type implementing `geoarrow.wkb`
2. **GeometryArray** - Extension array for reading geometry data
3. **GeometryBuilder** - Custom builder for appending geometry data
4. **Helper functions** - Field creation, encoding/decoding utilities

## Implementation Details

### GeometryExtensionType

Implements `arrow.ExtensionType` interface with:
- Extension name: `geoarrow.wkb`
- Storage type: `arrow.BinaryTypes.Binary`
- Serialization: JSON metadata with CRS information

### GeometryArray

Implements `array.ExtensionArray` interface:
- Embeds `array.ExtensionArrayBase`
- `ValueBytes(i int) []byte` - Get raw WKB bytes
- `Value(i int) (orb.Geometry, error)` - Get decoded geometry
- `String() string` - String representation
- `GetOneForMarshal(i int) any` - Marshaling support

### GeometryBuilder

Implements `array.Builder` via `array.CustomExtensionBuilder`:
- `Append(orb.Geometry) error` - Append geometry (auto-encodes to WKB)
- `AppendWKB([]byte)` - Append raw WKB bytes
- `AppendNull()` - Append null value
- `AppendValues([]orb.Geometry, []bool) error` - Batch append with validity
- `NewGeometryArray() *GeometryArray` - Build array

### Helper Functions

```go
// Create geometry field with CRS metadata
catalog.NewGeometryField(name string, nullable bool, srid int, geomType string) arrow.Field

// Encode/decode geometry
catalog.EncodeGeometry(orb.Geometry) ([]byte, error)
catalog.DecodeGeometry([]byte) (orb.Geometry, error)

// Validate geometry
catalog.ValidateGeometry(orb.Geometry) error

// Get geometry type name
catalog.GeometryTypeName(orb.Geometry) string
```

## Supported Geometry Types

| Type | Description |
|------|-------------|
| Point | Single coordinate |
| LineString | Sequence of coordinates |
| Polygon | Closed ring(s) |
| MultiPoint | Collection of points |
| MultiLineString | Collection of linestrings |
| MultiPolygon | Collection of polygons |
| GeometryCollection | Mixed geometry collection |

## DuckDB Client Requirements

To query geometry data from DuckDB, clients must:

```sql
-- Install and load spatial extension
INSTALL spatial;
LOAD spatial;

-- Register GeoArrow extension type handler
FROM register_geoarrow_extensions();

-- Then attach and query
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
SELECT * FROM demo.schema.table_with_geometry;
```

The `register_geoarrow_extensions()` function registers a type handler that converts `geoarrow.wkb` extension type to DuckDB's native GEOMETRY type.

## Usage Example

### Server-Side

```go
// Create schema with geometry field
schema := arrow.NewSchema([]arrow.Field{
    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
    catalog.NewGeometryField("geom", true, 4326, "Point"),
}, nil)

// Build records
builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
idBuilder := builder.Field(0).(*array.Int64Builder)
geomBuilder := builder.Field(1).(*catalog.GeometryBuilder)

idBuilder.Append(1)
geomBuilder.Append(orb.Point{-122.4194, 37.7749})

record := builder.NewRecordBatch()
```

### Client-Side (DuckDB)

```sql
-- Spatial queries work with geometry columns
SELECT name, ST_AsText(geom) as wkt FROM demo.geo.locations;
SELECT name FROM demo.geo.locations WHERE ST_X(geom) < 0;
SELECT ST_Distance(a.geom, b.geom) FROM table a, table b;
```

## Dependencies

- `github.com/paulmach/orb` - Geometry types and WKB encoding
- `github.com/apache/arrow-go/v18` - Arrow extension type interfaces

## Files Changed

### New/Modified Files

- `catalog/geometry.go` - GeometryExtensionType, GeometryArray, GeometryBuilder
- `catalog/geometry_test.go` - Unit tests for geometry support
- `examples/geometry/main.go` - Example server with geometry column
- `examples/geometry/README.md` - Example documentation
- `docs/api-guide.md` - API documentation for geometry support
- `README.md` - Feature highlight and documentation section

## Testing

### Unit Tests

- `TestGeometryExtensionType` - Extension type basics
- `TestGeometryExtensionType_Deserialize` - Deserialization
- `TestNewGeometryField` - Field creation with metadata
- `TestEncodeDecodeGeometry_*` - Point, LineString, Polygon encoding
- `TestValidateGeometry` - Geometry validation
- `TestGeometryTypeName` - Type name extraction
- `TestGeometryBuilder` - Builder functionality
- `TestGeometryBuilder_WithRecordBuilder` - Integration with RecordBuilder
- `TestGeometryArray_String` - Array string representation

### Integration Tests

- `TestGeometryDataTypes` - End-to-end test with DuckDB client

## Success Criteria

- [x] GeometryExtensionType properly implements arrow.ExtensionType
- [x] GeometryArray properly implements array.ExtensionArray
- [x] GeometryBuilder is automatically used by RecordBuilder
- [x] IPC serialization/deserialization works correctly
- [x] DuckDB with spatial extension can read geometry columns
- [x] All spatial functions (ST_AsText, ST_X, ST_Y, ST_Distance) work
- [x] Unit tests pass
- [x] Integration tests pass
- [x] Documentation complete

## References

- [GeoArrow Specification](https://geoarrow.org/)
- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial.html)
- [paulmach/orb](https://github.com/paulmach/orb)
- [Arrow Extension Types](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
