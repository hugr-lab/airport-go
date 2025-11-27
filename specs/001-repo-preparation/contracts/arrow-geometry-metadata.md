# Arrow Geometry Metadata Specification

**Version**: 1.0.0 | **Date**: 2025-11-26
**Purpose**: Document Arrow extension metadata for geometry columns compatible with GeoParquet and GeoArrow standards

## Overview

Geometry columns in Arrow use **extension types** with specific metadata to ensure compatibility with GeoParquet, GeoArrow, and DuckDB spatial extension. This document specifies the metadata conventions used by the Airport extension.

## Arrow Extension Type Metadata

### Extension Name

Geometry columns MUST set the `ARROW:extension:name` metadata field to identify the geometry encoding.

**Supported Extension Names**:

| Extension Name | Description | Storage Type |
|----------------|-------------|--------------|
| `geoarrow.wkb` | Well-Known Binary encoding (recommended) | Binary or LargeBinary |
| `geoarrow.point` | Native Point struct (x, y) | Struct<x: double, y: double> |
| `geoarrow.linestring` | Native LineString | List<Struct<x, y>> |
| `geoarrow.polygon` | Native Polygon | List<List<Struct<x, y>>> |
| `geoarrow.multipoint` | Native MultiPoint | List<Struct<x, y>> |
| `geoarrow.multilinestring` | Native MultiLineString | List<List<Struct<x, y>>> |
| `geoarrow.multipolygon` | Native MultiPolygon | List<List<List<Struct<x, y>>>> |
| `airport.geometry` | Airport-specific WKB (alias for geoarrow.wkb) | Binary |

**Recommendation**: Use `geoarrow.wkb` for maximum compatibility with DuckDB, GeoParquet, and other tools.

### Extension Metadata

Geometry columns SHOULD set the `ARROW:extension:metadata` field with a UTF-8 encoded JSON object containing CRS and encoding information.

**Metadata JSON Structure**:
```json
{
  "crs": {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84",
    "datum": {
      "type": "GeodeticReferenceFrame",
      "name": "World Geodetic System 1984",
      "ellipsoid": {
        "name": "WGS 84",
        "semi_major_axis": 6378137,
        "inverse_flattening": 298.257223563
      }
    },
    "coordinate_system": {
      "subtype": "ellipsoidal",
      "axis": [
        {
          "name": "Geodetic longitude",
          "abbreviation": "Lon",
          "direction": "east",
          "unit": "degree"
        },
        {
          "name": "Geodetic latitude",
          "abbreviation": "Lat",
          "direction": "north",
          "unit": "degree"
        }
      ]
    },
    "id": {
      "authority": "EPSG",
      "code": 4326
    }
  },
  "encoding": "WKB",
  "geometry_types": ["Point", "LineString", "Polygon"]
}
```

**Metadata Fields**:

- **`crs`** (REQUIRED): PROJJSON object describing the coordinate reference system
  - Use EPSG:4326 (WGS84 lon/lat) as default for geographic coordinates
  - Use EPSG:3857 (Web Mercator) for projected web map coordinates
  - See https://proj.org/ for CRS definitions

- **`encoding`** (OPTIONAL): String indicating encoding format
  - `"WKB"`: Well-Known Binary (default)
  - `"WKT"`: Well-Known Text (not recommended for Arrow)

- **`geometry_types`** (OPTIONAL): Array of allowed geometry type strings
  - Useful for columns with mixed geometry types
  - Examples: `["Point"]`, `["Polygon", "MultiPolygon"]`, `["LineString"]`
  - If omitted, any geometry type is allowed

- **`edges`** (OPTIONAL): String indicating edge interpretation
  - `"planar"`: Straight lines in Cartesian space (default)
  - `"spherical"`: Great circle arcs on a sphere

- **`bbox`** (OPTIONAL): Array of four numbers `[minx, miny, maxx, maxy]`
  - Bounding box for all geometries in the column
  - Useful for spatial indexing and query optimization

### Simplified Metadata for Common Cases

**EPSG:4326 (WGS84) - Most Common**:
```json
{
  "crs": {
    "id": {
      "authority": "EPSG",
      "code": 4326
    }
  }
}
```

**EPSG:3857 (Web Mercator)**:
```json
{
  "crs": {
    "id": {
      "authority": "EPSG",
      "code": 3857
    }
  }
}
```

**No CRS (unspecified coordinate system)**:
```json
{
  "crs": null
}
```

## Arrow Schema Field Metadata

In addition to extension metadata, individual Arrow schema fields MAY include custom metadata for application-specific purposes.

**Field-Level Metadata**:
```json
{
  "srid": "4326",
  "geometry_type": "POINT",
  "dimension": "XY"
}
```

**Field Metadata Keys** (Airport extension conventions):

- **`srid`**: String representation of SRID (e.g., `"4326"`, `"3857"`)
  - Redundant with extension metadata but easier to parse
  - Used by DuckDB for display purposes

- **`geometry_type`**: Expected geometry type (e.g., `"POINT"`, `"POLYGON"`, `"GEOMETRY"`)
  - Hints for validation and query planning
  - `"GEOMETRY"` means mixed/any type

- **`dimension`**: Coordinate dimension
  - `"XY"`: 2D (default)
  - `"XYZ"`: 3D with elevation
  - `"XYM"`: 2D with measure value
  - `"XYZM"`: 3D with measure

## Complete Arrow Schema Example

### Example 1: Points of Interest Table

```go
import (
    "encoding/json"
    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/memory"
)

// Extension metadata for WKB geometry
extensionMetadata := map[string]string{
    "crs": `{"id": {"authority": "EPSG", "code": 4326}}`,
    "encoding": "WKB",
    "geometry_types": `["Point"]`,
}
extensionMetadataJSON, _ := json.Marshal(extensionMetadata)

// Create geometry extension type
geomType := &GeometryExtensionType{
    ExtensionBase: arrow.ExtensionBase{
        Storage: arrow.BinaryTypes.Binary,
    },
}

// Create schema with geometry field
schema := arrow.NewSchema(
    []arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
        {
            Name:     "location",
            Type:     geomType,
            Nullable: true,
            Metadata: arrow.MetadataFrom(map[string]string{
                "ARROW:extension:name":     "geoarrow.wkb",
                "ARROW:extension:metadata": string(extensionMetadataJSON),
                "srid":                     "4326",
                "geometry_type":            "POINT",
                "dimension":                "XY",
            }),
        },
    },
    &arrow.Metadata{},
)
```

### Example 2: Administrative Boundaries

```go
// Extension metadata for polygon geometries
extensionMetadata := map[string]string{
    "crs": `{
        "id": {"authority": "EPSG", "code": 4326},
        "name": "WGS 84"
    }`,
    "encoding": "WKB",
    "geometry_types": `["Polygon", "MultiPolygon"]`,
}
extensionMetadataJSON, _ := json.Marshal(extensionMetadata)

schema := arrow.NewSchema(
    []arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
        {Name: "level", Type: arrow.BinaryTypes.String},
        {
            Name:     "boundary",
            Type:     geomType,
            Nullable: false,
            Metadata: arrow.MetadataFrom(map[string]string{
                "ARROW:extension:name":     "geoarrow.wkb",
                "ARROW:extension:metadata": string(extensionMetadataJSON),
                "srid":                     "4326",
                "geometry_type":            "MULTIPOLYGON",
                "dimension":                "XY",
            }),
        },
    },
    nil,
)
```

## GeoParquet Compatibility

When writing Arrow data to Parquet format (GeoParquet), the metadata is preserved with specific conventions:

### Parquet Schema Metadata

GeoParquet files include a top-level metadata key `geo` containing JSON metadata:

```json
{
  "version": "1.1.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Polygon"],
      "crs": {
        "id": {
          "authority": "EPSG",
          "code": 4326
        }
      },
      "bbox": [-180.0, -90.0, 180.0, 90.0]
    }
  }
}
```

**Key Fields**:
- `version`: GeoParquet spec version (use `"1.1.0"`)
- `primary_column`: Name of the primary geometry column
- `columns`: Object mapping geometry column names to their metadata

### DuckDB GeoParquet Support

DuckDB's spatial extension can read and write GeoParquet files:

```sql
-- Write table with geometry to GeoParquet
COPY (SELECT * FROM gis.places)
TO 'places.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Read GeoParquet file
SELECT ST_AsText(location), name
FROM read_parquet('places.parquet')
WHERE ST_Within(location, ST_MakeEnvelope(-122.5, 37.7, -122.3, 37.9));
```

**Requirements**:
- DuckDB 1.0+ with spatial extension loaded
- Geometry columns encoded as WKB in Binary Parquet type
- GeoParquet metadata in file-level Parquet metadata

## Testing Geometry Metadata

### Test Case 1: Metadata Round-Trip

```go
func TestGeometryMetadataRoundTrip(t *testing.T) {
    // Create schema with geometry metadata
    extensionMetadata := `{"crs": {"id": {"authority": "EPSG", "code": 4326}}}`

    field := arrow.Field{
        Name:     "geom",
        Type:     &GeometryExtensionType{},
        Nullable: true,
        Metadata: arrow.MetadataFrom(map[string]string{
            "ARROW:extension:name":     "geoarrow.wkb",
            "ARROW:extension:metadata": extensionMetadata,
        }),
    }

    // Verify metadata is preserved
    assert.Equal(t, "geoarrow.wkb", field.Metadata.Get("ARROW:extension:name"))
    assert.Equal(t, extensionMetadata, field.Metadata.Get("ARROW:extension:metadata"))

    // Parse CRS from metadata
    var metadata struct {
        CRS struct {
            ID struct {
                Authority string `json:"authority"`
                Code      int    `json:"code"`
            } `json:"id"`
        } `json:"crs"`
    }
    err := json.Unmarshal([]byte(extensionMetadata), &metadata)
    require.NoError(t, err)
    assert.Equal(t, "EPSG", metadata.CRS.ID.Authority)
    assert.Equal(t, 4326, metadata.CRS.ID.Code)
}
```

### Test Case 2: DuckDB Integration

**Test Plan**:
1. Create Airport server with geometry table
2. Connect DuckDB with Airport + spatial extensions
3. Insert geometries via Airport (test WKB encoding)
4. Query geometries from DuckDB (test spatial functions)
5. Export to GeoParquet (test metadata preservation)
6. Read GeoParquet back (test round-trip)

**Example Test Script** (to be implemented):
```go
func TestDuckDBGeometryIntegration(t *testing.T) {
    // Start Airport server with geometry table
    server := startAirportServer(t, withGeometryTable())
    defer server.Stop()

    // Run DuckDB client test
    duckdbTest := `
        -- Load extensions
        INSTALL airport FROM community;
        INSTALL spatial;
        LOAD airport;
        LOAD spatial;

        -- Connect to Airport
        CREATE SECRET airport_secret (
            TYPE AIRPORT,
            uri 'grpc://localhost:8815'
        );

        -- Test: Insert geometry via Airport
        INSERT INTO airport_catalog.gis.places (id, name, location)
        VALUES (1, 'Test', ST_Point(-122.4194, 37.7749));

        -- Test: Query geometry
        SELECT id, name, ST_AsText(location) as wkt
        FROM airport_catalog.gis.places;

        -- Test: Spatial query
        SELECT id, ST_Distance(
            location,
            ST_Point(-122.4, 37.8)
        ) as distance
        FROM airport_catalog.gis.places;

        -- Test: Export to GeoParquet
        COPY (SELECT * FROM airport_catalog.gis.places)
        TO 'test_places.parquet' (FORMAT PARQUET);

        -- Test: Read GeoParquet
        SELECT * FROM read_parquet('test_places.parquet');
    `

    // Execute DuckDB script
    output, err := runDuckDB(duckdbTest)
    require.NoError(t, err)

    // Verify geometry metadata in GeoParquet
    verifyGeoParquetMetadata(t, "test_places.parquet", map[string]interface{}{
        "version":        "1.1.0",
        "primary_column": "location",
        "columns": map[string]interface{}{
            "location": map[string]interface{}{
                "encoding":       "WKB",
                "geometry_types": []string{"Point"},
            },
        },
    })
}
```

## Developer Guidelines

### DO: Use Standard Extension Names

✅ **Correct**:
```go
metadata := arrow.MetadataFrom(map[string]string{
    "ARROW:extension:name": "geoarrow.wkb",
})
```

❌ **Incorrect**:
```go
metadata := arrow.MetadataFrom(map[string]string{
    "extension_name": "geometry", // Non-standard
})
```

### DO: Include CRS Metadata

✅ **Correct**:
```go
extensionMetadata := `{"crs": {"id": {"authority": "EPSG", "code": 4326}}}`
```

❌ **Incorrect**:
```go
// Missing CRS - coordinates are ambiguous
extensionMetadata := `{}`
```

### DO: Use Appropriate EPSG Codes

- **EPSG:4326**: Longitude/Latitude (WGS84) - Use for GPS coordinates, global data
- **EPSG:3857**: Web Mercator - Use for web maps (Google Maps, OpenStreetMap)
- **Local CRS**: Use appropriate local projection for regional data

### DO: Validate Metadata in Tests

```go
func TestGeometryFieldMetadata(t *testing.T) {
    field := getGeometryField()

    // Verify extension name
    extName := field.Metadata.Get("ARROW:extension:name")
    require.Equal(t, "geoarrow.wkb", extName)

    // Verify CRS is valid JSON
    extMetadata := field.Metadata.Get("ARROW:extension:metadata")
    var metadata map[string]interface{}
    err := json.Unmarshal([]byte(extMetadata), &metadata)
    require.NoError(t, err)

    // Verify CRS is present
    require.Contains(t, metadata, "crs")
}
```

### DON'T: Mix Different Geometry Encodings

❌ **Incorrect**:
```go
// Don't mix WKB and native struct encodings in same table
schema := arrow.NewSchema([]arrow.Field{
    {Name: "geom1", Type: &WKBGeometryType{}},   // WKB encoding
    {Name: "geom2", Type: &NativePointType{}},   // Struct encoding
}, nil)
```

✅ **Correct**:
```go
// Use consistent encoding across all geometry columns
schema := arrow.NewSchema([]arrow.Field{
    {Name: "location", Type: &WKBGeometryType{}},
    {Name: "boundary", Type: &WKBGeometryType{}},
}, nil)
```

## References

- **GeoParquet Specification**: https://geoparquet.org/releases/v1.1.0/
- **GeoArrow Specification**: https://geoarrow.org/extension-types
- **PROJJSON**: https://proj.org/en/stable/schemas/v0.7/projjson.schema.json
- **DuckDB Spatial Extension**: https://duckdb.org/docs/extensions/spatial
- **Arrow Extension Types**: https://arrow.apache.org/docs/format/Columnar.html#extension-types
- **OGC Simple Features**: https://www.ogc.org/standards/sfa

## Appendix: Common EPSG Codes

| EPSG Code | Name | Use Case |
|-----------|------|----------|
| 4326 | WGS 84 (lon/lat) | GPS, global geographic data |
| 3857 | Web Mercator | Web maps (Google, OSM) |
| 2154 | RGF93 / Lambert-93 | France national grid |
| 27700 | OSGB36 / British National Grid | UK mapping |
| 32633 | WGS 84 / UTM zone 33N | Central Europe projection |
| 32610 | WGS 84 / UTM zone 10N | Western US/Canada |

For more EPSG codes, visit https://epsg.io/

