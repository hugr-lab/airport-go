# Geometry Type Examples

**Version**: 1.0.0 | **Date**: 2025-11-26
**Purpose**: Demonstrate geometry type usage in DDL, DML, and queries

## Overview

Airport Go supports geospatial data through Arrow extension types using WKB (Well-Known Binary) encoding. Geometry operations use the `github.com/paulmach/orb` package for type-safe geometry handling.

## Creating Tables with Geometry Columns

### Example 1: Points of Interest

```go
action := &flight.Action{
    Type: "CreateTable",
    Body: []byte(`{
        "schema_name": "gis",
        "table_name": "poi",
        "schema": {
            "fields": [
                {"name": "id", "type": "int64", "nullable": false},
                {"name": "name", "type": "utf8", "nullable": false},
                {"name": "category", "type": "utf8", "nullable": true},
                {"name": "location", "type": "extension<airport.geometry>", "nullable": false}
            ],
            "metadata": {
                "location.srid": "4326",
                "location.geometry_type": "POINT"
            }
        }
    }`),
}
```

### Example 2: Administrative Boundaries

```go
action := &flight.Action{
    Type: "CreateTable",
    Body: []byte(`{
        "schema_name": "gis",
        "table_name": "boundaries",
        "schema": {
            "fields": [
                {"name": "id", "type": "int64"},
                {"name": "name", "type": "utf8"},
                {"name": "level", "type": "utf8"},
                {"name": "geometry", "type": "extension<airport.geometry>"}
            ],
            "metadata": {
                "geometry.srid": "4326",
                "geometry.geometry_type": "MULTIPOLYGON"
            }
        }
    }`),
}
```

### Example 3: Transit Routes

```go
action := &flight.Action{
    Type: "CreateTable",
    Body: []byte(`{
        "schema_name": "transit",
        "table_name": "routes",
        "schema": {
            "fields": [
                {"name": "route_id", "type": "utf8"},
                {"name": "route_name", "type": "utf8"},
                {"name": "path", "type": "extension<airport.geometry>"}
            ],
            "metadata": {
                "path.srid": "4326",
                "path.geometry_type": "LINESTRING"
            }
        }
    }`),
}
```

## Inserting Geometry Data

### Example 1: Inserting Points

```go
import (
    "github.com/paulmach/orb"
    "github.com/paulmach/orb/encoding/wkb"
    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/array"
    "github.com/apache/arrow/go/v18/arrow/memory"
)

// Define schema with geometry extension
schema := arrow.NewSchema([]arrow.Field{
    {Name: "id", Type: arrow.PrimitiveTypes.Int64},
    {Name: "name", Type: arrow.BinaryTypes.String},
    {Name: "location", Type: &GeometryExtensionType{}},
}, nil)

// Create record builder
builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

// Add data
builder.Field(0).(*array.Int64Builder).Append(1)
builder.Field(1).(*array.StringBuilder).Append("Golden Gate Bridge")

// Encode geometry as WKB
point := orb.Point{-122.4783, 37.8199} // San Francisco
wkbBytes, err := wkb.Marshal(point)
if err != nil {
    return err
}
builder.Field(2).(*array.BinaryBuilder).Append(wkbBytes)

// Create record and send via DoPut
record := builder.NewRecord()
defer record.Release()

descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorCMD,
    Cmd:  []byte(`{"action":"insert","schema_name":"gis","table_name":"poi"}`),
}

stream, err := client.DoPut(ctx)
stream.Send(&flight.FlightData{
    FlightDescriptor: descriptor,
    DataBody:         flight.SerializeRecord(record, memory.DefaultAllocator),
})
```

### Example 2: Batch Inserting Multiple Geometries

```go
// Multiple points of interest
pois := []struct{
    id   int64
    name string
    lon  float64
    lat  float64
}{
    {1, "Golden Gate Bridge", -122.4783, 37.8199},
    {2, "Alcatraz Island", -122.4230, 37.8267},
    {3, "Fisherman's Wharf", -122.4177, 37.8080},
    {4, "Coit Tower", -122.4058, 37.8024},
}

builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

for _, poi := range pois {
    builder.Field(0).(*array.Int64Builder).Append(poi.id)
    builder.Field(1).(*array.StringBuilder).Append(poi.name)

    point := orb.Point{poi.lon, poi.lat}
    wkbBytes, _ := wkb.Marshal(point)
    builder.Field(2).(*array.BinaryBuilder).Append(wkbBytes)
}

record := builder.NewRecord()
defer record.Release()
```

### Example 3: Inserting Polygons

```go
// San Francisco city boundary (simplified)
polygon := orb.Polygon{
    orb.Ring{ // Outer ring
        orb.Point{-122.5149, 37.7081},
        orb.Point{-122.3549, 37.7081},
        orb.Point{-122.3549, 37.8324},
        orb.Point{-122.5149, 37.8324},
        orb.Point{-122.5149, 37.7081}, // Close ring
    },
}

wkbBytes, err := wkb.Marshal(polygon)

builder.Field(0).(*array.Int64Builder).Append(1)
builder.Field(1).(*array.StringBuilder).Append("San Francisco")
builder.Field(2).(*array.BinaryBuilder).Append(wkbBytes)
```

## Reading Geometry Data

### Example 1: Querying and Decoding Geometries

```go
// Query table
descriptor := &flight.FlightDescriptor{
    Type: flight.DescriptorPATH,
    Path: []string{"gis", "poi"},
}

stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte("gis.poi")})
if err != nil {
    return err
}

for {
    data, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }

    reader, err := flight.DeserializeRecord(data.DataBody, memory.DefaultAllocator)
    if err != nil {
        return err
    }
    defer reader.Release()

    // Process geometry column
    geomColumn := reader.Column(2).(*array.Binary)

    for i := 0; i < geomColumn.Len(); i++ {
        if geomColumn.IsNull(i) {
            continue
        }

        wkbBytes := geomColumn.Value(i)
        geom, err := wkb.Unmarshal(wkbBytes)
        if err != nil {
            log.Printf("Invalid geometry at row %d: %v", i, err)
            continue
        }

        // Type switch on geometry
        switch g := geom.(type) {
        case orb.Point:
            fmt.Printf("Point: (%.4f, %.4f)\n", g.Lon(), g.Lat())
        case orb.LineString:
            fmt.Printf("LineString with %d points\n", len(g))
        case orb.Polygon:
            fmt.Printf("Polygon with %d rings\n", len(g))
        default:
            fmt.Printf("Geometry type: %T\n", g)
        }
    }
}
```

## DuckDB Client Examples

### Example 1: Spatial Query with DuckDB

```sql
-- examples/gis/client.sql

-- Install and load extensions
INSTALL airport FROM community;
INSTALL spatial;
LOAD airport;
LOAD spatial;

-- Connect to Airport server
CREATE SECRET airport_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:8815'
);

-- Query points within a bounding box
SELECT
    id,
    name,
    ST_AsText(location) as location_wkt,
    ST_X(location) as longitude,
    ST_Y(location) as latitude
FROM airport_catalog.gis.poi
WHERE ST_Within(
    location,
    ST_MakeEnvelope(-122.5, 37.7, -122.3, 37.9) -- San Francisco bounds
);

-- Distance query (find POIs within 1km of a point)
SELECT
    id,
    name,
    ST_Distance(
        location,
        ST_Point(-122.4194, 37.7749) -- Downtown SF
    ) as distance_meters
FROM airport_catalog.gis.poi
WHERE ST_DWithin(
    location,
    ST_Point(-122.4194, 37.7749),
    1000 -- 1km in meters
)
ORDER BY distance_meters;
```

### Example 2: Creating Geometries in DuckDB

```sql
-- Insert using ST_Point
INSERT INTO airport_catalog.gis.poi (id, name, location)
VALUES (
    5,
    'Palace of Fine Arts',
    ST_Point(-122.4486, 37.8029)
);

-- Insert polygon using WKT
INSERT INTO airport_catalog.gis.boundaries (id, name, level, geometry)
VALUES (
    1,
    'Mission District',
    'neighborhood',
    ST_GeomFromText('POLYGON((
        -122.4314 37.7489,
        -122.4094 37.7489,
        -122.4094 37.7689,
        -122.4314 37.7689,
        -122.4314 37.7489
    ))', 4326)
);
```

## Performance Tips

### Tip 1: Use Appropriate Batch Sizes

```go
const batchSize = 1000

// Stream large geometry datasets in batches
for batch := range geometryBatches {
    builder := array.NewRecordBuilder(allocator, schema)

    for i := 0; i < len(batch) && i < batchSize; i++ {
        // Add data...
    }

    record := builder.NewRecord()
    stream.Send(record)
    record.Release()
    builder.Release()
}
```

### Tip 2: Validate Geometries Before Encoding

```go
func validateAndEncode(geom orb.Geometry) ([]byte, error) {
    // Check for empty geometries
    if geom == nil {
        return nil, errors.New("nil geometry")
    }

    // Validate specific types
    if poly, ok := geom.(orb.Polygon); ok {
        if len(poly) == 0 || len(poly[0]) < 4 {
            return nil, errors.New("invalid polygon: insufficient points")
        }
        // Ensure ring is closed
        if !poly[0][0].Equal(poly[0][len(poly[0])-1]) {
            return nil, errors.New("polygon ring not closed")
        }
    }

    return wkb.Marshal(geom)
}
```

### Tip 3: Simplify Complex Geometries

```go
import "github.com/paulmach/orb/simplify"

// Simplify polygon to reduce vertex count
simplified := simplify.DouglasPeucker(polygon, 0.0001) // tolerance in degrees

wkbBytes, err := wkb.Marshal(simplified)
```

## Testing

### Unit Test Example

```go
func TestGeometryRoundTrip(t *testing.T) {
    // Test data
    testCases := []orb.Geometry{
        orb.Point{-122.4194, 37.7749},
        orb.LineString{
            orb.Point{-122.4, 37.7},
            orb.Point{-122.5, 37.8},
        },
        orb.Polygon{
            orb.Ring{
                orb.Point{0, 0},
                orb.Point{1, 0},
                orb.Point{1, 1},
                orb.Point{0, 1},
                orb.Point{0, 0},
            },
        },
    }

    for _, original := range testCases {
        // Encode
        wkbBytes, err := wkb.Marshal(original)
        require.NoError(t, err)

        // Decode
        decoded, err := wkb.Unmarshal(wkbBytes)
        require.NoError(t, err)

        // Verify equality
        assert.True(t, original.Equal(decoded))
    }
}
```

## Error Handling

### Common Errors

```go
// Invalid WKB data
wkbBytes := []byte{0x00, 0x01} // Truncated
geom, err := wkb.Unmarshal(wkbBytes)
if err != nil {
    // Handle: "WKB parse error"
}

// Unsupported geometry type
var unsupported orb.Geometry = orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}}
wkbBytes, err := wkb.Marshal(unsupported)
if err != nil {
    // Handle: Bounds cannot be marshaled directly
}

// Schema mismatch
// Trying to insert POLYGON into POINT column
// Returns: codes.InvalidArgument "geometry type mismatch"
```

## References

- orb package: https://github.com/paulmach/orb
- DuckDB Spatial: https://duckdb.org/docs/extensions/spatial
- WKB specification: https://www.ogc.org/standards/sfa
- EPSG codes: https://epsg.io/

