# Geometry (GeoArrow) Example

This example demonstrates an Apache Arrow Flight server with geometry column support using the GeoArrow WKB extension type. The geometries are compatible with DuckDB's spatial extension.

## Prerequisites

- Go 1.26+
- DuckDB 1.5+ (for client testing)
- Airport extension for DuckDB
- **Spatial extension for DuckDB** (required for geometry support)

## Installation

### Install DuckDB

**macOS (Homebrew)**:
```bash
brew install duckdb
```

**Linux/Other**:
Download from https://duckdb.org/docs/installation/

### Install Required Extensions

Start DuckDB and run:
```sql
INSTALL airport FROM community;
INSTALL spatial;
```

## Running the Server

Start the Airport Flight server:

```bash
go run main.go
```

The server will start on `localhost:50051` and output:
```
Airport Geometry server listening on :50051

Example catalog contains:
  - Schema: geo
    - Table: locations (with geometry column)
```

## Testing with DuckDB Client

In a separate terminal, start DuckDB:

```bash
duckdb
```

Then run the following commands:

```sql
-- Install and load extensions
INSTALL airport FROM community;
LOAD airport;

-- IMPORTANT: Required for geometry support
INSTALL spatial;
LOAD spatial;

-- NOTE: DuckDB 1.4 requires manual GeoArrow registration:
-- FROM register_geoarrow_extensions();
-- DuckDB 1.5+ registers GeoArrow extensions automatically.

-- Connect to the local Airport server
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');

-- Query locations with geometry
SELECT * FROM demo.geo.locations;

-- View geometry as WKT (Well-Known Text)
SELECT id, name, category, ST_AsText(geom) as wkt
FROM demo.geo.locations;

-- Find locations in the Western Hemisphere (negative longitude)
SELECT name, ST_X(geom) as longitude, ST_Y(geom) as latitude
FROM demo.geo.locations
WHERE ST_X(geom) < 0;

-- Calculate distances between locations (in degrees)
SELECT
    a.name as from_location,
    b.name as to_location,
    ST_Distance(a.geom, b.geom) as distance
FROM demo.geo.locations a, demo.geo.locations b
WHERE a.id < b.id
ORDER BY distance
LIMIT 5;

-- Filter by category
SELECT name, ST_AsText(geom) as location
FROM demo.geo.locations
WHERE category = 'landmark';
```

## Expected Output

Basic query:
```
┌───────┬────────────────────────┬──────────┬──────────────────────────────────────┐
│  id   │          name          │ category │                 geom                 │
│ int64 │        varchar         │ varchar  │               geometry               │
├───────┼────────────────────────┼──────────┼──────────────────────────────────────┤
│     1 │ San Francisco          │ city     │ POINT (-122.4194 37.7749)            │
│     2 │ New York               │ city     │ POINT (-73.9857 40.7484)             │
│     3 │ London                 │ city     │ POINT (-0.1276 51.5074)              │
│   ... │ ...                    │ ...      │ ...                                  │
└───────┴────────────────────────┴──────────┴──────────────────────────────────────┘
```

WKT output:
```
┌───────┬────────────────────────┬──────────┬─────────────────────────────────┐
│  id   │          name          │ category │               wkt               │
│ int64 │        varchar         │ varchar  │             varchar             │
├───────┼────────────────────────┼──────────┼─────────────────────────────────┤
│     1 │ San Francisco          │ city     │ POINT (-122.4194 37.7749)       │
│     2 │ New York               │ city     │ POINT (-73.9857 40.7484)        │
│     3 │ London                 │ city     │ POINT (-0.1276 51.5074)         │
└───────┴────────────────────────┴──────────┴─────────────────────────────────┘
```

## Important Notes

### GeoArrow Extension Registration

DuckDB 1.5+ automatically registers the GeoArrow extension types when the spatial extension is loaded. No manual registration is needed.

If you are using DuckDB 1.4, you must manually call `register_geoarrow_extensions()` after loading the spatial extension:

```sql
-- Only required for DuckDB 1.4
FROM register_geoarrow_extensions();
```

### Coordinate System

This example uses EPSG:4326 (WGS84), the standard geographic coordinate system:
- Coordinates are in (longitude, latitude) order
- Longitude ranges from -180 to 180
- Latitude ranges from -90 to 90

### Geometry Types

The `catalog.NewGeometryField()` function supports these geometry types:
- `Point`
- `LineString`
- `Polygon`
- `MultiPoint`
- `MultiLineString`
- `MultiPolygon`
- `GeometryCollection`
- `GEOMETRY` (any type)

## What's Happening

1. **Schema Definition**: The server creates a schema with a geometry field using `catalog.NewGeometryField()`, which creates a proper `geoarrow.wkb` Arrow extension type with CRS metadata.

2. **GeometryBuilder**: When building records, `catalog.GeometryBuilder` is automatically used for geometry columns. It accepts `orb.Geometry` values and encodes them to WKB (Well-Known Binary) format.

3. **IPC Transfer**: Geometries are serialized as WKB bytes in the Arrow IPC format with the `geoarrow.wkb` extension type metadata.

4. **DuckDB Decoding**: DuckDB's spatial extension (1.5+) automatically recognizes the `geoarrow.wkb` extension type and converts it to DuckDB's native GEOMETRY type.

5. **Spatial Queries**: Once decoded, you can use all of DuckDB's spatial functions (ST_AsText, ST_Distance, ST_X, ST_Y, etc.).

## Next Steps

- Try the [filter example](../filter/) for predicate pushdown
- Try the [dml example](../dml/) for INSERT/UPDATE/DELETE operations
- Read the [main README](../../README.md) for more features
