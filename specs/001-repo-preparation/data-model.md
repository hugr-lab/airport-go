# Data Model: Repository Preparation for Production Use

**Feature**: 001-repo-preparation | **Date**: 2025-11-26
**Purpose**: Define data structures for DDL/DML operations, CI configuration, and example artifacts

## Core Entities

### 1. DDL Action Payloads

These structures represent the JSON payloads sent in Flight DoAction RPC calls.

#### CreateSchemaAction
```go
type CreateSchemaAction struct {
    SchemaName  string `json:"schema_name"`  // Name of schema to create
    IfNotExists bool   `json:"if_not_exists"` // If true, succeed silently if schema exists
    Comment     string `json:"comment,omitempty"` // Optional schema comment
}
```

**Validation Rules**:
- `schema_name`: Required, non-empty, valid identifier (alphanumeric + underscore)
- `if_not_exists`: Optional, defaults to false
- Returns error `codes.AlreadyExists` if schema exists and `if_not_exists` is false

**State Transitions**: None → Schema Created

---

#### DropSchemaAction
```go
type DropSchemaAction struct {
    SchemaName string `json:"schema_name"` // Name of schema to drop
    IfExists   bool   `json:"if_exists"`   // If true, succeed silently if schema doesn't exist
    Cascade    bool   `json:"cascade"`     // If true, drop all tables in schema
}
```

**Validation Rules**:
- `schema_name`: Required, non-empty
- `if_exists`: Optional, defaults to false
- `cascade`: Optional, defaults to false (fail if schema has tables)
- Returns error `codes.NotFound` if schema doesn't exist and `if_exists` is false
- Returns error `codes.FailedPrecondition` if schema has tables and `cascade` is false

**State Transitions**: Schema Exists → Schema Deleted

---

#### CreateTableAction
```go
type CreateTableAction struct {
    SchemaName  string              `json:"schema_name"`   // Parent schema
    TableName   string              `json:"table_name"`    // Name of table to create
    IfNotExists bool                `json:"if_not_exists"` // Idempotent flag
    Schema      *ArrowSchemaPayload `json:"schema"`        // Arrow schema definition
    Comment     string              `json:"comment,omitempty"` // Optional table comment
}

type ArrowSchemaPayload struct {
    Fields   []ArrowField `json:"fields"`
    Metadata map[string]string `json:"metadata,omitempty"`
}

type ArrowField struct {
    Name     string `json:"name"`
    Type     string `json:"type"` // Arrow type string (e.g., "int64", "utf8", "float64")
    Nullable bool   `json:"nullable"`
}
```

**Validation Rules**:
- `schema_name` and `table_name`: Required, non-empty, valid identifiers
- `schema`: Required, must have at least one field
- Field names: Must be unique within schema
- Field types: Must be valid Arrow type strings
- Returns error `codes.AlreadyExists` if table exists and `if_not_exists` is false

**State Transitions**: None → Table Created

---

#### DropTableAction
```go
type DropTableAction struct {
    SchemaName string `json:"schema_name"` // Parent schema
    TableName  string `json:"table_name"`  // Name of table to drop
    IfExists   bool   `json:"if_exists"`   // Idempotent flag
}
```

**Validation Rules**:
- `schema_name` and `table_name`: Required, non-empty
- Returns error `codes.NotFound` if table doesn't exist and `if_exists` is false

**State Transitions**: Table Exists → Table Deleted

---

#### AlterTableAddColumnAction
```go
type AlterTableAddColumnAction struct {
    SchemaName string      `json:"schema_name"` // Parent schema
    TableName  string      `json:"table_name"`  // Table to alter
    IfExists   bool        `json:"if_exists"`   // Idempotent flag (for table existence)
    Column     ArrowField  `json:"column"`      // Column to add
}
```

**Validation Rules**:
- `schema_name`, `table_name`: Required, non-empty
- `column`: Required, must have valid name and type
- Returns error `codes.AlreadyExists` if column already exists
- Returns error `codes.NotFound` if table doesn't exist and `if_exists` is false

**State Transitions**: Table(N columns) → Table(N+1 columns)

---

#### AlterTableDropColumnAction
```go
type AlterTableDropColumnAction struct {
    SchemaName string `json:"schema_name"` // Parent schema
    TableName  string `json:"table_name"`  // Table to alter
    IfExists   bool   `json:"if_exists"`   // Idempotent flag
    ColumnName string `json:"column_name"` // Column to drop
}
```

**Validation Rules**:
- `schema_name`, `table_name`, `column_name`: Required, non-empty
- Returns error `codes.NotFound` if column doesn't exist and `if_exists` is false
- Cannot drop last column in table (returns `codes.FailedPrecondition`)

**State Transitions**: Table(N columns) → Table(N-1 columns)

---

### 1a. Geometry Type Support

#### GeometryExtensionType

Arrow extension type for geospatial data using WKB (Well-Known Binary) encoding.

```go
// Arrow extension type registration
type GeometryExtensionType struct {
    arrow.ExtensionBase
}

func (g *GeometryExtensionType) ArrayType() reflect.Type {
    return reflect.TypeOf((*array.Binary)(nil))
}

func (g *GeometryExtensionType) ExtensionName() string {
    return "airport.geometry"
}

func (g *GeometryExtensionType) Serialize() string {
    return "" // No parameters
}

func (g *GeometryExtensionType) StorageType() arrow.DataType {
    return arrow.BinaryTypes.Binary
}
```

**Storage Format**: Binary (WKB bytes)
**Geometry Library**: github.com/paulmach/orb

**Supported Geometry Types**:
- `orb.Point`: Single point (lon, lat or x, y)
- `orb.MultiPoint`: Collection of points
- `orb.LineString`: Sequence of connected points
- `orb.MultiLineString`: Collection of line strings
- `orb.Polygon`: Closed ring with optional holes
- `orb.MultiPolygon`: Collection of polygons
- `orb.Collection`: Heterogeneous geometry collection
- `orb.Bound`: Bounding box (min/max coordinates)

**WKB Type Codes** (ISO 19125):
```
Type                  Code    orb Type
----                  ----    --------
Point                 1       orb.Point
LineString            2       orb.LineString
Polygon               3       orb.Polygon
MultiPoint            4       orb.MultiPoint
MultiLineString       5       orb.MultiLineString
MultiPolygon          6       orb.MultiPolygon
GeometryCollection    7       orb.Collection
```

**Field Definition Example**:
```json
{
  "name": "location",
  "type": "extension<airport.geometry>",
  "nullable": true,
  "metadata": {
    "srid": "4326",
    "geometry_type": "POINT"
  }
}
```

**Usage in CREATE TABLE**:
```json
{
  "schema_name": "gis",
  "table_name": "places",
  "schema": {
    "fields": [
      {"name": "id", "type": "int64"},
      {"name": "name", "type": "utf8"},
      {"name": "location", "type": "extension<airport.geometry>"}
    ]
  }
}
```

**Validation Rules**:
- WKB data must be valid and parseable
- Geometry dimension (2D, 3D, 4D) should be consistent within column
- SRID metadata is optional but recommended (defaults to 4326 for lon/lat)
- Invalid WKB returns `codes.InvalidArgument`

**Coordinate Systems**:
- **EPSG:4326** (WGS84): Longitude/Latitude (default, -180 to 180, -90 to 90)
- **EPSG:3857** (Web Mercator): Projected meters (for web maps)
- Custom SRID: Specify in field metadata

**Encoding/Decoding**:
```go
import (
    "github.com/paulmach/orb"
    "github.com/paulmach/orb/encoding/wkb"
)

// Encode geometry to WKB
point := orb.Point{-122.4194, 37.7749}
wkbBytes, err := wkb.Marshal(point)

// Decode WKB to geometry
geom, err := wkb.Unmarshal(wkbBytes)
switch g := geom.(type) {
case orb.Point:
    fmt.Printf("Point: %.4f, %.4f\n", g.Lon(), g.Lat())
case orb.Polygon:
    fmt.Printf("Polygon with %d rings\n", len(g))
}
```

**DuckDB Compatibility**:
- DuckDB `GEOMETRY` type maps to `extension<airport.geometry>`
- DuckDB functions (ST_Point, ST_Distance, etc.) work with WKB
- Airport extension handles conversion automatically

**Performance**:
- WKB is binary-compact (smaller than GeoJSON or WKT)
- orb uses efficient coordinate slices
- Large geometries (>1MB) should be chunked across RecordBatches

---

### 2. DML Operation Descriptors

#### InsertDescriptor
```go
type InsertDescriptor struct {
    SchemaName string `json:"schema_name"` // Target schema
    TableName  string `json:"table_name"`  // Target table
}
```

**Data Format**: Arrow RecordBatch stream via DoPut
- Must match table schema exactly
- Batch size: Implementation-defined (recommend 1000-10000 rows per batch)
- Returns affected row count

**Validation Rules**:
- Schema compatibility: All columns in batch must exist in table
- Type compatibility: Arrow types must match table schema
- Returns error `codes.InvalidArgument` for schema mismatch

**State Transitions**: Table(N rows) → Table(N + batch size rows)

---

#### UpdateDescriptor
```go
type UpdateDescriptor struct {
    SchemaName string  `json:"schema_name"` // Target schema
    TableName  string  `json:"table_name"`  // Target table
    RowIds     []int64 `json:"row_ids"`     // Rows to update (via rowid pseudocolumn)
}
```

**Data Format**: Arrow RecordBatch stream via DoPut
- RecordBatch must include columns to update
- `RowIds` identifies which rows to modify
- Returns affected row count

**Validation Rules**:
- `row_ids`: Must be valid rowid values
- Schema: Updated columns must exist in table
- Returns error `codes.NotFound` for invalid rowid
- Returns error `codes.InvalidArgument` for schema mismatch

**State Transitions**: Table(row data) → Table(updated row data)

---

#### DeleteAction
```go
type DeleteAction struct {
    SchemaName string  `json:"schema_name"` // Target schema
    TableName  string  `json:"table_name"`  // Target table
    RowIds     []int64 `json:"row_ids"`     // Rows to delete (via rowid pseudocolumn)
}
```

**Data Format**: JSON action body via DoAction
- Returns affected row count

**Validation Rules**:
- `row_ids`: Must be valid rowid values
- Returns error `codes.NotFound` for invalid rowid (unless no strict mode)

**State Transitions**: Table(N rows) → Table(N - len(row_ids) rows)

---

### 3. Point-in-Time Query Parameters

#### ScanOptions (Extension)
```go
// Extends existing catalog.ScanOptions
type ScanOptions struct {
    Columns     []string          // Column projection (existing)
    Timestamp   *int64            // Unix timestamp in seconds (ts parameter)
    TimestampNs *int64            // Unix timestamp in nanoseconds (ts_ns parameter)
    Filters     []FilterExpr      // Filter predicates (existing, optional)
}
```

**Validation Rules**:
- At most one of `Timestamp` or `TimestampNs` can be set
- Timestamp values must be non-negative
- Future timestamps return empty result set
- If both are nil, query returns current data

**State Transitions**: N/A (read-only query)

---

### 4. Endpoint Discovery Responses

#### FlightInfoResponse
```go
// Returned by GetFlightInfo
type FlightInfoResponse struct {
    Schema       *arrow.Schema       // Table Arrow schema
    Descriptor   *FlightDescriptor   // Original request descriptor
    Endpoints    []FlightEndpoint    // Data source endpoints
    TotalRecords int64               // Total rows (-1 if unknown)
    TotalBytes   int64               // Total bytes (-1 if unknown)
}

type FlightEndpoint struct {
    Ticket   *Ticket   // Ticket for DoGet
    Location []string  // URIs where data can be fetched
}
```

**Validation Rules**:
- Schema: Must be valid Arrow schema
- Endpoints: Must have at least one endpoint with valid URI
- Ticket: Opaque bytes uniquely identifying data partition

**Relationships**: One FlightInfo per table; multiple endpoints for distributed data

---

#### ActionTypeList
```go
// Returned by ListActions
type ActionTypeList []ActionType

type ActionType struct {
    Type        string `json:"type"`        // Action name (e.g., "CreateSchema")
    Description string `json:"description"` // Human-readable description
}
```

**Validation Rules**:
- Type: Must be non-empty, unique across list
- Description: Recommended but optional

**Relationships**: Lists all supported DoAction types

---

### 5. CI Configuration

#### GitHubActionsWorkflow
```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
          cache: true
      - uses: golangci/golangci-lint-action@v4
        with:
          version: latest

  test:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
          cache: true
      - name: Run tests with race detector
        run: go test -v -race -timeout=10m ./...
      - name: Run integration tests
        run: go test -v -race -timeout=10m ./tests/integration/...
```

**Validation Rules**:
- Linting must pass with zero warnings
- All tests must pass
- Race detector must not report data races
- Timeout prevents hung jobs (10-15 minutes)

**State Transitions**: PR Opened → CI Running → CI Passed/Failed

---

### 6. Example Artifacts

#### DuckDBClientExample
```sql
-- examples/basic/client.sql
-- Install and load Airport extension
INSTALL airport FROM community;
LOAD airport;

-- Create connection secret
CREATE SECRET airport_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:8815'
);

-- Query catalog
SELECT * FROM airport_catalog.default.example_table LIMIT 10;
```

**README Structure**:
```markdown
# Example Name

## Prerequisites
- Go 1.21+
- DuckDB 1.4+
- Airport extension

## Setup
1. Install DuckDB: [instructions]
2. Install Airport: `INSTALL airport FROM community;`

## Running
1. Start server: `go run main.go`
2. Run client: `duckdb < client.sql`

## Expected Output
[Sample query results]
```

**Validation Rules**:
- SQL syntax must be valid DuckDB
- Connection URI must match server address
- Queries must work with example server catalogs

---

## Entity Relationships

```
Catalog
  ├─> Schema (1:N)
  │     ├─> Table (1:N)
  │     │     ├─> Column (1:N)
  │     │     └─> Row (1:N, identified by rowid)
  │     ├─> ScalarFunction (1:N)
  │     └─> TableFunction (1:N)
  │
  └─> FlightEndpoint (1:N)
        └─> Location (1:N)

DDL Actions → Catalog State Changes
DML Operations → Row Data Changes
Query Parameters → ScanOptions → RecordReader
```

## Validation Summary

### Global Constraints
- All identifiers: Must match regex `^[a-zA-Z_][a-zA-Z0-9_]*$`
- All JSON payloads: Must be valid JSON with required fields
- All Arrow schemas: Must have at least one field
- All RPC operations: Must accept `context.Context` for cancellation

### Error Code Mapping
- `codes.InvalidArgument`: Malformed request, invalid JSON, bad parameters
- `codes.NotFound`: Resource doesn't exist (without IF EXISTS)
- `codes.AlreadyExists`: Resource already exists (without IF NOT EXISTS)
- `codes.FailedPrecondition`: Operation violates constraint (e.g., drop non-empty schema)
- `codes.Unimplemented`: Unsupported action type or operation
- `codes.Internal`: Server-side error (storage failure, serialization error)

