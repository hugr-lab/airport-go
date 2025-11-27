# Research: Repository Preparation for Production Use

**Feature**: 001-repo-preparation | **Date**: 2025-11-26
**Purpose**: Resolve unknowns and establish technical approach for DDL/DML operations, CI/CD, and examples

## Research Questions

### 1. Apache Arrow Flight RPC DoAction/DoPut Patterns for DDL/DML

**Question**: What are the idiomatic patterns for implementing custom DoAction and DoPut handlers in Apache Arrow Flight Go?

**Decision**: Use Arrow Flight's `flight.Server` interface with custom action handlers

**Rationale**:
- Apache Arrow Flight provides `DoAction` RPC method for custom commands (DDL operations)
- `DoPut` RPC method is designed for streaming data uploads (INSERT operations)
- Flight Go SDK (`github.com/apache/arrow/go/v18/arrow/flight`) provides `flight.BaseFlightServer` with overridable methods
- DuckDB Airport extension uses this exact pattern for DDL/DML operations

**Implementation Pattern**:
```go
// DoAction handles DDL operations (CREATE SCHEMA, DROP TABLE, etc.)
func (s *FlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
    switch action.Type {
    case "CreateSchema":
        return s.handleCreateSchema(action.Body, stream)
    case "DropSchema":
        return s.handleDropSchema(action.Body, stream)
    // ... other DDL actions
    default:
        return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
    }
}

// DoPut handles INSERT operations
func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
    // Read FlightDescriptor to determine operation type
    // Read Arrow record batches from stream
    // Validate schema compatibility
    // Insert into catalog
}
```

**Alternatives Considered**:
- Custom gRPC service: Rejected - Flight RPC is standard, DuckDB Airport extension compatibility requires Flight protocol
- REST API: Rejected - Arrow Flight is optimized for columnar data transfer, aligns with project goals

**References**:
- Arrow Flight Go SDK: https://pkg.go.dev/github.com/apache/arrow/go/v18/arrow/flight
- DuckDB Airport extension: https://airport.query.farm/
- Arrow Flight RPC protocol: https://arrow.apache.org/docs/format/Flight.html

---

### 2. GitHub Actions Workflow for Go Projects

**Question**: What is the standard GitHub Actions configuration for Go projects with linting, testing, and race detection?

**Decision**: Use official `actions/setup-go` with golangci-lint and race detector

**Rationale**:
- `actions/setup-go@v5` is the official GitHub Actions for Go setup
- `golangci/golangci-lint-action@v4` provides caching and fast linting
- Race detector (`go test -race`) is standard Go tooling for concurrency safety
- Caching go modules speeds up CI significantly

**Implementation Pattern**:
```yaml
name: CI
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      - uses: golangci/golangci-lint-action@v4
        with:
          version: latest

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      - run: go test -v -race ./...
```

**Alternatives Considered**:
- CircleCI: Rejected - GitHub Actions is more integrated with GitHub repo
- Jenkins: Rejected - Too heavyweight for Go library project
- Manual linting scripts: Rejected - golangci-lint-action provides better caching

**References**:
- actions/setup-go: https://github.com/actions/setup-go
- golangci-lint-action: https://github.com/golangci/golangci-lint-action

---

### 3. IF EXISTS / IF NOT EXISTS Implementation Strategy

**Question**: How should IF EXISTS/IF NOT EXISTS clauses be represented in DoAction payloads and implemented in handlers?

**Decision**: Include boolean flags in JSON action payload; check catalog state before operation

**Rationale**:
- DuckDB Airport extension uses JSON payloads for action bodies
- Boolean flags (`if_not_exists`, `if_exists`) are clear and type-safe
- Catalog state checks are idempotent and safe with single-writer model
- Error handling becomes conditional based on flags

**Implementation Pattern**:
```go
type CreateSchemaAction struct {
    SchemaName   string `json:"schema_name"`
    IfNotExists  bool   `json:"if_not_exists"`
}

func (s *FlightServer) handleCreateSchema(body []byte, stream ...) error {
    var action CreateSchemaAction
    if err := json.Unmarshal(body, &action); err != nil {
        return status.Errorf(codes.InvalidArgument, "invalid action body: %v", err)
    }

    // Check if schema already exists
    existing, err := s.catalog.Schema(ctx, action.SchemaName)
    if err != nil {
        return err
    }
    if existing != nil {
        if action.IfNotExists {
            // Idempotent: return success without error
            return nil
        }
        return status.Errorf(codes.AlreadyExists, "schema '%s' already exists", action.SchemaName)
    }

    // Create schema
    return s.catalog.CreateSchema(ctx, action.SchemaName)
}
```

**Alternatives Considered**:
- Separate action types (CreateSchema vs CreateSchemaIfNotExists): Rejected - duplicates handler logic
- SQL parsing: Rejected - spec explicitly excludes SQL parsing (FR-249)
- Protobuf payloads: Rejected - JSON is simpler and DuckDB Airport uses JSON

---

### 4. DuckDB Client Example Format

**Question**: What format should DuckDB client examples use (SQL files, shell scripts, or separate binaries)?

**Decision**: SQL files with inline comments + README with shell commands

**Rationale**:
- DuckDB CLI supports reading SQL from files: `duckdb < example.sql`
- SQL format is familiar to database users
- README provides installation instructions and context
- Keeps examples lightweight (no additional Go code needed)

**Implementation Pattern**:
```sql
-- examples/basic/client.sql
-- Connect to Airport Flight server
INSTALL airport FROM community;
LOAD airport;

-- Connect to local server
CREATE SECRET airport_secret (
    TYPE AIRPORT,
    uri 'grpc://localhost:8815'
);

-- Query the catalog
SELECT * FROM airport_catalog.default.users LIMIT 10;
```

**README Structure**:
```markdown
# Basic Example

## Prerequisites
- DuckDB 1.4+
- Airport extension: `INSTALL airport FROM community;`

## Running the Server
```bash
go run main.go
```

## Running the Client
```bash
duckdb < client.sql
```
```

**Alternatives Considered**:
- Go client binary: Rejected - adds complexity, not representative of typical usage
- Python client: Rejected - introduces Python dependency
- Embedded SQL in Go: Rejected - harder to read and modify

---

### 5. Point-in-Time Query Parameter Propagation

**Question**: How should `ts` and `ts_ns` parameters flow from Flight GetFlightInfo/DoGet to catalog.Table.Scan?

**Decision**: Pass parameters via catalog.ScanOptions; catalog implementations handle time-travel

**Rationale**:
- `catalog.ScanOptions` already exists and is designed for query options
- Time-travel is storage-layer concern; Airport server should pass through parameters
- Allows different catalog implementations to handle time-travel differently
- Spec states "Point-in-time queries require backing implementation to maintain historical versions" (assumptions)

**Implementation Pattern**:
```go
// Extend catalog.ScanOptions (if not already present)
type ScanOptions struct {
    Columns    []string
    Timestamp  *int64  // ts parameter (Unix seconds)
    TimestampNs *int64 // ts_ns parameter (nanoseconds)
}

// In Flight DoGet handler
func (s *FlightServer) DoGet(ticket *flight.Ticket, stream ...) error {
    // Parse ticket to extract table name and parameters
    opts := &catalog.ScanOptions{
        Timestamp: extractTimestamp(ticket),
        TimestampNs: extractTimestampNs(ticket),
    }

    // Pass to catalog
    reader, err := table.Scan(ctx, opts)
    // ... stream reader to client
}
```

**Alternatives Considered**:
- Server-level time-travel logic: Rejected - violates separation of concerns, storage-agnostic design
- Separate time-travel API: Rejected - query parameters are standard Flight pattern

---

### 6. Endpoint Discovery Implementation

**Question**: What metadata should `flight_info` and `endpoints` actions return for distributed query routing?

**Decision**: Return FlightInfo with table schema and single localhost endpoint; `endpoints` lists available actions

**Rationale**:
- Spec assumes "stable network topology" and single-server usage for now (P3 feature)
- FlightInfo contains Arrow schema (required for clients) and endpoint URIs
- Single endpoint satisfies requirements; multi-endpoint can be added later
- `endpoints` action lists available DoAction types for discovery

**Implementation Pattern**:
```go
// flight_info action
func (s *FlightServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
    // Resolve table from descriptor
    schema := table.ArrowSchema()

    endpoint := &flight.FlightEndpoint{
        Ticket: &flight.Ticket{Ticket: []byte(desc.Path[0])},
        Location: []*flight.Location{{Uri: s.serverURI}},
    }

    return &flight.FlightInfo{
        Schema:           schema,
        FlightDescriptor: desc,
        Endpoint:         []*flight.FlightEndpoint{endpoint},
        TotalRecords:     -1, // Unknown
        TotalBytes:       -1,
    }, nil
}

// endpoints action
func (s *FlightServer) ListActions(ctx context.Context, stream ...) error {
    actions := []flight.ActionType{
        {Type: "CreateSchema", Description: "Create a new schema"},
        {Type: "DropSchema", Description: "Drop an existing schema"},
        {Type: "GetFlightInfo", Description: "Get table metadata"},
        // ... all supported actions
    }
    for _, action := range actions {
        stream.Send(&action)
    }
}
```

**Alternatives Considered**:
- Distributed endpoint routing: Deferred to future work (P3 already optional)
- Service discovery integration: Out of scope per spec

---

### 7. Geometry Type Support in Arrow and DuckDB

**Question**: How should geometry types (POINT, LINESTRING, POLYGON, etc.) be represented in Arrow and mapped to/from DuckDB?

**Decision**: Use Arrow extension types with WKB (Well-Known Binary) encoding; use github.com/paulmach/orb for geometry abstractions

**Rationale**:
- DuckDB supports geometry types via spatial extension (ST_Point, ST_LineString, ST_Polygon, etc.)
- Arrow doesn't have native geometry types but supports extension types for custom data
- WKB is the standard binary format for geometry (ISO 19125, OGC Simple Features)
- github.com/paulmach/orb provides Go-native geometry types (Point, LineString, Polygon, MultiPoint, etc.)
- orb package supports WKB encoding/decoding out of the box
- DuckDB Airport extension already uses WKB for geometry serialization

**Implementation Pattern**:
```go
// Arrow extension type for geometry
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
    return "" // No parameters needed
}

func (g *GeometryExtensionType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
    if !arrow.TypeEqual(storageType, arrow.BinaryTypes.Binary) {
        return nil, fmt.Errorf("invalid storage type for geometry: %s", storageType)
    }
    return &GeometryExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: storageType}}, nil
}

// Convert orb.Geometry to WKB for Arrow
func geometryToArrow(geom orb.Geometry, builder *array.BinaryBuilder) error {
    wkb, err := wkb.Marshal(geom)
    if err != nil {
        return err
    }
    builder.Append(wkb)
    return nil
}

// Convert WKB from Arrow to orb.Geometry
func arrowToGeometry(wkbBytes []byte) (orb.Geometry, error) {
    return wkb.Unmarshal(wkbBytes)
}
```

**DuckDB Geometry Types Mapping**:
```
DuckDB Type          orb.Geometry Type       WKB Type Code
-----------          -----------------       -------------
POINT                orb.Point               1
LINESTRING           orb.LineString          2
POLYGON              orb.Polygon             3
MULTIPOINT           orb.MultiPoint          4
MULTILINESTRING      orb.MultiLineString     5
MULTIPOLYGON         orb.MultiPolygon        6
GEOMETRYCOLLECTION   orb.Collection          7
GEOMETRY (any)       orb.Geometry (interface) varies
```

**Usage Example**:
```go
// Creating a table with geometry column
action := &flight.Action{
    Type: "CreateTable",
    Body: []byte(`{
        "schema_name": "gis",
        "table_name": "locations",
        "schema": {
            "fields": [
                {"name": "id", "type": "int64"},
                {"name": "name", "type": "utf8"},
                {"name": "location", "type": "extension<airport.geometry>"}
            ]
        }
    }`),
}

// Inserting geometry data
point := orb.Point{-122.4194, 37.7749} // San Francisco
wkb, _ := wkb.Marshal(point)

builder := array.NewBinaryBuilder(memory.DefaultAllocator)
builder.Append(wkb)

// Reading geometry data
geom, err := wkb.Unmarshal(wkbBytes)
switch g := geom.(type) {
case orb.Point:
    fmt.Printf("Point: %.4f, %.4f\n", g.Lon(), g.Lat())
case orb.LineString:
    fmt.Printf("LineString with %d points\n", len(g))
// ... other types
}
```

**Alternatives Considered**:
- GeoJSON in String columns: Rejected - inefficient, no type safety, larger size
- Custom binary format: Rejected - WKB is standard, widely supported
- Nested struct types: Rejected - complex, not compatible with DuckDB
- twpayne/go-geom package: Rejected - orb is more actively maintained and has better WKB support

**References**:
- github.com/paulmach/orb: https://github.com/paulmach/orb
- DuckDB Spatial Extension: https://duckdb.org/docs/extensions/spatial
- OGC Well-Known Binary: https://www.ogc.org/standards/sfa
- Arrow Extension Types: https://arrow.apache.org/docs/format/Columnar.html#extension-types

**Validation Requirements**:
- All WKB data must be valid (parseable)
- Geometry dimension (2D, 3D, 4D) must be consistent within column
- SRID (spatial reference system) metadata optional but should be preserved if present

**Performance Considerations**:
- WKB is binary-efficient (smaller than text formats like WKT or GeoJSON)
- orb package uses efficient coordinate slices (no per-point allocations)
- Large geometries (millions of vertices) should stream via RecordBatch chunking

---

## Best Practices Summary

### Go Testing with Race Detector
- Always run tests with `-race` flag in CI
- Use `t.Parallel()` for independent tests to catch race conditions
- Avoid shared mutable state in test fixtures

### Arrow IPC Serialization
- Use `internal/serialize` package for catalog metadata
- Leverage Arrow RecordBatch for efficient columnar data transfer
- Release Arrow objects (Release()) to avoid memory leaks

### Error Handling in Flight RPC
- Return `status.Errorf` with gRPC status codes
- Use `codes.InvalidArgument` for bad input
- Use `codes.AlreadyExists` for duplicate resources
- Use `codes.NotFound` for missing resources
- Use `codes.Unimplemented` for unsupported operations

### GitHub Actions Optimization
- Cache Go modules between runs
- Run linting and testing in parallel jobs
- Use latest stable action versions
- Set timeout-minutes to prevent hung jobs

## Technology Stack Decisions

### Primary Technologies
- **Go 1.21+**: Language version (already in use)
- **Apache Arrow Flight Go v18**: RPC framework (already in use)
- **github.com/paulmach/orb v0.11+**: Geometry abstractions for geospatial types
- **golangci-lint**: Linting tool (constitution requirement)
- **log/slog**: Structured logging (clarification answer #3)
- **GitHub Actions**: CI/CD platform

### Testing Tools
- Go standard `testing` package
- Race detector (`go test -race`)
- In-memory catalog implementations for integration tests

### Example Tools
- DuckDB 1.4+ CLI
- DuckDB Airport extension (latest version from community repository)

## Implementation Risks & Mitigations

### Risk: DDL without transactions
**Mitigation**: Document limitations clearly; single-writer model reduces conflicts; IF EXISTS/IF NOT EXISTS enables idempotent operations

### Risk: 100M row test data generation
**Mitigation**: Use Arrow RecordBatch streaming to generate data incrementally; run scale tests as separate benchmarks (not in standard CI)

### Risk: DuckDB Airport extension compatibility
**Mitigation**: Document required versions explicitly; provide installation instructions; align action payload formats with Airport extension

### Risk: Breaking API changes
**Mitigation**: Zero new public APIs in root package; all additions in `flight/` subpackage; existing catalog interfaces unchanged

## Next Steps

Phase 1 artifacts:
1. **data-model.md**: Define DDL/DML action payload structures
2. **contracts/**: Document Flight RPC action/put/get contracts
3. **quickstart.md**: Developer quickstart for new contributors
4. **Update agent context**: Run `.specify/scripts/bash/update-agent-context.sh claude`

