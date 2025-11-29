# Implementation Guide

This guide walks through implementing custom catalogs for airport-go, from simple static catalogs to full-featured dynamic implementations.

## Choosing an Approach

### CatalogBuilder (Recommended for Most Cases)

Use the built-in builder for:

- Static datasets
- Simple transformations
- Prototyping
- Read-only access

```go
catalog, _ := airport.NewCatalogBuilder().
    Schema("main").
    SimpleTable(airport.SimpleTableDef{
        Name:     "users",
        Schema:   userSchema,
        ScanFunc: scanUsers,
    }).
    Build()
```

### Custom Catalog Implementation

Implement interfaces directly for:

- Dynamic schemas (live reflection)
- DDL support (CREATE/DROP)
- DML support (INSERT/UPDATE/DELETE)
- Complex authorization
- External database proxying

## Step-by-Step Implementation

### Step 1: Define Your Data Model

Start by defining your Arrow schemas:

```go
import "github.com/apache/arrow-go/v18/arrow"

var userSchema = arrow.NewSchema([]arrow.Field{
    {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
    {Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
    {Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
    {Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
}, nil)
```

### Step 2: Implement Table

The Table interface is where data comes from:

```go
type UsersTable struct {
    db *sql.DB  // Your data source
}

func (t *UsersTable) Name() string {
    return "users"
}

func (t *UsersTable) Comment() string {
    return "User accounts"
}

func (t *UsersTable) ArrowSchema(columns []string) *arrow.Schema {
    if len(columns) == 0 {
        return userSchema
    }
    return catalog.ProjectSchema(userSchema, columns)
}

func (t *UsersTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // IMPORTANT: Must return data matching the FULL schema.
    // DuckDB handles column projection client-side.
    // The opts.Columns hint can be used to optimize your data source fetch,
    // but the returned RecordReader must have the full schema.

    rows, err := t.db.QueryContext(ctx, "SELECT * FROM users")
    if err != nil {
        return nil, err
    }

    // Convert to Arrow records with FULL schema
    return convertRowsToArrow(rows, t.ArrowSchema(nil))
}
```

### Step 3: Implement Schema

The Schema groups related tables:

```go
type MainSchema struct {
    name   string
    tables map[string]catalog.Table
}

func (s *MainSchema) Name() string {
    return s.name
}

func (s *MainSchema) Comment() string {
    return "Main application schema"
}

func (s *MainSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
    result := make([]catalog.Table, 0, len(s.tables))
    for _, t := range s.tables {
        result = append(result, t)
    }
    return result, nil
}

func (s *MainSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
    if t, ok := s.tables[name]; ok {
        return t, nil
    }
    return nil, nil  // Return nil for not found (not an error)
}

func (s *MainSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
    return nil, nil
}

func (s *MainSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
    return nil, nil
}

func (s *MainSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
    return nil, nil
}
```

### Step 4: Implement Catalog

The Catalog is the entry point:

```go
type MyCatalog struct {
    schemas map[string]catalog.Schema
}

func (c *MyCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    result := make([]catalog.Schema, 0, len(c.schemas))
    for _, s := range c.schemas {
        result = append(result, s)
    }
    return result, nil
}

func (c *MyCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
    if s, ok := c.schemas[name]; ok {
        return s, nil
    }
    return nil, nil
}
```

### Step 5: Wire It Together

```go
func main() {
    db, _ := sql.Open("postgres", connectionString)

    myCatalog := &MyCatalog{
        schemas: map[string]catalog.Schema{
            "main": &MainSchema{
                name: "main",
                tables: map[string]catalog.Table{
                    "users": &UsersTable{db: db},
                },
            },
        },
    }

    config := airport.ServerConfig{Catalog: myCatalog}
    grpcServer := grpc.NewServer(airport.ServerOptions(config)...)
    airport.NewServer(grpcServer, config)

    lis, _ := net.Listen("tcp", ":50051")
    grpcServer.Serve(lis)
}
```

## Adding DDL Support

### DynamicSchema for CREATE/DROP TABLE

```go
type DynamicMainSchema struct {
    mu     sync.RWMutex
    name   string
    tables map[string]catalog.Table
}

// Implement base Schema interface methods...
func (s *DynamicMainSchema) Name() string { return s.name }
func (s *DynamicMainSchema) Comment() string { return "" }
func (s *DynamicMainSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    result := make([]catalog.Table, 0, len(s.tables))
    for _, t := range s.tables {
        result = append(result, t)
    }
    return result, nil
}
func (s *DynamicMainSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    if t, ok := s.tables[name]; ok {
        return t, nil
    }
    return nil, nil
}
func (s *DynamicMainSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
    return nil, nil
}
func (s *DynamicMainSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
    return nil, nil
}
func (s *DynamicMainSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
    return nil, nil
}

// DynamicSchema interface methods
func (s *DynamicMainSchema) CreateTable(ctx context.Context, name string, schema *arrow.Schema, opts catalog.CreateTableOptions) (catalog.Table, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, exists := s.tables[name]; exists {
        switch opts.OnConflict {
        case catalog.OnConflictIgnore:
            return s.tables[name], nil
        case catalog.OnConflictReplace:
            delete(s.tables, name)
        default: // OnConflictError
            return nil, catalog.ErrAlreadyExists
        }
    }

    table := &InMemoryTable{
        name:   name,
        schema: schema,
        data:   make([]arrow.Record, 0),
    }
    s.tables[name] = table
    return table, nil
}

func (s *DynamicMainSchema) DropTable(ctx context.Context, name string, opts catalog.DropTableOptions) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, exists := s.tables[name]; !exists {
        if opts.IgnoreNotFound {
            return nil
        }
        return catalog.ErrNotFound
    }

    delete(s.tables, name)
    return nil
}

func (s *DynamicMainSchema) RenameTable(ctx context.Context, oldName, newName string, opts catalog.RenameTableOptions) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    table, exists := s.tables[oldName]
    if !exists {
        if opts.IgnoreNotFound {
            return nil
        }
        return catalog.ErrNotFound
    }

    if _, exists := s.tables[newName]; exists {
        return catalog.ErrAlreadyExists
    }

    delete(s.tables, oldName)
    s.tables[newName] = table
    return nil
}
```

### DynamicCatalog for CREATE/DROP SCHEMA

```go
type DynamicMyCatalog struct {
    mu      sync.RWMutex
    schemas map[string]catalog.Schema
}

func (c *DynamicMyCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    result := make([]catalog.Schema, 0, len(c.schemas))
    for _, s := range c.schemas {
        result = append(result, s)
    }
    return result, nil
}

func (c *DynamicMyCatalog) Schema(ctx context.Context, name string) (catalog.Schema, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    if s, ok := c.schemas[name]; ok {
        return s, nil
    }
    return nil, nil
}

func (c *DynamicMyCatalog) CreateSchema(ctx context.Context, name string, opts catalog.CreateSchemaOptions) (catalog.Schema, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if _, exists := c.schemas[name]; exists {
        return nil, catalog.ErrAlreadyExists
    }

    schema := &DynamicMainSchema{
        name:   name,
        tables: make(map[string]catalog.Table),
    }
    c.schemas[name] = schema
    return schema, nil
}

func (c *DynamicMyCatalog) DropSchema(ctx context.Context, name string, opts catalog.DropSchemaOptions) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    schema, exists := c.schemas[name]
    if !exists {
        if opts.IgnoreNotFound {
            return nil
        }
        return catalog.ErrNotFound
    }

    // Check for non-empty schema
    tables, _ := schema.Tables(ctx)
    if len(tables) > 0 {
        return catalog.ErrSchemaNotEmpty
    }

    delete(c.schemas, name)
    return nil
}
```

## Adding DML Support

### InsertableTable

```go
type InMemoryTable struct {
    mu     sync.RWMutex
    name   string
    schema *arrow.Schema
    data   []arrow.Record
}

func (t *InMemoryTable) Name() string { return t.name }
func (t *InMemoryTable) Comment() string { return "" }
func (t *InMemoryTable) ArrowSchema(columns []string) *arrow.Schema {
    if len(columns) == 0 {
        return t.schema
    }
    return catalog.ProjectSchema(t.schema, columns)
}

func (t *InMemoryTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    t.mu.RLock()
    defer t.mu.RUnlock()

    // Copy and retain records
    records := make([]arrow.Record, len(t.data))
    for i, rec := range t.data {
        rec.Retain()
        records[i] = rec
    }
    return array.NewRecordReader(t.schema, records)
}

func (t *InMemoryTable) Insert(ctx context.Context, rows array.RecordReader) (*catalog.DMLResult, error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    var count int64
    for rows.Next() {
        record := rows.Record()
        record.Retain()  // Keep the record
        t.data = append(t.data, record)
        count += record.NumRows()
    }

    if err := rows.Err(); err != nil {
        return nil, err
    }

    return &catalog.DMLResult{AffectedRows: count}, nil
}
```

## Adding Statistics Support

### StatisticsTable

```go
func (t *MyTable) ColumnStatistics(ctx context.Context, columnName string, columnType string) (*catalog.ColumnStats, error) {
    // Check if column exists
    found := false
    for i := 0; i < t.schema.NumFields(); i++ {
        if t.schema.Field(i).Name == columnName {
            found = true
            break
        }
    }
    if !found {
        return nil, catalog.ErrNotFound
    }

    // Return statistics (nil fields for unavailable stats)
    hasNotNull := true
    hasNull := false
    distinctCount := uint64(1000)

    return &catalog.ColumnStats{
        HasNotNull:    &hasNotNull,
        HasNull:       &hasNull,
        DistinctCount: &distinctCount,
        Min:           int64(0),      // Type must match column's Arrow type
        Max:           int64(10000),
    }, nil
}
```

## Best Practices

### Memory Management

Always release Arrow objects when done:

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    builder := array.NewRecordBuilder(memory.DefaultAllocator, t.schema)
    defer builder.Release()  // Release builder

    // ... populate builder ...

    record := builder.NewRecord()
    // Don't release record here - RecordReader takes ownership

    return array.NewRecordReader(t.schema, []arrow.Record{record})
}
```

### Error Handling

Return appropriate errors:

```go
func (s *MySchema) Table(ctx context.Context, name string) (catalog.Table, error) {
    // For "not found", return nil, nil (not an error)
    if t, ok := s.tables[name]; ok {
        return t, nil
    }
    return nil, nil

    // For actual errors (e.g., database connection issues)
    // return nil, fmt.Errorf("database error: %w", err)
}
```

Use sentinel errors for DDL/DML operations:

```go
// In your implementations, use these sentinel errors:
if exists {
    return nil, catalog.ErrAlreadyExists
}
if !found {
    return nil, catalog.ErrNotFound
}
```

### Concurrent Access

All implementations must be thread-safe:

```go
type SafeTable struct {
    mu     sync.RWMutex
    schema *arrow.Schema
    data   []arrow.Record
}

func (t *SafeTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    t.mu.RLock()
    defer t.mu.RUnlock()

    // Copy data reference while holding lock
    records := make([]arrow.Record, len(t.data))
    for i, rec := range t.data {
        rec.Retain()
        records[i] = rec
    }

    return array.NewRecordReader(t.schema, records)
}
```

### Important: Return Full Schema Data

Table.Scan must return data matching the full table schema. DuckDB handles column projection on the client side.

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    // opts.Columns is a hint for optimization, but you MUST return full schema
    // The server validates that returned schema matches the table's declared schema

    // CORRECT: Return all columns
    return fetchAllColumns(ctx, t.schema)

    // WRONG: Return only projected columns
    // projectedSchema := catalog.ProjectSchema(t.schema, opts.Columns)
    // return fetchProjected(ctx, projectedSchema)
}
```

### Context Cancellation

Check for cancellation in long operations:

```go
func (t *MyTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    records := make([]arrow.Record, 0)

    for {
        select {
        case <-ctx.Done():
            // Clean up and return
            for _, r := range records {
                r.Release()
            }
            return nil, ctx.Err()
        default:
            // Continue processing
        }

        record, done := fetchNextBatch()
        if done {
            break
        }
        records = append(records, record)
    }

    return array.NewRecordReader(t.schema, records)
}
```

## Common Patterns

### Database Proxy

Proxy an existing database through Flight:

```go
type PostgresProxy struct {
    db *sql.DB
}

func (p *PostgresProxy) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    rows, err := p.db.QueryContext(ctx, `
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
    `)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var schemas []catalog.Schema
    for rows.Next() {
        var name string
        rows.Scan(&name)
        schemas = append(schemas, &PostgresSchema{db: p.db, name: name})
    }
    return schemas, nil
}
```

### Caching Catalog

Cache catalog metadata for performance:

```go
type CachedCatalog struct {
    inner      catalog.Catalog
    cache      map[string]catalog.Schema
    cacheTime  time.Time
    cacheTTL   time.Duration
    mu         sync.RWMutex
}

func (c *CachedCatalog) Schemas(ctx context.Context) ([]catalog.Schema, error) {
    c.mu.RLock()
    if time.Since(c.cacheTime) < c.cacheTTL && c.cache != nil {
        schemas := make([]catalog.Schema, 0, len(c.cache))
        for _, s := range c.cache {
            schemas = append(schemas, s)
        }
        c.mu.RUnlock()
        return schemas, nil
    }
    c.mu.RUnlock()

    // Refresh cache
    c.mu.Lock()
    defer c.mu.Unlock()

    schemas, err := c.inner.Schemas(ctx)
    if err != nil {
        return nil, err
    }

    c.cache = make(map[string]catalog.Schema)
    for _, s := range schemas {
        c.cache[s.Name()] = s
    }
    c.cacheTime = time.Now()

    return schemas, nil
}
```

### Multi-tenant Catalog

Serve different data based on identity:

```go
func (t *TenantTable) Scan(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
    identity := airport.IdentityFromContext(ctx)
    if identity == "" {
        return nil, airport.ErrUnauthorized
    }

    // Filter data by tenant
    rows, err := t.db.QueryContext(ctx,
        "SELECT * FROM data WHERE tenant_id = $1", identity)
    if err != nil {
        return nil, err
    }

    return convertRowsToArrow(rows, t.schema)
}
```

## Examples

For complete working examples, see:

- [examples/basic](../examples/basic/) - Simple static catalog
- [examples/auth](../examples/auth/) - Authentication
- [examples/ddl](../examples/ddl/) - DDL operations
- [examples/dml](../examples/dml/) - DML operations
- [examples/functions](../examples/functions/) - Custom functions
- [examples/timetravel](../examples/timetravel/) - Time-travel queries
- [examples/tls](../examples/tls/) - TLS encryption

## Testing Your Implementation

Use integration tests with DuckDB:

```go
func TestMyCatalog(t *testing.T) {
    // Start your server
    cat := NewMyCatalog()
    server := startTestServer(t, cat)
    defer server.Stop()

    // Connect with DuckDB
    db, _ := sql.Open("duckdb", "")
    db.Exec("INSTALL airport FROM community")
    db.Exec("LOAD airport")
    db.Exec(fmt.Sprintf("ATTACH '' AS test (TYPE airport, LOCATION 'grpc://%s')", server.Address()))

    // Test queries
    rows, _ := db.Query("SELECT * FROM test.main.users")
    // Verify results...
}
```

See [tests/integration/](../tests/integration/) for comprehensive test examples.
