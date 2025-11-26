# Research: Airport Go Flight Server Package

**Date**: 2025-11-25
**Context**: Implementation planning for Apache Arrow Flight server package compatible with DuckDB Airport Extension

## Overview

This document consolidates research findings for building the `airport` Go package. Research focused on Arrow Flight SDK patterns, DuckDB Airport protocol requirements, serialization best practices, and gRPC service integration patterns.

---

## 1. Arrow Flight Go SDK Patterns

### Decision

Use Apache Arrow Go v14+ with `BaseFlightServer` embedding pattern.

### Rationale

- Official Apache Arrow Go SDK (`github.com/apache/arrow/go/v14/arrow/flight`) provides mature, well-tested Flight RPC implementation
- BaseFlightServer embedding ensures forward compatibility with Flight protocol changes
- SDK handles gRPC registration, message serialization, and streaming mechanics automatically
- Latest versions (v14+, published in 2024) include performance improvements and bug fixes

### Alternatives Considered

- **Direct gRPC implementation**: Rejected due to complexity of manually implementing Flight protocol, schema serialization, and streaming patterns
- **Older Arrow versions (v10-v12)**: Rejected due to missing features and known issues fixed in recent releases
- **Third-party Flight libraries**: None exist with comparable maturity for Go

### Implementation Notes

**Server Structure Pattern**:
```go
type FlightServer struct {
    flight.BaseFlightServer  // MUST embed for forward compatibility
    catalog   Catalog
    allocator memory.Allocator
}
```

**Key Methods to Implement**:
1. `DoGet` - Server streaming for SELECT queries (receives ticket, streams Arrow batches)
2. `DoPut` - Client streaming for INSERT operations (receives Arrow batches, returns ack)
3. `DoExchange` - Bidirectional streaming for UPDATE/DELETE/Functions
4. `DoAction` - Server streaming for DDL and metadata operations
5. `GetFlightInfo` - Returns metadata about available data (schema, endpoints, estimates)

**Memory Management**:
- Use `memory.DefaultAllocator` (goroutine-safe, no configuration needed)
- Follow reference counting pattern: `defer record.Release()`
- Use `CheckedAllocator` in tests to verify no leaks
- Critical: Always call `Release()` - memory won't be freed until refcount reaches zero

**IPC Reader/Writer Pattern**:
```go
// DoGet streaming
reader, _ := fetchData(ticket)
defer reader.Release()

writer := flight.NewRecordWriter(stream, ipc.WithSchema(reader.Schema()))
defer writer.Close()

for reader.Next() {
    writer.Write(reader.Record())
}
```

---

## 2. Catalog Serialization for DuckDB Airport

### Decision

Use Arrow IPC for catalog data + ZStandard compression + MessagePack for parameters.

### Rationale

- DuckDB Airport Extension expects Flight SQL protocol catalog schemas
- Arrow Flight SQL specifies standard schemas for GetCatalogs, GetDbSchemas, GetTables
- ZStandard provides optimal balance of compression ratio (~60-70%) and speed
- MessagePack is compact, fast, and well-suited for passing structured parameters

### Alternatives Considered

- **JSON for everything**: Rejected due to 3-5x larger payload sizes and slower parsing
- **Protocol Buffers**: Rejected as Airport extension uses MessagePack convention
- **Gzip/LZ4 compression**: ZStandard provides better compression ratios with comparable speed

### Implementation Notes

**Flight SQL Catalog Schemas**:

- **GetCatalogs**: Returns `catalog_name: utf8 not null`
- **GetDbSchemas**: Returns `catalog_name: utf8 (nullable)`, `db_schema_name: utf8 not null`
- **GetTables**: Returns catalog/schema/table names, `table_type: utf8`, `table_schema: bytes` (optional IPC-serialized schema)

**ZStandard Compression** (`github.com/klauspost/compress/zstd`):
```go
// Create reusable encoder (store in server struct)
encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
defer encoder.Close()

compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
```

**Performance**:
- Reuse encoders/decoders to eliminate allocations
- Pre-allocate destination buffers
- Use `SpeedDefault` (level 3) for balanced performance

**Catalog Action Handler Pattern**:
1. Fetch schemas from catalog interface
2. Build Arrow record with schema metadata
3. Serialize to Arrow IPC format
4. Compress with ZStandard
5. Return as Flight Result

---

## 3. MessagePack Parameter Handling

### Decision

Use `github.com/vmihailenco/msgpack/v5` library.

### Rationale

- Excellent performance: ~12,814 ns/op vs JSON's 69,438 ns/op (5x faster)
- Idiomatic Go API with struct tags support (compatible with `json` tags)
- Efficient memory usage (26 allocs/op for typical structs)
- Active maintenance and Go modules support

### Alternatives Considered

- **ugorji/go codec**: More features but inconsistent benchmark results, complex API
- **hashicorp/go-msgpack**: Lower-level, requires more manual handling
- **Standard JSON**: 5x slower, larger payload sizes

### Implementation Notes

**Deserialization Pattern**:
```go
type ScanParams struct {
    Schema  string   `msgpack:"schema"`
    Table   string   `msgpack:"table"`
    Columns []string `msgpack:"columns,omitempty"`
    Filters []byte   `msgpack:"filters,omitempty"`
}

var params ScanParams
msgpack.Unmarshal(data, &params)
```

**Struct Tag Optimization**:
- `msgpack:"name"` - Required field
- `msgpack:"name,omitempty"` - Omit if empty
- `msgpack:"-"` - Never serialize
- `msgpack:"name,as_array"` - Encode as array

**Performance Best Practices**:
- Reuse encoder/decoder for hot paths
- Use `DisallowUnknownFields()` for strict validation
- Prefer RFC3339 strings or Unix timestamps for time values

---

## 4. gRPC Service Registration Patterns

### Decision

Register Flight service on existing `*grpc.Server` using `grpc.ServiceRegistrar` interface.

### Rationale

- gRPC allows multiple services on single server (Flight + custom services)
- `ServiceRegistrar` interface provides flexibility for testing and custom wrappers
- Standard pattern used by all gRPC code generators
- Enables adding Flight service alongside other services (health checks, reflection)

### Alternatives Considered

- **Create separate gRPC server for Flight**: Rejected due to port/TLS duplication overhead
- **Dynamic service registration**: Unnecessary complexity for static Flight service
- **Custom gRPC server wrapper**: Rejected as standard server works perfectly

### Implementation Notes

**Registration Pattern**:
```go
func RegisterAirportServer(grpcServer *grpc.Server, catalog Catalog) error {
    flightServer := &FlightServer{
        catalog:   catalog,
        allocator: memory.DefaultAllocator,
    }

    flight.RegisterFlightServiceServer(grpcServer, flightServer)
    reflection.Register(grpcServer) // Optional: for grpcurl debugging

    return nil
}
```

**Multiple Services on Single Port**:
```go
grpcServer := grpc.NewServer(...)
flight.RegisterFlightServiceServer(grpcServer, myFlightServer)
health.RegisterHealthServer(grpcServer, healthServer)
reflection.Register(grpcServer)
```

**Critical**: Register all services before calling `Serve()` - registration is not thread-safe.

---

## 5. Context Propagation in gRPC

### Decision

Use `metadata.FromIncomingContext()` + `grpc-ecosystem/go-grpc-middleware/auth` interceptor pattern.

### Rationale

- Official gRPC metadata package provides standard context propagation
- Interceptor pattern centralizes authentication logic (no duplication across handlers)
- Context values propagate automatically through handler chain
- Widely adopted pattern in Go gRPC ecosystem
- Supports both unary and stream interceptors

### Alternatives Considered

- **Manual metadata extraction in each handler**: Rejected due to code duplication
- **Custom interceptor without middleware library**: Possible but reinvents wheel
- **HTTP headers inspection**: Rejected as gRPC uses metadata, not HTTP headers directly

### Implementation Notes

**Authentication Interceptor**:
```go
func (s *Server) authFunc(ctx context.Context) (context.Context, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, status.Error(codes.Unauthenticated, "no metadata")
    }

    authHeaders := md.Get("authorization")
    token := strings.TrimPrefix(authHeaders[0], "Bearer ")

    identity, err := s.authenticator.Authenticate(ctx, token)
    if err != nil {
        return nil, status.Error(codes.Unauthenticated, "invalid token")
    }

    req := &Request{Token: token, Identity: identity}
    ctx = context.WithValue(ctx, requestContextKey, req)
    return ctx, nil
}
```

**Server Setup with Interceptors**:
```go
grpcServer := grpc.NewServer(
    grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(server.authFunc)),
    grpc.StreamInterceptor(grpc_auth.StreamServerInterceptor(server.authFunc)),
)
```

**Using Request in Handlers**:
```go
func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
    req, ok := RequestFromContext(stream.Context())
    if !ok {
        return status.Error(codes.Internal, "no request context")
    }

    if !s.authorize(req.Identity, "read", ticket.Table) {
        return status.Error(codes.PermissionDenied, "access denied")
    }

    // Continue with authorized request...
}
```

**Critical Gotchas**:
- Metadata keys are lowercase (gRPC auto-converts)
- Use `FromIncomingContext` on server side, not `FromOutgoingContext`
- Keys ending in "-bin" get automatic base64 encoding/decoding
- Appropriate status codes: `Unauthenticated` for auth failures, `PermissionDenied` for authz failures

---

## Summary of Key Decisions

| Area | Choice | Primary Reason |
|------|--------|----------------|
| **Flight SDK** | Apache Arrow Go v14+ | Official, mature, actively maintained |
| **Memory Management** | `memory.DefaultAllocator` | Goroutine-safe, no config needed |
| **Catalog Serialization** | Arrow IPC + ZStandard | Flight SQL standard + optimal compression |
| **Parameters** | vmihailenco/msgpack/v5 | 5x faster than JSON, idiomatic API |
| **Compression** | klauspost/compress/zstd | Best Go ZStandard implementation |
| **gRPC Registration** | ServiceRegistrar pattern | Flexible, standard, supports multiple services |
| **Authentication** | grpc-middleware auth interceptor | Centralized, standard pattern, clean propagation |

---

## Next Steps

Phase 1 will use these findings to generate:
1. **data-model.md**: Interface definitions for Catalog, Schema, Table entities
2. **contracts/**: Go interface signatures for Flight RPC handlers and catalog builder API
3. **quickstart.md**: 30-line example demonstrating basic server setup with static catalog
