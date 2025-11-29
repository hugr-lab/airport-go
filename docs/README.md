# Airport Go Documentation

This directory contains detailed documentation for the airport-go package, a high-level Go implementation for building Apache Arrow Flight servers compatible with the DuckDB Airport Extension.

## Quick Links

- [Protocol Overview](protocol.md) - Airport protocol, Flight actions, and message formats
- [API Guide](api-guide.md) - Public API reference and interface documentation
- [Implementation Guide](implementation.md) - Guide for implementing custom catalogs

## Getting Started

For a quick introduction, see the main [README](../README.md).

For example implementations, see the [examples/](../examples/) directory.

## Architecture Overview

Airport Go follows an interface-based design where you implement catalog interfaces to expose your data through the Flight protocol:

```
┌─────────────────────────────────────────────────────────────┐
│                        DuckDB Client                        │
│                    (Airport Extension)                      │
└─────────────────────────────────────────────────────────────┘
                              │
                        Arrow Flight
                         (gRPC/HTTP2)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Airport Go Server                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Flight RPC Handler                      │   │
│  │  - GetFlightInfo  - DoGet  - DoAction               │   │
│  └─────────────────────────────────────────────────────┘   │
│                              │                              │
│                              ▼                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Catalog Interface Layer                    │   │
│  │  - Catalog  - Schema  - Table  - Functions          │   │
│  └─────────────────────────────────────────────────────┘   │
│                              │                              │
│                              ▼                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Your Implementation                     │   │
│  │  - CatalogBuilder (static) or Custom Catalog        │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Module Structure

The repository uses Go workspaces with three modules:

```
airport-go/
├── go.mod              # Main library module (no DuckDB dependency)
├── go.work             # Workspace configuration
├── examples/
│   └── go.mod          # Examples module
└── tests/
    └── go.mod          # Tests module (with DuckDB)
```

This separation keeps the main library lightweight while allowing integration tests and examples to use DuckDB.

## Document Index

### [Protocol Overview](protocol.md)

- Airport protocol specification
- Flight actions and their purposes
- Message serialization formats
- Authentication flow

### [API Guide](api-guide.md)

- Core interfaces (Catalog, Schema, Table)
- Dynamic interfaces (DynamicCatalog, DynamicSchema, DynamicTable)
- Function interfaces (ScalarFunction, TableFunction)
- CatalogBuilder API
- Server configuration

### [Implementation Guide](implementation.md)

- Step-by-step implementation walkthrough
- Best practices
- Performance considerations
- Common patterns and recipes

## Version Compatibility

| airport-go | Go     | Arrow-Go | DuckDB Airport Extension |
|------------|--------|----------|--------------------------|
| v0.x       | 1.25+  | v18.x    | Latest community build   |

## External Resources

- [DuckDB Airport Extension](https://airport.query.farm) - Client-side extension
- [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) - Protocol specification
- [Arrow Go](https://github.com/apache/arrow/go) - Go implementation of Apache Arrow
- [gRPC](https://grpc.io/) - Transport layer
