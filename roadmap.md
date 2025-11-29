# Airport Go Roadmap

## Completed Features

### 001-repo-preparation

- [x] Repository structure and CI/CD
- [x] Core interfaces (Catalog, Schema, Table)
- [x] golangci-lint configuration

### 001-001-flight-server

- [x] Flight RPC server implementation
- [x] Catalog builder API
- [x] Bearer token authentication
- [x] Table functions support
- [x] Time travel queries

### 002-dml-transactions

- [x] INSERT operations with RETURNING
- [x] UPDATE operations with RETURNING
- [x] DELETE operations with RETURNING
- [x] Transaction manager interface
- [x] Column projection (ArrowSchema with columns parameter)

### 003-ddl-operations

- [x] CreateSchema, DropSchema actions
- [x] CreateTable, DropTable actions
- [x] AddColumn, RemoveColumn actions
- [x] DynamicCatalog/Schema/Table interfaces
- [x] CREATE TABLE AS SELECT support (CTAS)
- [x] Integration tests and examples
- [x] DuckDB metadata verification

Reference: <https://airport.query.farm/server_actions.html>

## Future Features

### column_statistics action

- [ ] Implement column_statistics action
- [ ] Integration tests

### Filter Pushdown

- [ ] Parse filters to basic expressions (AND, OR, =, <, >, IN)
- [ ] Return parsed filters in ScanOptions
- [ ] Integration tests for filter handling

### Module Reorganization

- [ ] Separate go.mod for examples
- [ ] Separate go.mod for integration tests
- [ ] Refactor benchmarks to use DuckDB as client
