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

### 004-column-statistics

- [x] StatisticsTable interface in catalog/table.go
- [x] ColumnStats struct with all 7 statistics fields
- [x] column_statistics action handler in flight/doaction_statistics.go
- [x] DuckDB type to Arrow type mapping
- [x] Arrow IPC serialization for RecordBatch response
- [x] can_produce_statistics metadata for tables
- [x] Integration tests with DuckDB client

Reference: <https://airport.query.farm/server_action_column_statistics.html>

### 005-module-reorganization

- [x] Separate go.mod for examples
- [x] Separate go.mod for integration tests
- [x] Refactor benchmarks to use DuckDB as client
- [x] Remove unused packages from go.mod
- [x] Update documentation for new structure
- [x] Create a comprehensive documentation for protocol and package usage in the separate docs folder
- [x] Go workspace configuration (go.work)

### 006-returning-optimization

- [ ] Investigate how to get returning column names from DuckDB (it seems that currently we return what we receive from DuckDB)
- [ ] Provide the parameter with columns to be returned in RETURNING clause in DML table methods
- [ ] Create integration tests to verify that only requested columns are returned in RETURNING clause, even if they are not provided in the incoming RecordBatch

## Future Features

### Filter Pushdown

- [ ] Parse filters to basic expressions (AND, OR, =, <, >, IN)
- [ ] Return parsed filters in ScanOptions
- [ ] Integration tests for filter handling
