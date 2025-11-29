# Feature Specification: DDL Operations

**Feature Branch**: `003-ddl-operations`
**Created**: 2025-11-29
**Status**: Draft
**Input**: User description: "DDL operations implementation - CreateSchema, DropSchema, CreateTable, DropTable, add_column, remove_column actions with DynamicCatalog/Schema/Table interfaces"

## Clarifications

### Session 2025-11-29

- Q: What happens when drop_schema is called on a schema containing tables? → A: Error if schema contains tables (require explicit table drops first)

## User Scenarios & Testing

### User Story 1 - Create Schema Dynamically (Priority: P1)

Application developers need to create new schemas within their Airport-managed catalog at runtime, enabling them to organize tables logically and support multi-tenant architectures.

**Why this priority**: Schema creation is the foundation of DDL operations - tables cannot be created without a schema to contain them. This is the prerequisite for all other DDL operations.

**Independent Test**: Can be fully tested by calling the create_schema action with schema name and verifying the new schema appears in subsequent list_schemas calls. Delivers immediate value by enabling dynamic schema organization.

**Acceptance Scenarios**:

1. **Given** a configured DynamicCatalog, **When** a developer calls the create_schema action with a valid schema name, **Then** the schema is created and becomes visible in catalog queries
2. **Given** a create_schema request with an optional comment and tags, **When** the operation completes, **Then** the schema metadata is stored and retrievable
3. **Given** a schema name that already exists, **When** create_schema is called, **Then** the system returns an appropriate error indicating the schema already exists
4. **Given** a Catalog that does not implement DynamicCatalog, **When** create_schema is called, **Then** the system returns an error indicating schema creation is not supported

---

### User Story 2 - Create Table Dynamically (Priority: P1)

Application developers need to create new tables within existing schemas at runtime, enabling them to define data structures programmatically based on application requirements or user configurations.

**Why this priority**: Table creation is equally fundamental to schema creation - together they form the core DDL capability. Most applications need to create tables before they can store data.

**Independent Test**: Can be tested by creating a table via create_table action with an Arrow schema definition, then verifying the table appears in list_tables and can be queried. Works independently once schema exists.

**Acceptance Scenarios**:

1. **Given** an existing schema in a DynamicCatalog, **When** a developer calls create_table with a valid table name and Arrow schema, **Then** the table is created with the specified columns
2. **Given** a create_table request with on_conflict="ignore", **When** the table already exists, **Then** the operation succeeds without error
3. **Given** a create_table request with on_conflict="replace", **When** the table already exists, **Then** the existing table is replaced with the new definition
4. **Given** a create_table request with on_conflict="error" (default), **When** the table already exists, **Then** the system returns an error indicating the table exists
5. **Given** a Schema that does not implement DynamicSchema, **When** create_table is called, **Then** the system returns an error indicating table creation is not supported

---

### User Story 3 - Drop Schema (Priority: P2)

Application developers need to remove schemas that are no longer needed, enabling cleanup of unused namespace and supporting schema lifecycle management.

**Why this priority**: Dropping schemas is important for cleanup but less frequent than creation. It depends on schema creation being available first.

**Independent Test**: Can be tested by creating a schema, then dropping it, and verifying it no longer appears in list_schemas. Delivers value by enabling full schema lifecycle management.

**Acceptance Scenarios**:

1. **Given** an existing schema with no tables, **When** drop_schema is called, **Then** the schema is removed from the catalog
2. **Given** an existing schema containing tables, **When** drop_schema is called, **Then** the system returns an error indicating tables must be dropped first
3. **Given** a schema that does not exist, **When** drop_schema is called with ignore_not_found=false, **Then** the system returns an error
4. **Given** a schema that does not exist, **When** drop_schema is called with ignore_not_found=true, **Then** the operation succeeds without error
5. **Given** a Catalog that does not implement DynamicCatalog, **When** drop_schema is called, **Then** the system returns an error indicating schema deletion is not supported

---

### User Story 4 - Drop Table (Priority: P2)

Application developers need to remove tables that are no longer needed, enabling cleanup of obsolete data structures and supporting table lifecycle management.

**Why this priority**: Dropping tables complements table creation. Important for cleanup and migration scenarios.

**Independent Test**: Can be tested by creating a table, then dropping it, and verifying it no longer appears in table listings. Works independently once tables can be created.

**Acceptance Scenarios**:

1. **Given** an existing table, **When** drop_table is called, **Then** the table is removed from the schema
2. **Given** a table that does not exist, **When** drop_table is called with ignore_not_found=false, **Then** the system returns an error
3. **Given** a table that does not exist, **When** drop_table is called with ignore_not_found=true, **Then** the operation succeeds without error
4. **Given** a Schema that does not implement DynamicSchema, **When** drop_table is called, **Then** the system returns an error indicating table deletion is not supported

---

### User Story 5 - Add Column to Table (Priority: P3)

Application developers need to add new columns to existing tables, enabling schema evolution as application requirements change over time.

**Why this priority**: Column modification is a more advanced DDL operation. Most initial development focuses on creating complete table structures, with column additions coming during maintenance phases.

**Independent Test**: Can be tested by creating a table, adding a new column via add_column, then verifying the table schema includes the new column. Delivers value by enabling schema evolution.

**Acceptance Scenarios**:

1. **Given** an existing table, **When** add_column is called with a column schema, **Then** the new column is added to the table
2. **Given** a column that already exists, **When** add_column is called with if_column_not_exists=false, **Then** the system returns an error
3. **Given** a column that already exists, **When** add_column is called with if_column_not_exists=true, **Then** the operation succeeds without error
4. **Given** a Table that does not implement DynamicTable, **When** add_column is called, **Then** the system returns an error indicating column modification is not supported

---

### User Story 6 - Remove Column from Table (Priority: P3)

Application developers need to remove columns from existing tables, enabling schema evolution by removing deprecated or unused fields.

**Why this priority**: Column removal is complementary to column addition but even less frequent, typically used during major schema refactoring.

**Independent Test**: Can be tested by creating a table with multiple columns, removing one via remove_column, then verifying the table schema no longer includes that column.

**Acceptance Scenarios**:

1. **Given** an existing table with multiple columns, **When** remove_column is called with a column name, **Then** the column is removed from the table
2. **Given** a column that does not exist, **When** remove_column is called with if_column_exists=false, **Then** the system returns an error
3. **Given** a column that does not exist, **When** remove_column is called with if_column_exists=true, **Then** the operation succeeds without error
4. **Given** remove_column is called with cascade=true, **When** dependent objects exist, **Then** the column and its dependents are removed
5. **Given** a Table that does not implement DynamicTable, **When** remove_column is called, **Then** the system returns an error indicating column modification is not supported

---

### Edge Cases

- What happens when create_schema or create_table is called on a static (builder-created) catalog? → Returns error indicating operation not supported (FR-010)
- How does the system handle concurrent DDL operations on the same schema or table? → Implementation must be goroutine-safe (FR-014); actual conflict resolution delegated to user implementation
- What occurs if a table with foreign key relationships is dropped? → Out of scope; foreign key enforcement delegated to user implementation
- How does the system respond when drop_schema is called on a schema containing tables? → Returns error requiring tables be dropped first (FR-016)
- What happens if add_column is called with an invalid Arrow type? → Returns validation error (FR-015)
- How does the system handle remove_column when the column is the only column in the table? → Delegated to user implementation; server passes request to DynamicTable

## Requirements

### Functional Requirements

- **FR-001**: System MUST implement create_schema action accepting catalog_name, schema name, optional comment, and optional tags
- **FR-002**: System MUST implement drop_schema action accepting type, catalog_name, schema_name, name, and ignore_not_found parameters
- **FR-003**: System MUST implement create_table action accepting catalog_name, schema_name, table_name, arrow_schema, on_conflict, and constraint arrays
- **FR-004**: System MUST implement drop_table action accepting type, catalog_name, schema_name, name, and ignore_not_found parameters
- **FR-005**: System MUST implement add_column action accepting catalog, schema, name, column_schema, ignore_not_found, and if_column_not_exists parameters
- **FR-006**: System MUST implement remove_column action accepting catalog, schema, name, removed_column, ignore_not_found, if_column_exists, and cascade parameters
- **FR-007**: System MUST define DynamicCatalog interface extending Catalog with CreateSchema and DropSchema methods
- **FR-008**: System MUST define DynamicSchema interface extending Schema with CreateTable and DropTable methods
- **FR-009**: System MUST define DynamicTable interface extending Table with AddColumn and RemoveColumn methods
- **FR-010**: System MUST return appropriate errors when DDL operations are attempted on non-dynamic implementations
- **FR-016**: System MUST reject drop_schema when the schema contains tables, returning an error indicating tables must be dropped first
- **FR-011**: System MUST serialize response using msgpack format for create_schema (AirportSerializedContentsWithSHA256Hash)
- **FR-012**: System MUST return FlightInfo for create_table, add_column, and remove_column actions
- **FR-013**: System MUST support on_conflict parameter with values "error", "ignore", and "replace" for create_table
- **FR-014**: All DDL operations MUST be goroutine-safe
- **FR-015**: System MUST validate Arrow schema format for create_table and add_column operations

### Key Entities

- **DynamicCatalog**: Extension of Catalog interface that supports schema creation and deletion at runtime
- **DynamicSchema**: Extension of Schema interface that supports table creation and deletion at runtime
- **DynamicTable**: Extension of Table interface that supports column addition and removal at runtime
- **DDL Action Parameters**: Request structures for each DDL operation (create_schema, drop_schema, create_table, drop_table, add_column, remove_column)
- **Conflict Resolution**: Strategy for handling existing objects during create operations (error, ignore, replace)

## Success Criteria

### Measurable Outcomes

- **SC-001**: Developers can create and drop schemas dynamically in catalogs that implement DynamicCatalog
- **SC-002**: Developers can create and drop tables dynamically in schemas that implement DynamicSchema
- **SC-003**: Developers can add and remove columns dynamically on tables that implement DynamicTable
- **SC-004**: System correctly rejects DDL operations on static (non-dynamic) implementations with informative error messages
- **SC-005**: All DDL action parameter formats match the DuckDB Airport Extension specification
- **SC-006**: DDL operations complete without data corruption when executed concurrently (thread-safety verified by race detector)
- **SC-007**: Integration tests demonstrate full DDL lifecycle (create schema, create table, modify columns, drop table, drop schema)

## Assumptions

- **A-001**: The DuckDB Airport Extension protocol for DDL actions is stable and will not change during implementation
- **A-002**: Arrow schema serialization format for create_table and add_column uses IPC wire format
- **A-003**: Static catalogs (built with NewCatalogBuilder) will not implement dynamic interfaces - they remain immutable
- **A-004**: The existing msgpack encoding infrastructure can be reused for DDL request/response serialization
- **A-005**: Constraint handling (NOT NULL, UNIQUE, CHECK) in create_table is metadata-only; enforcement is delegated to user implementations
- **A-006**: Cascade behavior in remove_column is defined by user implementation; the server only passes the flag

## Scope

### In Scope

- create_schema action implementation
- drop_schema action implementation
- create_table action implementation
- drop_table action implementation
- add_column action implementation
- remove_column action implementation
- DynamicCatalog interface definition
- DynamicSchema interface definition
- DynamicTable interface definition
- Error handling for non-dynamic implementations
- Integration tests for all DDL operations
- Example code demonstrating dynamic catalog usage

### Out of Scope

- rename_table action (future feature)
- rename_column action (future feature)
- change_column_type action (future feature)
- add_constraint action (future feature)
- set_not_null, drop_not_null, set_default actions (future feature)
- Foreign key constraint enforcement
- Cascade delete behavior for schemas containing tables (user implementation responsibility)
- Transaction support for DDL operations (DDL operations are auto-committed)
- Schema migration tooling or versioning

## Dependencies

- **D-001**: Arrow Flight RPC infrastructure for DoAction handling
- **D-002**: Existing Catalog, Schema, and Table interfaces from catalog package
- **D-003**: msgpack serialization for action request/response encoding
- **D-004**: Arrow IPC format for schema serialization in create_table and add_column
