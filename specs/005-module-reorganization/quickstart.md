# Quickstart: Module Reorganization

**Feature**: 005-module-reorganization
**Date**: 2025-11-29

This guide explains how to work with the new multi-module structure.

## For Library Users

**No changes required.** Import the library as before:

```go
import "github.com/hugr-lab/airport-go"
import "github.com/hugr-lab/airport-go/catalog"
```

The library now has fewer dependencies - DuckDB is no longer pulled in.

---

## For Contributors

### Initial Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/hugr-lab/airport-go.git
   cd airport-go
   ```

2. Create a local workspace:
   ```bash
   go work init . ./examples ./tests
   ```

3. Verify setup:
   ```bash
   go work sync
   go build ./...
   ```

### Running Tests

**Unit tests** (main module, no DuckDB required):
```bash
go test ./...
```

**Integration tests** (requires DuckDB):
```bash
cd tests
go test ./integration/...
```

**Benchmarks**:
```bash
cd tests
go test -bench=. ./benchmarks/...
```

**All tests with race detector**:
```bash
go test -race ./...
cd tests && go test -race ./...
```

### Running Examples

Each example is a standalone application:

```bash
cd examples/basic
go run .
```

In another terminal, test with DuckDB:
```sql
ATTACH '' AS demo (TYPE airport, LOCATION 'grpc://localhost:50051');
SELECT * FROM demo.main.users;
```

### Before Committing

Always synchronize and tidy:

```bash
# From repository root
go work sync
go mod tidy
cd examples && go mod tidy && cd ..
cd tests && go mod tidy && cd ..

# Review changes
git diff go.mod examples/go.mod tests/go.mod
```

---

## Directory Structure

```
airport-go/
├── go.mod                    # Main library (minimal deps)
├── go.work                   # LOCAL ONLY (not committed)
│
├── docs/                     # Documentation
│   ├── README.md
│   ├── protocol.md
│   ├── api-guide.md
│   └── implementation.md
│
├── examples/                 # Separate module
│   ├── go.mod                # github.com/hugr-lab/airport-go/examples
│   ├── auth/
│   ├── basic/
│   └── ...
│
└── tests/                    # Separate module
    ├── go.mod                # github.com/hugr-lab/airport-go/tests
    ├── integration/
    └── benchmarks/
```

---

## Common Tasks

### Add a New Dependency to Main Module

```bash
go get github.com/some/package
go mod tidy
```

### Add a Test Dependency

```bash
cd tests
go get github.com/some/test-package
go mod tidy
```

### Add a New Example

1. Create directory: `examples/myexample/`
2. Add `main.go` with `package main`
3. Run: `cd examples && go mod tidy`

### Update Arrow-Go Version

Update all modules to maintain consistency:

```bash
go get github.com/apache/arrow-go/v18@latest
cd examples && go get github.com/apache/arrow-go/v18@latest
cd tests && go get github.com/apache/arrow-go/v18@latest
```

---

## Troubleshooting

### "module not found" errors

Ensure go.work exists and includes all modules:
```bash
go work init . ./examples ./tests
```

### DuckDB not found during main module tests

Main module tests should NOT require DuckDB. If they do, move them to `tests/integration/`.

### Conflicting dependencies

Run `go work sync` to synchronize all modules.

### CI failing but local works

CI doesn't use go.work. Ensure each module builds independently:
```bash
# Test as CI sees it
go build ./...                    # Main module
cd examples && go build ./...     # Examples
cd tests && go build ./...        # Tests
```
