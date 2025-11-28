# airport-go Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-11-25

## Active Technologies
- Go 1.21+ (001-repo-preparation)
- Implementation-defined (spec remains storage-agnostic); Airport server abstracts storage via catalog.Catalog interface (001-repo-preparation)
- Go 1.25 (as specified in go.mod) (002-dml-transactions)
- N/A (storage-agnostic via catalog.Catalog interface) (002-dml-transactions)

- Go 1.25+ (recommended for latest stdlib features and performance) (001-001-flight-server)

## Project Structure

```text
# Repository root contains airport package (github.com/hugr-lab/airport-go)
*.go                     # Root package files (server.go, config.go, etc.)
catalog/                 # Subpackage: catalog interfaces and builder
flight/                  # Subpackage: Flight RPC handler implementations
auth/                    # Subpackage: authentication implementations
internal/                # Internal utilities (not public API)
examples/                # Example usage code
testutil/                # Test utilities for integration tests

tests/
├── unit/                # Unit tests (co-located with source: *_test.go)
├── integration/         # Integration tests with DuckDB
└── benchmarks/          # Performance benchmarks
```

## Commands

# Add commands for Go 1.25+ (recommended for latest stdlib features and performance)

## Code Style

Go 1.25+ (recommended for latest stdlib features and performance): Follow standard conventions

## Recent Changes
- 002-dml-transactions: Added Go 1.25 (as specified in go.mod)
- 001-repo-preparation: Added Go 1.21+

- 001-001-flight-server: Added Go 1.25+ (recommended for latest stdlib features and performance)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
