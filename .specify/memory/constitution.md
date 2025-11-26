<!--
Sync Impact Report:
- Version change: 0.0.0 → 1.0.0
- Initial constitution ratification
- Principles defined:
  1. Code Quality (idiomatic Go)
  2. Testing Standards (unit + integration)
  3. User Experience Consistency (stable API)
  4. Performance Requirements (efficiency)
- Templates requiring updates:
  ✅ plan-template.md - Constitution Check section ready
  ✅ spec-template.md - No changes required (technology-agnostic requirements)
  ✅ tasks-template.md - Test-driven approach aligns with Testing Standards
- Follow-up TODOs: None
-->

# Airport Go Package Constitution

## Core Principles

### I. Code Quality

The airport-go package MUST follow idiomatic Go style at all times:

- All code MUST comply with `gofmt`, `golangci-lint`, and Go conventions documented in [Effective Go](https://go.dev/doc/effective_go) and [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Simplicity over cleverness: Code MUST be readable, maintainable, and avoid unnecessary abstractions
- All public APIs MUST be explicitly documented with godoc-compatible comments
- No silent failures: Errors MUST be handled explicitly; library code MUST NOT panic

**Rationale**: Idiomatic Go code ensures consistency, maintainability, and alignment with ecosystem expectations. Explicit error handling prevents surprises and makes debugging tractable.

### II. Testing Standards

Testing is non-negotiable for quality assurance:

- All new code MUST include unit tests
- Critical logic (data transformations, protocol handling, authentication) MUST have integration tests
- Tests MUST be deterministic and isolated from external environments (no live services, no network dependencies unless explicitly mocked)
- CI MUST run tests with race detector (`-race`) and static analysis on every pull request

**Rationale**: Tests catch regressions early, document expected behavior, and enable confident refactoring. Race detector ensures concurrency safety. Deterministic tests prevent flaky builds.

### III. User Experience Consistency

The public API is a contract with users and MUST remain stable:

- Public API MUST be predictable, stable, and consistent across all modules
- Function and type names MUST follow Go naming idioms (no `GetX` prefixes, proper acronym capitalization)
- Documentation and examples MUST enable quick adoption by new users
- Breaking changes MUST be avoided unless absolutely necessary; when unavoidable, MUST follow [semantic versioning](https://semver.org/)

**Rationale**: API stability builds trust. Predictable naming reduces cognitive load. Clear documentation and examples lower the barrier to entry and reduce support burden.

### IV. Performance Requirements

The package MUST be efficient and non-blocking:

- Aim for efficient memory usage; avoid unnecessary allocations (profile with `pprof` when optimizing)
- Use streaming or incremental processing where applicable (especially for large Arrow data)
- Avoid blocking operations; use `context.Context` for cancellation of long-running tasks
- Performance hotspots MUST be profiled and optimized as needed

**Rationale**: Efficient memory usage matters for high-throughput data processing. Streaming prevents memory exhaustion with large datasets. Context cancellation enables graceful shutdown and prevents resource leaks.

## Development Workflow

### Code Review Gates

All pull requests MUST pass these gates before merge:

1. **Linting & Formatting**: `gofmt` and `golangci-lint` pass with no warnings
2. **Tests**: All unit and integration tests pass with race detector enabled
3. **Documentation**: Public APIs have godoc comments; complex logic has inline comments
4. **API Review**: Breaking changes require explicit approval and version bump justification

### Testing Requirements

- **Unit tests**: Required for all business logic, utilities, and data transformations
- **Integration tests**: Required for protocol implementations (Flight RPC handlers), authentication flows, and cross-module interactions
- **Contract tests**: Required for external interfaces (Airport protocol compliance)

### Complexity Justification

If a design violates the "Simplicity" principle (e.g., introduces abstractions, complex patterns, or performance optimizations that reduce clarity), the pull request description MUST include:

- **Why Needed**: Specific problem being solved
- **Simpler Alternative Rejected Because**: Concrete reasons why the simpler approach is insufficient

## Governance

### Constitution Authority

This constitution supersedes all other practices. When in conflict:

1. Constitution principles take precedence
2. Clarification through discussion and amendment if needed
3. Pragmatic exceptions documented in Complexity Justification (rare)

### Amendments

Constitution amendments require:

1. **Proposal**: Document proposed change with rationale
2. **Review**: Maintainer approval (unanimous for principle changes)
3. **Migration Plan**: For changes affecting existing code, provide migration path
4. **Version Bump**: Increment `CONSTITUTION_VERSION` according to semantic versioning

### Compliance Review

- All PRs MUST verify compliance with constitution principles
- Reviewers MUST call out violations and request justification or refactoring
- CI MUST enforce automated gates (linting, tests, race detector)

**Version**: 1.0.0 | **Ratified**: 2025-11-25 | **Last Amended**: 2025-11-25
