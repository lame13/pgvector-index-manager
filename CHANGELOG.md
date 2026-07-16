# Changelog

All notable changes to this project will be documented in this file.

The project uses semantic versioning.

## [0.1.0] - 2026-07-16

### Added

- Initial release of pgvector-index-manager.
- Go CLI with `status`, `plan`, `apply`, and `version` commands.
- Catalog inspection for pgvector HNSW indexes on target tables.
- Drift detection comparing current indexes against desired configuration.
- Concurrent index builds with unique temporary names.
- Ownership tracking via index definition comments.
- Advisory lock serialization across multiple manager instances.
- Verification of index health (valid, ready, live) before retirement.
- Protection against dropping unowned indexes by default.
- Configurable HNSW parameters (m, ef_construction).
- Support for vector and halfvec types with cosine, l2, and ip metrics.
- Configurable build timeouts and grace periods.
- One-shot and continuous reconciliation modes.
- Privacy-safe JSON reports with redacted connection strings and 0600 file permissions.
- Strict YAML configuration validation with helpful error messages.
- Docker Compose demo with PostgreSQL and pgvector.
- Synthetic dataset with 10,000 documents at multiple filter selectivity levels.
- Focused tests for configuration, catalog inspection, drift detection, advisory locks, and reporting.
- Go 1.21-compatible module graph and Docker build.
- Apache-2.0 license.
