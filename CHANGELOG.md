# Changelog

All notable changes to this project will be documented in this file.

The project uses semantic versioning.

## [0.1.1] - 2026-07-19

### Fixed

- Persist retirement grace deadlines in structured index metadata so a restart cannot bypass the remaining grace period.
- Refuse to replace a same-name index owned by another managed family or ownership tag, even when `drop_unowned` is enabled.
- Verify the catalog structure of managed indexes, including the key layout, vector source/cast, stored type and dimensions, operator class, HNSW parameters, referenced columns, and an attested fingerprint of the complete expression and predicate.
- Detect schema namespace conflicts before building a replacement when the desired index name is already occupied by another index, table, view, materialized view, sequence, or foreign table.
- Apply configured statement and lock timeouts when recovering retirement state from an earlier release.

### Changed

- Ownership metadata version 2 records the verified structural fingerprint and, for retiring indexes, an absolute UTC retirement deadline.
- Indexes carrying `0.1.0` ownership metadata are rebuilt once so they acquire the stronger structural attestation.
- Raise the build requirement from Go 1.21 to patched Go 1.25.12 and update pgx to the release that fixes GO-2026-5004.

### Tests

- Add PostgreSQL/pgvector integration coverage for durable grace recovery, cross-family ownership conflicts, copied-comment structural drift, and namespace-conflict preflight.
- Add GitHub Actions checks for unit tests, the race detector, vet, builds, Staticcheck, Govulncheck, and PostgreSQL/pgvector 16, 17, and 18 integration tests.

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
