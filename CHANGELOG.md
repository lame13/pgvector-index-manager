# Changelog

All notable changes to this project will be documented in this file.

The project uses semantic versioning.

## [0.2.0] - 2026-09-28

### Added

- Multi-target configuration: one file may declare a `targets` list, and `status`, `plan`, and `apply` reconcile every target in a single pass. Top-level sections act as shared defaults and each target overrides only the fields it sets, so a DSN, table, or reconcile policy can be shared while every managed index family keeps its own index name, population, and ownership tag.
- Per-target JSON reports. A named target writes `report-<name>.json` instead of `report.json`, so one pass over several targets no longer overwrites earlier reports. Report permissions and privacy defaults are unchanged.
- `config.LoadTargets` returns one resolved and validated `Config` per target. Legacy single-target files load through the same path.
- `reconciler.ApplyAll` and `reconciler.ApplyAllContinuous` reconcile several targets in order and isolate failures, so a blocked or failing target does not stop the others.
- `testdata/sample-config-multi.yaml`, a documented multi-target example.

### Changed

- `apply`, `plan`, and `status` accept multi-target files and label each block of output with its target name. `apply` exits non-zero when any target fails and reports "nothing to apply" only when no target had work.
- Targets that share a DSN share a single connection pool. Connections open on first use, so an unavailable database does not stop healthy targets or prevent continuous mode from retrying.
- Continuous mode is a run-level setting: `reconcile.continuous` and `reconcile.interval` belong in the shared section, and declaring them inside a target is rejected with an explicit error.
- Configuration files without a `targets` list keep their previous behavior, including the `report.json` filename and the single-target exit codes.

### Tests

- Cover shared-default inheritance, per-target overrides including explicit `false` and an explicit empty filter list, and rejection of missing, unsafe, or duplicate target names, duplicate managed index names, and run-level settings declared per target.
- Add PostgreSQL/pgvector integration coverage for reconciling and repairing independent populations sharing an ownership tag, converging idempotently without retiring a peer family, and isolating a blocked target from a healthy one.
- Verify per-target report naming and that a multi-target run never overwrites `report.json`.
- Exercise CLI failure isolation for unavailable databases, missing tables, and unwritable report directories; include these tests in the PostgreSQL CI matrix.

### Fixed

- Continue `status` and `plan` after a target fails, and report every target's error. Continue writing later targets' reports when an earlier report fails.
- Reject an explicit empty target list instead of accidentally applying the shared defaults as a single target.
- Reject target names that differ only by case, preventing report overwrites on case-insensitive filesystems.
- Compare schema and index names separately when detecting duplicates, allowing quoted identifiers containing dots and identical index names on separate DSNs.
- Return an error for nonpositive continuous polling intervals instead of panicking.
- Correct examples that implied separate ownership tags were required for independent managed index families.
- Run integration test packages sequentially against their shared CI database to prevent concurrent pgvector extension creation.
- Require Go 1.26.8 consistently for builds, Docker, and CI, and update `golang.org/x/text` to 0.39.0 to address the vulnerabilities reported by Govulncheck.

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
