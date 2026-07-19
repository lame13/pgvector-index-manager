# pgvector-index-manager

[![CI](https://github.com/lame13/pgvector-index-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/lame13/pgvector-index-manager/actions/workflows/ci.yml)

Safe reconciler for population-specific pgvector HNSW indexes.

pgvector-index-manager inspects, plans, and safely reconciles pgvector HNSW indexes for population-specific workloads. One configuration owns one named index family and may define a partial-index population with safe equality/`IN` filters. Separate configurations can manage other populations on the same table without retiring each other's indexes.

## Quick Start (Docker Compose)

```bash
cd docker
docker compose up --build --abort-on-container-exit --exit-code-from status
```

This starts PostgreSQL 16 with pgvector, seeds 10,000 synthetic documents with 128-dimensional vectors, runs the status command, and stops the services when the command exits. The demo database is ephemeral.

To run the full workflow (status → apply → status):

```bash
cd docker
docker compose up --build postgres
docker compose run --rm status
docker compose run --rm apply
docker compose run --rm status
docker compose down
```

To select another supported PostgreSQL major or avoid a local port conflict:

```bash
POSTGRES_MAJOR=17 POSTGRES_PORT=55432 docker compose up --build --abort-on-container-exit --exit-code-from status
```

## Installation

### Binary

Download a static binary from the releases page, or build from source:

```bash
go build -o pgvector-index-manager .
```

### Go install

```bash
go install github.com/lame13/pgvector-index-manager@latest
```

### Docker

```bash
docker build -f docker/Dockerfile -t pgvector-index-manager .
docker run --rm pgvector-index-manager version
```

## Usage

```bash
# Inspect current indexes
pgvector-index-manager status -c config.yaml

# Show what changes would be made
pgvector-index-manager plan -c config.yaml

# Execute reconciliation (one-shot)
pgvector-index-manager apply -c config.yaml

# Execute reconciliation (dry-run)
pgvector-index-manager apply -c config.yaml --dry-run

# Print version
pgvector-index-manager version
```

### Commands

| Command   | Description                                    |
|-----------|------------------------------------------------|
| `status`  | Inspect current pgvector HNSW indexes          |
| `plan`    | Show what index changes would be made           |
| `apply`   | Execute index reconciliation                    |
| `version` | Print the version                               |

### Exit Codes

| Code | Meaning                              |
|------|--------------------------------------|
| 0    | Success or no changes needed         |
| 1    | Runtime error                        |

## Configuration

See [`testdata/sample-config.yaml`](testdata/sample-config.yaml) for a fully documented example.

### Key Sections

**connection** — PostgreSQL DSN plus bounded statement and lock timeouts. Catalog inspection also uses the statement timeout as a client-side deadline.

**table** — Schema, table name, and source vector/halfvec column.

**index** — HNSW index settings:
- `name` — Index name
- `type` — `vector` or `halfvec`
- `metric` — `cosine`, `l2`, or `ip`
- `dimensions` — Required vector identity (1–2,000 for `vector`, 1–4,000 for `halfvec`)
- `m` — HNSW m parameter (1-100, default: 16)
- `ef_construction` — HNSW ef_construction parameter (1-1000, default: 64)

**population** — Optional column/value filters used to build a partial HNSW index. Columns are quoted as identifiers and values as PostgreSQL literals; arbitrary SQL is not accepted. An empty filter list indexes the whole table.

**reconcile** — Reconciliation settings:
- `ownership_tag` — Names the manager instance. Structured comments also record the managed index family, complete spec hash and verified catalog-structure fingerprint.
- `drop_unowned` — Allows replacement of a conflicting same-name unowned index (default: false). It never sweeps unrelated indexes; after an authorized swap, a structured retirement marker lets a later run finish cleanup safely.
- `build_timeout` — Maximum time for a single index build.
- `grace_period` — Time to wait after publishing a replacement before retiring the old index. The absolute UTC deadline is persisted on the retiring index, so restarts honor the remaining period.
- `continuous` — Enable continuous reconciliation mode.
- `interval` — Polling interval in continuous mode.

**report** — Output directory, target label, and privacy controls.

## How It Works

1. **Inspect** — Query PostgreSQL catalog for all HNSW indexes on the target table and preflight the desired schema/name across PostgreSQL's relation namespace.
2. **Detect drift** — Compare health, ownership, vector type/dimensions, population, opclass, `m`, and `ef_construction` against a stable spec hash.
3. **Plan** — Determine what changes are needed.
4. **Acquire lock** — Use advisory locks to serialize operations across instances.
5. **Build** — Create replacement index concurrently with a unique temporary name.
6. **Verify** — Confirm the replacement is valid, ready, live, and structurally correct: one HNSW key, no included columns, the configured vector source/cast, stored type/dimensions, opclass, parameters, population columns, and predicate presence.
7. **Tag** — Record structured ownership, managed-family, spec-hash, and an attested fingerprint of the complete catalog expression/predicate via `COMMENT ON INDEX`.
8. **Publish** — Atomically move the previous desired-name index aside and rename the verified replacement.
9. **Verify again** — Confirm the published desired-name index still matches the complete spec.
10. **Retire** — Persist a UTC grace deadline, wait only for the remaining period (including after restart), then drop only superseded indexes from the same managed family with `DROP INDEX CONCURRENTLY`.
11. **Report** — Write a privacy-safe JSON report.

### Safety Features

- **Ownership tracking** — Only indexes with matching structured ownership and managed-family metadata are retired automatically; another managed family is never treated as merely unowned.
- **Advisory locks** — A dedicated PostgreSQL session holds the table-scoped lock for the complete mutation sequence.
- **Concurrent builds** — Build replacement indexes without blocking reads.
- **Structural verification** — Confirm health and catalog structure, then bind the exact expression and predicate fingerprint to the ownership marker so copied or stale claims cannot hide replacement drift.
- **Configurable timeouts** — Bound index build duration.
- **Durable grace period** — Optional persisted delay between publication and retirement that survives process restarts.
- **Unowned protection** — Refuse to replace a conflicting same-name unowned index by default, and never sweep unrelated indexes.
- **Namespace preflight** — Refuse before building when the desired schema/name belongs to another PostgreSQL relation.

### Drift Detection

The tool detects drift when:
- No HNSW indexes exist on the table
- The desired index is missing
- The desired index is unhealthy (invalid, not ready, or not live)
- The operator class doesn't match (e.g., `vector_cosine_ops` vs `vector_l2_ops`)
- HNSW parameters don't match (m, ef_construction)
- Vector dimensions, population filters, or ownership/spec metadata don't match
- The indexed vector expression, stored vector type, key layout, referenced columns, or attested predicate fingerprint changes
- Another relation occupies the desired schema/name
- A stale replacement from the same managed family is awaiting retirement

## Privacy-Safe Defaults

Reports are safe to commit or share by default:

- Connection strings are **redacted** by default.
- Table names, index names, ownership tags, detailed actions, and database errors are **omitted or generalized** unless `report.include_target_details` is enabled.
- Report files are written with **0600** permissions.

## Operational Safety

- `status` and `plan` can use read-only catalog credentials. `apply` requires privileges to create, comment, rename, and drop indexes on the target table/schema.
- Set `connection.statement_timeout` and `connection.lock_timeout` for your environment.
- Set `reconcile.build_timeout` independently for long concurrent builds; normal catalog/swap/retirement operations use the connection timeouts.
- Consider `reconcile.grace_period` for production environments.
- Always review `plan` before setting `drop_unowned: true`; this is the explicit authorization to replace a conflicting same-name unowned index.
- `drop_unowned` never authorizes replacement of an index carrying valid metadata for another ownership tag or managed family.
- The first `0.1.1` apply rebuilds indexes tagged by `0.1.0` once because the older metadata does not attest the catalog structure.

## Continuous Mode

Enable continuous reconciliation to monitor and repair indexes automatically:

```yaml
reconcile:
  continuous: true
  interval: 5m
```

When enabled, `apply` runs in a loop at the configured interval, checking for drift and applying changes as needed. This is useful for:
- Automatic index repair after failures
- Ensuring indexes stay in sync with configuration
- Monitoring index health in long-running deployments

## Synthetic Dataset

The Docker Compose demo seeds a `documents` table with:

| Rows | Filter                  | Filter selectivity |
|------|-------------------------|--------------------|
| 10,000 | `status = active`    | 100%               |
| 1,000  | `tier = standard`    | 10%                |
| 100    | `tier = premium`     | 1%                 |
| 10     | `tier = enterprise`  | 0.1%               |

All rows have `status = 'active'` for 100% selectivity filtering.

## Requirements

- PostgreSQL 16, 17, or 18 with pgvector 0.8+ installed
- Go 1.25.12+ (for building from source)

## License

Apache-2.0. See [LICENSE](LICENSE).

## Disclaimer

This is an **unofficial** tool for managing pgvector indexes. It is not affiliated with or endorsed by the pgvector project. pgvector is developed by Andrew Kane and contributors.
