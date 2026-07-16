-- pgvector-index-manager: synthetic data seed
-- Generates 10,000 documents with deterministic pseudo-random 128-dimensional
-- vectors. Both the row ID and dimension participate in the hash so PostgreSQL
-- cannot evaluate one uncorrelated random vector and reuse it for every row.

INSERT INTO documents (embedding, status, tier)
SELECT
    array_agg(
        (
            (hashtextextended(i::text || ':' || dimension::text, 42) % 1000000)::double precision
            / 1000000.0
        )::real
        ORDER BY dimension
    )::vector(128),
    'active',
    CASE
        WHEN i <= 10   THEN 'enterprise'
        WHEN i <= 110  THEN 'premium'
        WHEN i <= 1110 THEN 'standard'
        ELSE 'none'
    END
FROM generate_series(1, 10000) AS rows(i)
CROSS JOIN generate_series(1, 128) AS dimensions(dimension)
GROUP BY i
ORDER BY i;

ANALYZE documents;
