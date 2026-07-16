-- pgvector-index-manager: schema and index setup
-- Creates a demo documents table with a 128-dimensional vector column.
-- The index manager will create and manage HNSW indexes on this table.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
    id bigserial PRIMARY KEY,
    embedding vector(128) NOT NULL,
    status text NOT NULL DEFAULT 'active',
    tier text NOT NULL DEFAULT 'none'
);
