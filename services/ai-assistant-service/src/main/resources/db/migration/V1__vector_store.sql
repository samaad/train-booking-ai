-- RAG embeddings for PgVectorStore (its default table shape). mxbai-embed-large → vector(1024).
-- The extension is created by infra/postgres/init as superuser; IF NOT EXISTS returns before any
-- privilege check when it is already present, so this also runs as the vectordb role.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE vector_store (
    id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content   text,
    metadata  json,
    embedding vector(1024)
);

CREATE INDEX vector_store_embedding_idx ON vector_store
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
