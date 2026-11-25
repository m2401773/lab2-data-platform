-- RAG: векторная БД (pgvector) + полнотекст в одной таблице -> гибридный поиск одним SQL.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA rag;

CREATE TABLE rag.documents (
    doc_id     TEXT PRIMARY KEY,          -- путь к файлу в docs-raw
    title      TEXT NOT NULL,
    doc_type   TEXT NOT NULL,             -- md / pdf / docx
    sha256     TEXT NOT NULL,             -- для инкрементальной переиндексации
    version    INT  NOT NULL DEFAULT 1,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE rag.chunks (
    chunk_id   BIGSERIAL PRIMARY KEY,
    doc_id     TEXT NOT NULL REFERENCES rag.documents ON DELETE CASCADE,
    chunk_no   INT  NOT NULL,
    section    TEXT,                      -- заголовок раздела -> цитирование в ответе
    content    TEXT NOT NULL,
    tsv        TSVECTOR GENERATED ALWAYS AS (to_tsvector('russian', coalesce(section,'') || ' ' || content)) STORED,
    embedding  VECTOR(384) NOT NULL
);

-- HNSW: приближённый kNN, быстрый поиск без обучения индекса (в отличие от IVFFlat)
CREATE INDEX chunks_hnsw ON rag.chunks USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
-- GIN по tsvector: точные термины, номера приказов, коды ошибок
CREATE INDEX chunks_fts  ON rag.chunks USING gin (tsv);

-- Serving-слой живого мониторинга (сюда пишет потоковая обработка, читает Grafana)
CREATE SCHEMA serving;
CREATE TABLE serving.live_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    event_type   TEXT        NOT NULL,
    events_cnt   INT         NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, event_type)
);
