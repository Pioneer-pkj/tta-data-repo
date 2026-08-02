-- Vector store for the internal-docs RAG corpus.
-- Rebuilt nightly by rag_index_refresh.rebuild_vector_index.

CREATE TABLE IF NOT EXISTS `gcai-may-26.rag.doc_chunks` (
  chunk_id    STRING NOT NULL,
  source      STRING NOT NULL,
  chunk_index INT64,
  content     STRING,
  embedding   ARRAY<FLOAT64>,
  embedded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ANN index over the embedding column. IVF with cosine distance.
CREATE VECTOR INDEX IF NOT EXISTS doc_chunks_embedding_idx
ON `gcai-may-26.rag.doc_chunks`(embedding)
OPTIONS (
  index_type = 'IVF',
  distance_type = 'COSINE',
  ivf_options = '{"num_lists": 100}'
);

-- Reference retrieval query used by the internal support agent.
-- Returns the 5 nearest chunks to a caller-supplied query embedding.
--
-- SELECT
--   base.chunk_id,
--   base.source,
--   base.content,
--   distance
-- FROM VECTOR_SEARCH(
--   TABLE `gcai-may-26.rag.doc_chunks`, 'embedding',
--   (SELECT @query_embedding AS embedding),
--   top_k => 5,
--   distance_type => 'COSINE'
-- );
