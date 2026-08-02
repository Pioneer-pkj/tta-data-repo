"""Retrieve the nearest document chunks for a natural-language query."""

import os

from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "gcai-may-26")
DATASET = os.getenv("RAG_DATASET", "rag")
EMBEDDING_MODEL = "text-embedding-004"
TOP_K = 5

VECTOR_SEARCH_SQL = """
SELECT
  base.chunk_id,
  base.source,
  base.content,
  distance
FROM VECTOR_SEARCH(
  TABLE `{project}.{dataset}.doc_chunks`, 'embedding',
  (SELECT @query_embedding AS embedding),
  top_k => @top_k,
  distance_type => 'COSINE'
)
ORDER BY distance
"""


def embed_query(query: str) -> list[float]:
    model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
    return model.get_embeddings([query])[0].values


def retrieve(query: str, top_k: int = TOP_K) -> list[dict]:
    """Return the top_k most similar chunks to `query`."""
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("query_embedding", "FLOAT64", embed_query(query)),
            bigquery.ScalarQueryParameter("top_k", "INT64", top_k),
        ]
    )
    sql = VECTOR_SEARCH_SQL.format(project=PROJECT_ID, dataset=DATASET)
    rows = client.query(sql, job_config=job_config).result()

    return [
        {
            "chunk_id": r["chunk_id"],
            "source": r["source"],
            "content": r["content"],
            "distance": r["distance"],
        }
        for r in rows
    ]


if __name__ == "__main__":
    import sys

    question = sys.argv[1] if len(sys.argv) > 1 else "How much parental leave do I get?"
    for i, hit in enumerate(retrieve(question), 1):
        print(f"\n[{i}] {hit['source']} (distance={hit['distance']:.4f})")
        print(hit["content"][:300])
