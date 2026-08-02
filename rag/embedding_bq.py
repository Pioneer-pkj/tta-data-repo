"""Embed document chunks and load them into the BigQuery vector table."""

import json
import os

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from vertexai.language_models import TextEmbeddingModel

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "gcai-may-26")
DATASET = os.getenv("RAG_DATASET", "rag")
TABLE = "doc_chunks"
EMBEDDING_MODEL = "text-embedding-004"
BATCH_SIZE = 100

SCHEMA = [
    bigquery.SchemaField("chunk_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("chunk_index", "INTEGER"),
    bigquery.SchemaField("content", "STRING"),
    bigquery.SchemaField("embedding", "FLOAT", mode="REPEATED"),
]


def create_dataset_and_table_if_not_exists(client: bigquery.Client) -> str:
    """Ensure the RAG dataset and vector table exist."""
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        print(f"Creating dataset {dataset_id}...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)

    table_id = f"{dataset_id}.{TABLE}"
    try:
        client.get_table(table_id)
    except NotFound:
        print(f"Creating table {table_id}...")
        client.create_table(bigquery.Table(table_id, schema=SCHEMA))

    return table_id


def embed_batch(model: TextEmbeddingModel, texts: list[str]) -> list[list[float]]:
    """Embed a batch of chunk texts."""
    return [e.values for e in model.get_embeddings(texts)]


def embed_and_load(
    project_id: str = PROJECT_ID,
    dataset: str = DATASET,
    chunks_path: str = "/tmp/chunks.json",
    expected_chunks: int | None = None,
) -> int:
    """Embed every chunk and replace the contents of the vector table."""
    with open(chunks_path, "r", encoding="utf-8") as fh:
        chunks = json.load(fh)

    if expected_chunks is not None and len(chunks) != expected_chunks:
        raise ValueError(
            f"Chunk count mismatch: expected {expected_chunks}, got {len(chunks)}. "
            "Upstream chunking may have failed partway."
        )

    client = bigquery.Client(project=project_id)
    table_id = create_dataset_and_table_if_not_exists(client)
    model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)

    rows = []
    for start in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[start : start + BATCH_SIZE]
        vectors = embed_batch(model, [c["content"] for c in batch])
        for chunk, vector in zip(batch, vectors):
            rows.append(
                {
                    "chunk_id": f"{chunk['source']}::{chunk['index']}",
                    "source": chunk["source"],
                    "chunk_index": chunk["index"],
                    "content": chunk["content"],
                    "embedding": vector,
                }
            )
        print(f"  embedded {min(start + BATCH_SIZE, len(chunks))}/{len(chunks)}")

    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    print(f"Loaded {len(rows)} embedded chunks into {table_id}")
    return len(rows)


if __name__ == "__main__":
    embed_and_load()
