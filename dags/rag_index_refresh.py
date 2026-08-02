"""Nightly refresh of the internal-docs RAG index.

Chunks the HR/policy PDF corpus, generates embeddings, and reloads the
BigQuery vector table used by the internal support agent.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

PROJECT_ID = "gcai-may-26"
DATASET = "rag"
CORPUS_BUCKET = "gcai-may-26-internal-docs"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def chunk_corpus(**context):
    from rag.chunking import chunk_corpus_from_gcs

    n = chunk_corpus_from_gcs(CORPUS_BUCKET, prefix="policies/")
    context["ti"].xcom_push(key="chunk_count", value=n)


def embed_chunks(**context):
    from rag.embedding_bq import embed_and_load

    count = context["ti"].xcom_pull(task_ids="chunk_corpus", key="chunk_count")
    embed_and_load(project_id=PROJECT_ID, dataset=DATASET, expected_chunks=count)


with DAG(
    dag_id="rag_index_refresh",
    description="Rebuild the internal-docs embedding index in BigQuery.",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["rag", "embeddings", "vector-search"],
) as dag:

    chunk = PythonOperator(
        task_id="chunk_corpus",
        python_callable=chunk_corpus,
    )

    embed = PythonOperator(
        task_id="embed_chunks",
        python_callable=embed_chunks,
        execution_timeout=timedelta(minutes=20),
    )

    rebuild_index = BigQueryInsertJobOperator(
        task_id="rebuild_vector_index",
        configuration={
            "query": {
                "query": "{% include 'sql/04_vector_index.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    chunk >> embed >> rebuild_index
