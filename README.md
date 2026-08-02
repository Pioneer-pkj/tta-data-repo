# tta-data-repo

Batch and streaming data pipelines, owned by the **data engineering team**.

> ### ⚠️ Training sample — contains deliberate defects
> This repository is a fixture for the CloudOps ADK capstone. The DAGs and SQL
> intentionally include real-world reliability and cost defects so the Data
> Engineer agent has genuine findings to work. Do not copy into production.

## Layout

| Path | Contents |
| --- | --- |
| `dags/daily_sales_etl.py` | Nightly load → aggregate → data quality gate |
| `dags/rag_index_refresh.py` | Nightly rebuild of the internal-docs embedding index |
| `dataflow/streaming_orders_pipeline.py` | Pub/Sub → 1-min windows → BigQuery |
| `sql/01_create_tables.sql` | Core sales table DDL |
| `sql/02_daily_sales_agg.sql` | Daily mart rebuild |
| `sql/03_data_quality_checks.sql` | Freshness, uniqueness, referential integrity |
| `sql/04_vector_index.sql` | Vector table + IVF/COSINE index, `VECTOR_SEARCH` reference query |
| `rag/chunking.py` | PDF → overlapping character chunks |
| `rag/embedding_bq.py` | `text-embedding-004` → BigQuery vector table |
| `rag/retrieval.py` | Query embedding → `VECTOR_SEARCH` top-k |

## Pipelines

**daily_sales_etl** — 02:00 UTC. GCS → `sales.raw_orders`, rebuilds
`sales.daily_sales_agg`, then runs the DQ gate into `sales.dq_results`.

**rag_index_refresh** — 03:00 UTC. Chunks the policy corpus from GCS, embeds
with `text-embedding-004`, truncates and reloads `rag.doc_chunks`, then rebuilds
the vector index.

**streaming_orders_pipeline** — always on. Pub/Sub `orders-sub` → one-minute
tumbling windows → `sales.store_revenue_1m`.

## Local run

```bash
pip install -r requirements.txt

python rag/chunking.py                      # corpus/*.pdf -> chunks.json
python rag/embedding_bq.py                  # -> rag.doc_chunks
python rag/retrieval.py "parental leave"    # top-k vector search

python dataflow/streaming_orders_pipeline.py \
  --runner=DirectRunner --streaming
```

## How the agents use this repo

The **Data Engineer agent** reads open issues here, pulls the referenced DAG or
pipeline, checks Dataflow and Composer logs in Cloud Logging, inspects table
schema and freshness through the BigQuery MCP tools, confirms behaviour against
the official Google Cloud docs, and proposes a fix plus a backfill plan.

The agent is read-only: it never drops, writes, cancels or drains anything.
