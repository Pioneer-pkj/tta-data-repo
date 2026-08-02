-- Core sales tables.
-- Run once per environment before enabling the daily_sales_etl DAG.

CREATE SCHEMA IF NOT EXISTS `gcai-may-26.sales`
OPTIONS (location = 'US');

-- Raw landing table for order events.
CREATE TABLE IF NOT EXISTS `gcai-may-26.sales.raw_orders` (
  order_id     STRING   NOT NULL,
  store_id     STRING   NOT NULL,
  customer_id  STRING,
  order_ts     TIMESTAMP NOT NULL,
  amount       NUMERIC,
  currency     STRING,
  status       STRING,
  ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Daily aggregate mart consumed by the BI layer.
CREATE TABLE IF NOT EXISTS `gcai-may-26.sales.daily_sales_agg` (
  sales_date   DATE     NOT NULL,
  store_id     STRING   NOT NULL,
  order_count  INT64,
  gross_revenue NUMERIC,
  avg_order_value NUMERIC
)
PARTITION BY sales_date
CLUSTER BY store_id;

-- Streaming output from the Dataflow pipeline.
CREATE TABLE IF NOT EXISTS `gcai-may-26.sales.store_revenue_1m` (
  store_id     STRING,
  window_start TIMESTAMP,
  order_count  INT64,
  revenue      FLOAT64
)
PARTITION BY DATE(window_start);
