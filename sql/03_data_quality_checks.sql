-- Data quality gate for the daily sales mart.
-- Writes one row per check into sales.dq_results.

INSERT INTO `gcai-may-26.sales.dq_results` (run_ts, check_name, status, detail)

-- 1. Freshness: yesterday must be present.
SELECT
  CURRENT_TIMESTAMP(),
  'freshness_daily_sales_agg',
  IF(COUNT(*) > 0, 'PASS', 'FAIL'),
  FORMAT('rows for %t: %d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), COUNT(*))
FROM `gcai-may-26.sales.daily_sales_agg`
WHERE sales_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

UNION ALL

-- 2. No negative revenue.
SELECT
  CURRENT_TIMESTAMP(),
  'non_negative_revenue',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  FORMAT('%d rows with negative revenue', COUNT(*))
FROM `gcai-may-26.sales.daily_sales_agg`
WHERE gross_revenue < 0

UNION ALL

-- 3. Order ids must be unique in the raw landing table.
SELECT
  CURRENT_TIMESTAMP(),
  'unique_order_ids',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  FORMAT('%d duplicated order_id values', COUNT(*))
FROM (
  SELECT order_id
  FROM `gcai-may-26.sales.raw_orders`
  GROUP BY order_id
  HAVING COUNT(*) > 1
)

UNION ALL

-- 4. Referential integrity against the customer lookup.
SELECT
  CURRENT_TIMESTAMP(),
  'orphan_customers',
  IF(COUNT(*) = 0, 'PASS', 'WARN'),
  FORMAT('%d orders with unknown customer_id', COUNT(*))
FROM `gcai-may-26.sales.raw_orders` o
LEFT JOIN `gcai-may-26.sales.customer_lookup` c USING (customer_id)
WHERE c.customer_id IS NULL;
