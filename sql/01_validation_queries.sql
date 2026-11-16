-- Reconciliation across medallion layers
SELECT 'bronze' AS layer, COUNT(*) AS row_count FROM enterprise_dev.lakehouse.bronze_orders
UNION ALL SELECT 'silver', COUNT(*) FROM enterprise_dev.lakehouse.silver_orders
UNION ALL SELECT 'quarantine', COUNT(*) FROM enterprise_dev.lakehouse.quarantine_orders;

-- Duplicate business keys should return zero rows after Silver deduplication
SELECT order_id, COUNT(*) AS cnt
FROM enterprise_dev.lakehouse.silver_orders
GROUP BY order_id HAVING COUNT(*) > 1;

-- Gold metrics
SELECT * FROM enterprise_dev.lakehouse.gold_daily_sales ORDER BY order_date DESC;
SELECT * FROM enterprise_dev.lakehouse.gold_data_quality_metrics;
