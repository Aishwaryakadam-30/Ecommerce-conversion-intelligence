
SELECT 
    product_id,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END)::numeric, 2) AS revenue
FROM fact_events
WHERE product_id IS NOT NULL
GROUP BY product_id
ORDER BY revenue DESC NULLS LAST
LIMIT 10;
