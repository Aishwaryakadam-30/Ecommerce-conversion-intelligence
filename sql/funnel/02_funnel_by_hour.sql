
SELECT 
    EXTRACT(HOUR FROM timestamp) AS hour,
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS buyers,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) 
          / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END), 0), 2) AS conversion_rate
FROM fact_events
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hour;
