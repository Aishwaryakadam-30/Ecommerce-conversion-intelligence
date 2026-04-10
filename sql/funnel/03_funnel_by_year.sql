
WITH user_journey AS (
    SELECT 
        EXTRACT(YEAR FROM timestamp) AS year,
        user_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM fact_events
    GROUP BY EXTRACT(YEAR FROM timestamp), user_id
)
SELECT 
    year,
    SUM(viewed) AS viewers,
    SUM(carted) AS cart_adders,
    SUM(purchased) AS buyers,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(viewed), 0), 2) AS conversion_rate
FROM user_journey
GROUP BY year
ORDER BY year;
