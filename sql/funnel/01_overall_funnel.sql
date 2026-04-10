
WITH user_journey AS (
    SELECT 
        user_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_viewed,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM fact_events
    GROUP BY user_id
)
SELECT 
    SUM(viewed) AS page_views,
    SUM(product_viewed) AS product_views,
    SUM(carted) AS add_to_carts,
    SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(product_viewed) / NULLIF(SUM(viewed), 0), 2) AS view_to_product_pct,
    ROUND(100.0 * SUM(carted) / NULLIF(SUM(product_viewed), 0), 2) AS product_to_cart_pct,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(carted), 0), 2) AS cart_to_purchase_pct,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(viewed), 0), 2) AS overall_conversion
FROM user_journey;
