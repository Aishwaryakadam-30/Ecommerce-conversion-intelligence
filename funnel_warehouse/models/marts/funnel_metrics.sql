-- Marts model: aggregate funnel metrics per user
WITH user_events AS (
    SELECT * FROM {{ ref('stg_events') }}
),
user_funnel AS (
    SELECT
        shopper_id,
        MAX(CASE WHEN action_taken = 'page_view' THEN 1 ELSE 0 END) AS has_viewed_page,
        MAX(CASE WHEN action_taken = 'product_view' THEN 1 ELSE 0 END) AS has_viewed_product,
        MAX(CASE WHEN action_taken = 'add_to_cart' THEN 1 ELSE 0 END) AS has_added_to_cart,
        MAX(CASE WHEN action_taken = 'purchase' THEN 1 ELSE 0 END) AS has_purchased,
        SUM(CASE WHEN action_taken = 'purchase' THEN purchase_value ELSE 0 END) AS total_spent,
        MIN(event_time) AS first_seen_time,
        MAX(event_time) AS last_seen_time
    FROM user_events
    GROUP BY shopper_id
)
SELECT
    shopper_id,
    has_viewed_page,
    has_viewed_product,
    has_added_to_cart,
    has_purchased,
    total_spent,
    first_seen_time,
    last_seen_time,
    CASE 
        WHEN has_purchased = 1 THEN 'converter'
        WHEN has_added_to_cart = 1 THEN 'cart_abandoner'
        WHEN has_viewed_product = 1 THEN 'browser'
        ELSE 'visitor'
    END AS shopper_segment
FROM user_funnel
