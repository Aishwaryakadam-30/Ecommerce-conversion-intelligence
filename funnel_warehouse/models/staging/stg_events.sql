-- Staging model: clean raw events from fact_events
WITH raw_events AS (
    SELECT * FROM {{ source('warehouse', 'fact_events') }}
)
SELECT
    event_id,
    user_id AS shopper_id,
    session_id,
    timestamp AS event_time,
    event_type AS action_taken,
    product_id,
    amount AS purchase_value,
    EXTRACT(YEAR FROM timestamp) AS event_year,
    EXTRACT(MONTH FROM timestamp) AS event_month,
    EXTRACT(HOUR FROM timestamp) AS event_hour
FROM raw_events
WHERE user_id IS NOT NULL
  AND event_type IS NOT NULL
