
-- Cohort Retention Analysis
WITH user_first_month AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(timestamp)) AS cohort_month
    FROM fact_events
    GROUP BY user_id
),
user_activity AS (
    SELECT 
        f.user_id,
        u.cohort_month,
        DATE_TRUNC('month', f.timestamp) AS activity_month,
        EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', f.timestamp), u.cohort_month)) AS months_since_first
    FROM fact_events f
    JOIN user_first_month u ON f.user_id = u.user_id
)
SELECT 
    cohort_month,
    months_since_first,
    COUNT(DISTINCT user_id) AS retained_users
FROM user_activity
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;
