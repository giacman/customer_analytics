WITH customer_orders_log AS (
  SELECT
    customer_account_id,
    DATE_TRUNC('month', order_date) AS order_month
  FROM
    "prod-dwh".business_discovery.bd_internet_orders
  GROUP BY customer_account_id,order_month
), time_lapse AS (
    SELECT customer_account_id,
           order_month,
           --the proportion of customers who return after x lag of time
           lead(order_month, 1) over (partition BY customer_account_id ORDER BY customer_account_id, order_month) as lead
    FROM customer_orders_log
), time_diff_calculated AS (
    SELECT customer_account_id,
           TO_CHAR(order_month, 'YYYY-mm') AS order_month,
           TO_CHAR(lead, 'YYYY-mm') AS lead,
           MONTHS_BETWEEN (lead, order_month) AS time_diff
    FROM time_lapse
), customer_categorized AS (
    SELECT customer_account_id,
       order_month,
       CASE
             WHEN time_diff <= 4  THEN 'retained' --adjust the value to change the "retention window"
             WHEN time_diff > 4 THEN 'lagger'
             WHEN time_diff IS NULL THEN 'lost'
       END AS cust_type
FROM time_diff_calculated
) SELECT
      order_month,
    CAST(COUNT(CASE WHEN cust_type='retained' THEN customer_account_id END) AS NUMERIC)/count(customer_account_id) AS retention
  FROM customer_categorized
GROUP BY 1
ORDER BY 1
