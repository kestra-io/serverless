select
    location_name,
    count(distinct customer_id) as nr_customers,
    count(*) as nr_orders,
    sum(order_total) as revenue_usd
from {{ ref('orders') }}
group by 1