select
    customer_name,
    count_lifetime_orders as lifetime_orders,
    lifetime_spend as lifetime_spend_usd,
    lifetime_spend / count_lifetime_orders as avg_order_value_usd
from {{ ref('customers') }}
order by lifetime_spend_usd desc