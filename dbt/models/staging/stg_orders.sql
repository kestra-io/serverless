with renamed as (
    select
        id as order_id,
        store_id as location_id,
        customer as customer_id,
        (order_total / 100.0) as order_total,
        (tax_paid / 100.0) as tax_paid,
        ---------- timestamps
        ordered_at
    from {{ source('raw', 'raw_orders') }}
)
select * from renamed
