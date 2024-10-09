with renamed as (
    select
        id as customer_id,
        name as customer_name
    from {{ source('raw', 'raw_customers') }}
)
select * from renamed
