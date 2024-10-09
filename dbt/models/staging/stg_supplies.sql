with renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,
        id as supply_id,
        sku as product_id,
        name as supply_name,
        (cost / 100.0) as supply_cost,
        perishable as is_perishable_supply
    from {{ source('raw', 'raw_supplies') }}
)
select * from renamed
