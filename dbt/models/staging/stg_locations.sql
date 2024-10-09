with source as (
    select * from {{ source('raw', 'raw_stores') }}
    -- where opened_at <= {{ var('truncate_timespan_to') }}
),
renamed as (
    select
        id as location_id,
        name as location_name,
        tax_rate,
        opened_at
    from source
)
select * from renamed
