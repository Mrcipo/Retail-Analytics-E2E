with

source as (
    select * from {{ source('raw', 'off_products') }}
),

renamed as (
    select
        code,
        trim(product_name)                               as product_name,
        trim(categories)                                 as categories,
        trim(brands)                                     as brands,
        trim(countries)                                  as countries,
        to_timestamp(last_modified_t)                    as last_modified_at,
        _ingested_at
    from source
)

select * from renamed
