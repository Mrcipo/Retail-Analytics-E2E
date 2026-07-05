with

source as (
    select * from {{ source('raw', 'tiendanube_order_lines') }}
),

renamed as (
    select
        cast(order_id   as bigint)          as order_id,
        product_code,
        trim(product_name)                  as product_name,
        cast(quantity   as integer)         as quantity,
        cast(unit_price as decimal(12, 2))  as unit_price,
        cast(quantity as decimal(12, 2))
            * cast(unit_price as decimal(12, 2)) as line_total
    from source
)

select * from renamed
