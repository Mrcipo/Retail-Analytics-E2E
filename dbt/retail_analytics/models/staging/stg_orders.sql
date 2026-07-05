with

source as (
    select * from {{ source('raw', 'tiendanube_orders') }}
),

renamed as (
    select
        cast(order_id    as bigint)                      as order_id,
        cast(store_id    as integer)                     as store_id,
        cast(customer_id as bigint)                      as customer_id,
        trim(customer_name)                              as customer_name,
        lower(trim(customer_email))                      as customer_email,
        cast(created_at  as timestamp)                   as created_at,
        cast(total       as decimal(12, 2))              as total,
        cast(_received_at as timestamp)                  as _received_at
    from source
)

select * from renamed
