with

source as (
    select * from {{ source('raw', 'afip_padron') }}
),

renamed as (
    select
        trim(replace(cuit, '-', '')) as cuit,
        trim(razon_social)           as razon_social,
        upper(trim(condicion_iva))   as condicion_iva,
        trim(domicilio_fiscal)       as domicilio_fiscal,
        trim(actividad_principal)    as actividad_principal,
        try_cast(fecha_inscripcion as date) as fecha_inscripcion,
        _ingested_at
    from source
)

select * from renamed
