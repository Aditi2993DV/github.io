with source as (

    select * from {{ source('zendesk', 'brand') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        id                                          as brand_id,

        -- brand attributes
        name                                        as brand_name,
        subdomain,
        url                                         as brand_url,
        active                                      as is_active,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,
        updated_at::timestamp_ntz                    as updated_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
