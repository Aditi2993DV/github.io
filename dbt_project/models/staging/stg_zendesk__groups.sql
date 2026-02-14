with source as (

    select * from {{ source('zendesk', 'group') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        id                                          as group_id,

        -- group attributes
        name                                        as group_name,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,
        updated_at::timestamp_ntz                    as updated_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
