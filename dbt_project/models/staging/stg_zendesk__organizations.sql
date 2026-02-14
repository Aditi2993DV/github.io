with source as (

    select * from {{ source('zendesk', 'organization') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        id                                          as organization_id,

        -- organization attributes
        name                                        as organization_name,
        details                                     as organization_details,
        notes                                       as organization_notes,
        group_id                                    as default_group_id,
        shared_tickets                              as has_shared_tickets,
        shared_comments                             as has_shared_comments,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,
        updated_at::timestamp_ntz                    as updated_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
