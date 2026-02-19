with source as (

    select * from {{ source('zendesk', 'ticket') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        id                                          as ticket_id,
        organization_id,
        requester_id,
        submitter_id,
        assignee_id,
        group_id,
        brand_id,

        -- ticket attributes
        subject,
        description,
        type                                        as ticket_type,
        status                                      as ticket_status,
        priority                                    as ticket_priority,
        via__channel                                 as channel,
        is_public,
        recipient,

        -- satisfaction
        satisfaction_rating__score                   as satisfaction_score,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,
        updated_at::timestamp_ntz                    as updated_at,
        due_at::timestamp_ntz                        as due_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
