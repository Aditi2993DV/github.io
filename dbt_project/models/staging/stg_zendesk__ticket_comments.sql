with source as (

    select * from {{ source('zendesk', 'ticket_comment') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        id                                          as comment_id,
        ticket_id,
        author_id,

        -- comment attributes
        body,
        public                                      as is_public,
        via__channel                                 as channel,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
