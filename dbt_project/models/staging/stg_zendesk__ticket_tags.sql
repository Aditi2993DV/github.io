with source as (

    select * from {{ source('zendesk', 'ticket_tag') }}

),

renamed as (

    select
        ticket_id,
        tag                                         as tag_name,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
