with source as (

    select * from {{ source('zendesk', 'ticket_metric') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        ticket_id,

        -- time metrics (in minutes)
        reply_time_in_minutes__calendar              as first_reply_time_calendar_minutes,
        reply_time_in_minutes__business              as first_reply_time_business_minutes,
        full_resolution_time_in_minutes__calendar    as full_resolution_time_calendar_minutes,
        full_resolution_time_in_minutes__business    as full_resolution_time_business_minutes,
        agent_wait_time_in_minutes__calendar         as agent_wait_time_calendar_minutes,
        agent_wait_time_in_minutes__business         as agent_wait_time_business_minutes,
        requester_wait_time_in_minutes__calendar     as requester_wait_time_calendar_minutes,
        requester_wait_time_in_minutes__business     as requester_wait_time_business_minutes,
        on_hold_time_in_minutes__calendar            as on_hold_time_calendar_minutes,
        on_hold_time_in_minutes__business            as on_hold_time_business_minutes,

        -- counts
        reopens,
        replies,
        assignee_stations                            as assignee_station_count,
        group_stations                               as group_station_count,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,
        updated_at::timestamp_ntz                    as updated_at,
        assigned_at::timestamp_ntz                   as first_assigned_at,
        initially_assigned_at::timestamp_ntz         as initially_assigned_at,
        solved_at::timestamp_ntz                     as solved_at,
        latest_comment_added_at::timestamp_ntz       as latest_comment_added_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
