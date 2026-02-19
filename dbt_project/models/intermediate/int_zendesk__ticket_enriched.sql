{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Enriches tickets with requester, assignee, organization, group, and brand
    details in a single wide record. Downstream mart models select from this
    to avoid repeating the same join logic.
*/

with tickets as (

    select * from {{ ref('stg_zendesk__tickets') }}

),

requesters as (

    select * from {{ ref('stg_zendesk__users') }}

),

assignees as (

    select * from {{ ref('stg_zendesk__users') }}

),

organizations as (

    select * from {{ ref('stg_zendesk__organizations') }}

),

groups as (

    select * from {{ ref('stg_zendesk__groups') }}

),

brands as (

    select * from {{ ref('stg_zendesk__brands') }}

),

metrics as (

    select * from {{ ref('stg_zendesk__ticket_metrics') }}

),

enriched as (

    select
        -- ticket core
        t.ticket_id,
        t.subject,
        t.description,
        t.ticket_type,
        t.ticket_status,
        t.ticket_priority,
        t.channel,
        t.is_public,
        t.satisfaction_score,
        t.created_at                                as ticket_created_at,
        t.updated_at                                as ticket_updated_at,

        -- requester
        t.requester_id,
        req.user_name                               as requester_name,
        req.email                                   as requester_email,

        -- assignee
        t.assignee_id,
        asg.user_name                               as assignee_name,
        asg.email                                   as assignee_email,

        -- organization
        t.organization_id,
        org.organization_name,

        -- group
        t.group_id,
        grp.group_name,

        -- brand
        t.brand_id,
        brd.brand_name,

        -- metrics
        m.first_reply_time_calendar_minutes,
        m.first_reply_time_business_minutes,
        m.full_resolution_time_calendar_minutes,
        m.full_resolution_time_business_minutes,
        m.requester_wait_time_calendar_minutes,
        m.requester_wait_time_business_minutes,
        m.agent_wait_time_calendar_minutes,
        m.on_hold_time_calendar_minutes,
        m.reopens,
        m.replies,
        m.assignee_station_count,
        m.group_station_count,
        m.solved_at,

        -- derived
        case
            when t.ticket_status in ('solved', 'closed') then true
            else false
        end                                         as is_resolved,

        datediff('hour', t.created_at, coalesce(m.solved_at, current_timestamp()))
                                                    as hours_open

    from tickets t
    left join requesters    req on t.requester_id    = req.user_id
    left join assignees     asg on t.assignee_id     = asg.user_id
    left join organizations org on t.organization_id = org.organization_id
    left join groups        grp on t.group_id        = grp.group_id
    left join brands        brd on t.brand_id        = brd.brand_id
    left join metrics       m   on t.ticket_id       = m.ticket_id

)

select * from enriched
