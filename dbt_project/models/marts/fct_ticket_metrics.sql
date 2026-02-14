{{
    config(
        materialized='table',
        unique_key='ticket_id'
    )
}}

/*
    Fact table for ticket-level operational metrics and SLA compliance.
    Grain: one row per ticket.
*/

with enriched as (

    select * from {{ ref('int_zendesk__ticket_enriched') }}

),

sla as (

    select * from {{ ref('int_zendesk__sla_compliance') }}

),

final as (

    select
        e.ticket_id,
        e.ticket_priority,
        e.ticket_status,
        e.ticket_type,
        e.channel,
        e.is_resolved,
        e.assignee_id,
        e.group_id,
        e.organization_id,
        e.brand_id,

        -- timing metrics
        e.first_reply_time_calendar_minutes,
        e.first_reply_time_business_minutes,
        e.full_resolution_time_calendar_minutes,
        e.full_resolution_time_business_minutes,
        e.requester_wait_time_calendar_minutes,
        e.requester_wait_time_business_minutes,
        e.agent_wait_time_calendar_minutes,
        e.on_hold_time_calendar_minutes,
        e.hours_open,

        -- volume metrics
        e.reopens,
        e.replies,
        e.assignee_station_count,
        e.group_station_count,

        -- SLA
        s.first_reply_target_minutes,
        s.first_reply_sla_met,
        s.resolution_target_minutes,
        s.resolution_sla_met,

        -- satisfaction
        e.satisfaction_score,

        -- timestamps
        e.ticket_created_at,
        e.ticket_updated_at,
        e.solved_at

    from enriched e
    left join sla s on e.ticket_id = s.ticket_id

)

select * from final
