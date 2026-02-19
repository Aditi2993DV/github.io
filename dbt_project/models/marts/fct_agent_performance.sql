{{
    config(
        materialized='table',
        unique_key='agent_id'
    )
}}

/*
    Fact table for agent-level performance metrics.
    Grain: one row per agent per group.
*/

select
    agent_id,
    agent_name,
    agent_email,
    group_name,

    total_tickets_assigned,
    tickets_resolved,
    tickets_open,

    -- response time
    round(avg_first_reply_time_minutes, 2)          as avg_first_reply_time_minutes,
    round(median_first_reply_time_minutes, 2)        as median_first_reply_time_minutes,

    -- resolution time
    round(avg_resolution_time_minutes, 2)            as avg_resolution_time_minutes,
    round(median_resolution_time_minutes, 2)         as median_resolution_time_minutes,

    -- quality
    round(avg_reopens_per_ticket, 2)                 as avg_reopens_per_ticket,
    round(avg_replies_per_ticket, 2)                 as avg_replies_per_ticket,

    -- satisfaction
    good_satisfaction_count,
    bad_satisfaction_count,
    rated_ticket_count,
    satisfaction_pct

from {{ ref('int_zendesk__agent_performance') }}
