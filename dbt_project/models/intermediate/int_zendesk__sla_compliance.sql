{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Evaluates each ticket against SLA thresholds defined via the seed file
    (priority-based targets). Produces per-ticket SLA compliance flags
    consumed by mart models.
*/

with enriched_tickets as (

    select * from {{ ref('int_zendesk__ticket_enriched') }}

),

sla_targets as (

    select * from {{ ref('zendesk_ticket_priority_weights') }}

),

sla_evaluated as (

    select
        t.ticket_id,
        t.ticket_priority,
        t.ticket_status,
        t.is_resolved,

        -- first reply SLA
        t.first_reply_time_business_minutes,
        s.first_reply_target_minutes,
        case
            when t.first_reply_time_business_minutes is null then null
            when t.first_reply_time_business_minutes <= s.first_reply_target_minutes then true
            else false
        end as first_reply_sla_met,

        -- resolution SLA
        t.full_resolution_time_business_minutes,
        s.resolution_target_minutes,
        case
            when not t.is_resolved then null
            when t.full_resolution_time_business_minutes is null then null
            when t.full_resolution_time_business_minutes <= s.resolution_target_minutes then true
            else false
        end as resolution_sla_met,

        t.ticket_created_at

    from enriched_tickets t
    left join sla_targets s
        on lower(coalesce(t.ticket_priority, 'normal')) = lower(s.priority)

)

select * from sla_evaluated
