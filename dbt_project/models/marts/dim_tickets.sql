{{
    config(
        materialized='table',
        unique_key='ticket_id'
    )
}}

/*
    Dimension table for Zendesk tickets with full context.
    Grain: one row per ticket.
*/

with enriched as (

    select * from {{ ref('int_zendesk__ticket_enriched') }}

),

ticket_tags as (

    select
        ticket_id,
        listagg(tag_name, ', ') within group (order by tag_name) as tags
    from {{ ref('stg_zendesk__ticket_tags') }}
    group by ticket_id

),

final as (

    select
        e.ticket_id,
        e.subject,
        e.ticket_type,
        e.ticket_status,
        e.ticket_priority,
        e.channel,
        e.is_public,
        e.is_resolved,
        e.satisfaction_score,

        -- requester
        e.requester_id,
        e.requester_name,
        e.requester_email,

        -- assignee
        e.assignee_id,
        e.assignee_name,

        -- org / group / brand
        e.organization_id,
        e.organization_name,
        e.group_id,
        e.group_name,
        e.brand_id,
        e.brand_name,

        -- tags
        tt.tags,

        -- timestamps
        e.ticket_created_at,
        e.ticket_updated_at,
        e.solved_at,
        e.hours_open

    from enriched e
    left join ticket_tags tt on e.ticket_id = tt.ticket_id

)

select * from final
