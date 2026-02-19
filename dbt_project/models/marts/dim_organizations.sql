{{
    config(
        materialized='table',
        unique_key='organization_id'
    )
}}

/*
    Dimension table for Zendesk organizations with ticket volume summary.
    Grain: one row per organization.
*/

with organizations as (

    select * from {{ ref('stg_zendesk__organizations') }}

),

ticket_counts as (

    select
        organization_id,
        count(*)                                                    as total_tickets,
        count(case when is_resolved then 1 end)                     as resolved_tickets,
        count(case when not is_resolved then 1 end)                 as open_tickets,
        min(ticket_created_at)                                      as first_ticket_at,
        max(ticket_created_at)                                      as latest_ticket_at
    from {{ ref('int_zendesk__ticket_enriched') }}
    where organization_id is not null
    group by 1

),

final as (

    select
        o.organization_id,
        o.organization_name,
        o.organization_details,
        o.organization_notes,
        o.has_shared_tickets,
        o.has_shared_comments,
        o.created_at,
        o.updated_at,

        coalesce(tc.total_tickets, 0)               as total_tickets,
        coalesce(tc.resolved_tickets, 0)             as resolved_tickets,
        coalesce(tc.open_tickets, 0)                 as open_tickets,
        tc.first_ticket_at,
        tc.latest_ticket_at

    from organizations o
    left join ticket_counts tc on o.organization_id = tc.organization_id

)

select * from final
