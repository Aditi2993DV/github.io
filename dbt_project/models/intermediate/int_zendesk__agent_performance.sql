{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Aggregates ticket-level data per assignee (agent) to produce
    agent performance indicators used by the fct_agent_performance mart.
*/

with enriched_tickets as (

    select * from {{ ref('int_zendesk__ticket_enriched') }}
    where assignee_id is not null

),

agent_stats as (

    select
        assignee_id                                                     as agent_id,
        assignee_name                                                   as agent_name,
        assignee_email                                                  as agent_email,
        group_name,

        count(*)                                                        as total_tickets_assigned,
        count(case when is_resolved then 1 end)                         as tickets_resolved,
        count(case when not is_resolved then 1 end)                     as tickets_open,

        -- response time
        avg(first_reply_time_business_minutes)                          as avg_first_reply_time_minutes,
        median(first_reply_time_business_minutes)                       as median_first_reply_time_minutes,

        -- resolution time
        avg(case when is_resolved
            then full_resolution_time_business_minutes end)             as avg_resolution_time_minutes,
        median(case when is_resolved
            then full_resolution_time_business_minutes end)             as median_resolution_time_minutes,

        -- quality signals
        avg(reopens)                                                    as avg_reopens_per_ticket,
        avg(replies)                                                    as avg_replies_per_ticket,

        -- satisfaction
        count(case when satisfaction_score = 'good' then 1 end)         as good_satisfaction_count,
        count(case when satisfaction_score = 'bad' then 1 end)          as bad_satisfaction_count,
        count(case when satisfaction_score is not null then 1 end)       as rated_ticket_count

    from enriched_tickets
    group by 1, 2, 3, 4

)

select
    *,
    case
        when rated_ticket_count > 0
        then round(good_satisfaction_count * 100.0 / rated_ticket_count, 2)
        else null
    end as satisfaction_pct

from agent_stats
