{{
    config(
        materialized='incremental',
        unique_key='summary_date',
        incremental_strategy='merge'
    )
}}

/*
    Daily aggregate of ticket volume, response times, and SLA compliance.
    Grain: one row per day.
    Incremental: only processes tickets created/updated since last run.
*/

with enriched as (

    select * from {{ ref('int_zendesk__ticket_enriched') }}

    {% if is_incremental() %}
        where ticket_created_at >= (select max(summary_date) from {{ this }})
    {% endif %}

),

sla as (

    select * from {{ ref('int_zendesk__sla_compliance') }}

),

daily as (

    select
        e.ticket_created_at::date                                       as summary_date,

        -- volume
        count(*)                                                        as tickets_created,
        count(case when e.is_resolved then 1 end)                       as tickets_resolved,
        count(case when e.ticket_priority = 'urgent' then 1 end)        as urgent_tickets,
        count(case when e.ticket_priority = 'high' then 1 end)          as high_priority_tickets,

        -- channel mix
        count(case when e.channel = 'email' then 1 end)                 as email_tickets,
        count(case when e.channel = 'web' then 1 end)                   as web_tickets,
        count(case when e.channel = 'chat' then 1 end)                  as chat_tickets,
        count(case when e.channel = 'api' then 1 end)                   as api_tickets,

        -- response time
        round(avg(e.first_reply_time_business_minutes), 2)              as avg_first_reply_time_minutes,
        round(avg(case when e.is_resolved
            then e.full_resolution_time_business_minutes end), 2)       as avg_resolution_time_minutes,

        -- SLA compliance
        count(case when s.first_reply_sla_met then 1 end)               as first_reply_sla_met_count,
        count(case when s.first_reply_sla_met is not null then 1 end)   as first_reply_sla_evaluated_count,
        count(case when s.resolution_sla_met then 1 end)                as resolution_sla_met_count,
        count(case when s.resolution_sla_met is not null then 1 end)    as resolution_sla_evaluated_count,

        -- satisfaction
        count(case when e.satisfaction_score = 'good' then 1 end)       as good_satisfaction_count,
        count(case when e.satisfaction_score = 'bad' then 1 end)        as bad_satisfaction_count

    from enriched e
    left join sla s on e.ticket_id = s.ticket_id
    group by 1

)

select
    *,
    case
        when first_reply_sla_evaluated_count > 0
        then round(first_reply_sla_met_count * 100.0 / first_reply_sla_evaluated_count, 2)
        else null
    end as first_reply_sla_pct,
    case
        when resolution_sla_evaluated_count > 0
        then round(resolution_sla_met_count * 100.0 / resolution_sla_evaluated_count, 2)
        else null
    end as resolution_sla_pct

from daily
