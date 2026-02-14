{{
    config(
        materialized='table',
        unique_key='user_id'
    )
}}

/*
    Dimension table for Zendesk users (end-users, agents, admins).
    Grain: one row per user.
*/

with users as (

    select * from {{ ref('stg_zendesk__users') }}

),

organizations as (

    select * from {{ ref('stg_zendesk__organizations') }}

),

final as (

    select
        u.user_id,
        u.user_name,
        u.email,
        u.user_role,
        u.is_active,
        u.is_suspended,
        u.locale,
        u.time_zone,

        -- organization
        u.organization_id,
        o.organization_name,

        -- timestamps
        u.created_at,
        u.updated_at,
        u.last_login_at

    from users u
    left join organizations o on u.organization_id = o.organization_id

)

select * from final
