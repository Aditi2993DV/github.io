with source as (

    select * from {{ source('zendesk', 'user') }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- ids
        id                                          as user_id,
        organization_id,

        -- user attributes
        name                                        as user_name,
        email,
        role                                        as user_role,
        active                                      as is_active,
        suspended                                   as is_suspended,
        locale,
        time_zone,

        -- timestamps
        created_at::timestamp_ntz                    as created_at,
        updated_at::timestamp_ntz                    as updated_at,
        last_login_at::timestamp_ntz                 as last_login_at,

        -- fivetran metadata
        _fivetran_synced::timestamp_ntz              as _fivetran_synced

    from source

)

select * from renamed
