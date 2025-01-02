with source as (

    select * from {{ source('lahman', 'franchises') }}

),

transform as (

    select
        franchid as franchise_id
      , franchname as franchise_name
      , case
            when active = 'Y' then true::boolean
            else false::boolean
        end as is_active
      , naassoc as national_association_team_id

    from source

)

select * from transform