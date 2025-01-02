{{
    config(
        materialized='view'
    )
}}

with chadwick as (

    select person_id, baseball_reference_id  from {{ ref('chadwick__register') }}

)

, people as (

    select playerid as lahman_id, bbrefID as baseball_reference_id from {{ source('lahman', 'people') }}

)

, transform as (

    select
        chadwick.person_id
      , people.lahman_id

    from people
    left join chadwick
        on people.baseball_reference_id = chadwick.baseball_reference_id

)

select * from transform