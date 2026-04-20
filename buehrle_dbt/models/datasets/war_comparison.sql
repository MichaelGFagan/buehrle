{{
    config(
        materialized='table'
    )
}}

with bbref_batting as (

    select
        person_id
      , year_id
      , sum(wins_above_replacement) as war

    from {{ ref('baseball_reference__batting_war_dlt') }}
    group by 1, 2

)

, bbref_pitching as (

    select
        person_id
      , year_id
      , sum(wins_above_replacement) as war

    from {{ ref('baseball_reference__pitching_war_dlt') }}
    group by 1, 2

)

, fangraphs_batting as (

    select
        chadwick.person_id
      , fg.year_id
      , sum(fg.war) as war

    from {{ ref('fangraphs__batting_dlt') }} as fg
    inner join {{ ref('chadwick__register') }} as chadwick
        on fg.fangraphs_id = chadwick.fangraphs_id
    group by 1, 2

)

, fangraphs_pitching as (

    select
        chadwick.person_id
      , fg.year_id
      , sum(fg.war) as war

    from {{ ref('fangraphs__pitching_dlt') }} as fg
    inner join {{ ref('chadwick__register') }} as chadwick
        on fg.fangraphs_id = chadwick.fangraphs_id
    group by 1, 2

)

, bbref as (

    select
        coalesce(b.person_id, p.person_id)      as person_id
      , coalesce(b.year_id, p.year_id)          as year_id
      , coalesce(b.war, 0) + coalesce(p.war, 0) as war

    from bbref_batting as b
    full outer join bbref_pitching as p
        on b.person_id = p.person_id
       and b.year_id   = p.year_id

)

, fangraphs as (

    select
        coalesce(b.person_id, p.person_id)      as person_id
      , coalesce(b.year_id, p.year_id)     as year_id
      , coalesce(b.war, 0) + coalesce(p.war, 0) as war

    from fangraphs_batting as b
    full outer join fangraphs_pitching as p
        on b.person_id = p.person_id
       and b.year_id   = p.year_id

)

, combined as (

    select
        coalesce(b.person_id, f.person_id) as person_id
      , coalesce(b.year_id,   f.year_id)   as year_id
      , b.war                              as bbref_war
      , f.war                              as fangraphs_war
      , (b.war + f.war) / 2                as average_war
      , abs(b.war - f.war)                 as war_difference

    from bbref as b
    full outer join fangraphs as f
        on b.person_id = f.person_id
       and b.year_id   = f.year_id

)

select
    combined.person_id
  , chadwick.first_name
  , chadwick.last_name
  , combined.year_id
  , combined.bbref_war
  , combined.fangraphs_war
  , combined.average_war
  , combined.war_difference

from combined
left join {{ ref('chadwick__register') }} as chadwick
    on combined.person_id = chadwick.person_id
