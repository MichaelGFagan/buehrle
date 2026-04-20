{{ 
  config(
    materialized='table'
    ) 
}}

with source as (

    select * from {{ source('chadwick', 'register') }}

),

transformed as (

    select 
        key_uuid as person_id
      , key_mlbam as mlbam_id
      , key_retro as retrosheet_id
      , key_bbref as baseball_reference_id
      , key_bbref_minors as baseball_reference_minor_league_id
      , key_fangraphs as fangraphs_id
      , key_npb as npb_id
      , key_sr_nfl as pro_football_reference_id
      , key_sr_nba as basketball_reference_id
      , key_sr_nhl as hockey_reference_id
      , strip_accents(name_last) as last_name
      , strip_accents(name_first) as first_name
      , strip_accents(name_given) as given_name
      , name_suffix as suffix
      , name_last as last_name_accented
      , name_first as first_name_accented
      , name_given as given_name_accented
      , name_matrilineal as matrilineal_name
      , case
            when birth_year is null or birth_month is null or birth_day is null then null
            when birth_year > date_part('year', today()) or birth_year < 1800 then null
            when birth_month > 12 or birth_month < 1 then null
            when birth_day > 31 or birth_day < 1 then null
            else make_date(birth_year::bigint, birth_month::bigint, birth_day::bigint)
        end as birth_date
      , birth_year::bigint as birth_year
      , birth_month::bigint as birth_month
      , birth_day::bigint as birth_day
      , case
            when death_year is null or death_month is null or death_day is null then null
            when death_year > date_part('year', today()) or death_year < 1800 then null
            when death_month > 12 or death_month < 1 then null
            when death_day > 31 or death_day < 1 then null
            else make_date(death_year::bigint, death_month::bigint, death_day::bigint)
        end as death_date
      , death_year::bigint as death_year
      , death_month::bigint as death_month
      , death_day::bigint as death_day
      , pro_played_first::bigint as first_pro_game_played
      , pro_played_last::bigint as last_pro_game_played
      , mlb_played_first::bigint as first_mlb_game_played
      , mlb_played_last::bigint as last_mlb_game_played
      , col_played_first::bigint as first_college_game_played
      , col_played_last::bigint as last_college_game_played
      , pro_managed_first::bigint as first_pro_game_managed
      , pro_managed_last::bigint as last_pro_game_managed
      , mlb_managed_first::bigint as first_mlb_game_managed
      , mlb_managed_last::bigint as last_mlb_game_managed
      , col_managed_first::bigint as first_college_game_managed
      , col_managed_last::bigint as last_college_game_managed
      , pro_umpired_first::bigint as first_pro_game_umpired
      , pro_umpired_last::bigint as last_pro_game_umpired
      , mlb_umpired_first::bigint as first_mlb_game_umpired
      , mlb_umpired_last::bigint as last_mlb_game_umpired

    from source

)

select * from transformed
