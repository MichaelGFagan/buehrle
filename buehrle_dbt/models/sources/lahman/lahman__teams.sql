with source as (

    select * from {{ source('lahman', 'teams') }}

),

transform as (

    select
        teamid as team_id
      , franchid as franchise_id
      , divid as division_id
      , lgid as league_id
      , yearid::int as year_id
      , teamidbr as baseball_reference_id
      , teamidretro as retrosheet_id
      , name
      , park
      , attendance::int as attendance
      , rank::int as standings_rank
      , g::int as games
      , ghome::int as home_games
      , w::int as wins
      , l::int as losses
      , case
            when divwin = 'Y' then TRUE
            when divwin = 'N' then FALSE
        end as is_division_winner
      , case
            when wcwin = 'Y' then TRUE
            when wcwin = 'N' then FALSE
        end as is_wild_card_winner
      , case
            when lgwin = 'Y' then TRUE
            when lgwin = 'N' then FALSE
        end as is_league_champion
      , case
            when wswin = 'Y' then TRUE
            when wswin = 'N' then FALSE
        end as is_world_series_champion
      , r::int as runs_scored
      , ab::int as at_bats
      , h::int as hits
      , _2b::int as doubles
      , _3b::int as triples
      , hr::int as home_runs
      , bb::int as walks
      , so::int as strikeouts
      , sb::int as stolen_bases
      , cs::int as caught_stealing
      , hbp::int as hit_by_pitch
      , sf::int as sacrifice_flies
      , ra::int as runs_allowed
      , er::int as earned_runs_allowed
      , era::int as earned_run_average
      , cg::int as complete_games
      , sho::int as shutouts
      , sv::int as saves
      , ipouts::int as outs_pitched
      , ha::int as hits_allowed
      , hra::int as home_runs_allowed
      , bba::int as walks_allowed
      , soa::int as strikeouts_pitched
      , e::int as errors
      , dp::int as double_plays
      , fp::int as fielding_percentage
      , bpf::int as batting_park_factor
      , ppf::int as pitching_park_factor

    from source

)

select * from transform