with source as (

    select * from {{ source('lahman', 'hall_of_fame') }}

),

lookup as (

    select * from {{ ref('lahman__person_id_lookup') }}

),

transform as (

    select
        lookup.person_id
      , source.yearid::int as year_id
      , source.votedby as voting_body
      , source.ballots::int as total_ballots_cast
      , source.needed::int as votes_needed
      , source.votes::int as votes_received
      , case
            when source.inducted = 'Y' then true::boolean
            when source.inducted = 'N' then false::boolean
        end as was_inducted
      , source.category
      , source.needed_note as note
    
    from source
    left join lookup
        on source.playerid = lookup.lahman_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'person_id',
            'year_id',
            'voting_body']) }} as hall_of_fame_voting_result_id
      , *

    from transform

)

select * from final