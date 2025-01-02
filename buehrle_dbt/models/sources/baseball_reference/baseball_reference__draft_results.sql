with draft_results as (

    select * from {{ source('baseball_reference', 'draft_results') }}

),

chadwick as (

    select * from {{ ref('chadwick__register') }}

),

transformed as (
    
    select
        draft_results.year as year_id
      , coalesce(draft_results.dt, '6rg') as draft_type
      , draft_results.rnd as round
      , draft_results.rdpck as round_pick
      , draft_results.ovpck as overall_pick
      , draft_results.team_id
      , draft_results.tm as team_name
      , chadwick.person_id
      , replace(replace(draft_results.name, concat(chr(160), '(minors)'), ''), '*', '') as name
      , replace(replace(draft_results.bonus, '$', ''), ',', '')::integer as bonus
      , draft_results.pos as position
      , draft_results.draftedoutof as school
      , draft_results.type as school_type
      , nullif(draft_results.notes, 'NA') as notes
    
    from draft_results
    left join chadwick
        on draft_results.player_id = case draft_results.id_type
                                         when 'baseball_reference_id' then chadwick.baseball_reference_id
                                         when 'baseball_reference_minor_league_id' then chadwick.baseball_reference_minor_league_id
                                         else null
                                     end

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year_id',
            'draft_type',
            'overall_pick']) }} as draft_result_id
      , *

    from transformed

)

select * from final