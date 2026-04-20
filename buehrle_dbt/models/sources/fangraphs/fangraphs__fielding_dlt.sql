select
    playerid as fangraphs_id
  , season   as year_id
  , * exclude (playerid, season)

from {{ source('fangraphs_dlt', 'fielding') }}
