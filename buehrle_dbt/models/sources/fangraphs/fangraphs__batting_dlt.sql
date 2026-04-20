select
    playerid  as fangraphs_id
  , x_mlbamid as mlbam_id
  , season    as year_id
  , * exclude (playerid, x_mlbamid, season)

from {{ source('fangraphs_dlt', 'batting') }}
