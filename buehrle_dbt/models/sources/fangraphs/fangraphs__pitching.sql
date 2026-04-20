select
    json(record_content).playerid::int as fangraphs_id
  , json(record_content).Season::int   as year_id
  , record_content
  , extracted_at

from {{ source('fangraphs', 'pitching') }}
