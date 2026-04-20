import re
import json

# ── 1. Load field order from schema ──────────────────────────────────────────
with open('data/fangraphs/batting/batting_schema.json') as f:
    schema = json.load(f)
fields = list(schema.keys())

# ── 2. Parse long names from macro files ─────────────────────────────────────
macro_files = [
    'buehrle_dbt/macros/fangraphs_context_columns.sql',
    'buehrle_dbt/macros/fangraphs_standard_and_advanced_columns.sql',
    'buehrle_dbt/macros/fangraphs_batted_ball_columns.sql',
    'buehrle_dbt/macros/fangraphs_value_columns.sql',
    'buehrle_dbt/macros/fangraphs_win_probability_columns.sql',
    'buehrle_dbt/macros/fangraphs_sports_info_solutions_columns.sql',
    'buehrle_dbt/macros/fangraphs_statcast_columns.sql',
    'buehrle_dbt/macros/fangraphs_violation_columns.sql',
    'buehrle_dbt/macros/fangraphs_pitch_info_columns.sql',
]

long_name_map = {}
pattern = re.compile(r"'raw_name':\s*'([^']+)'.*?'column_name':\s*'([^']+)'", re.DOTALL)
for path in macro_files:
    with open(path) as f:
        content = f.read()
    for raw_name, col_name in pattern.findall(content):
        if raw_name not in long_name_map:
            long_name_map[raw_name] = col_name

# ── 3. Manual overrides (JSON key vs macro raw_name discrepancies) ────────────
# JSON has 'BB%+' and 'K%+'; macros have 'BB+' and 'K+'
long_name_map['BB%+'] = long_name_map.get('BB+', 'walk_percentage_plus')
long_name_map['K%+']  = long_name_map.get('K+',  'strikeout_percentage_plus')
# JSON has 'position' (lowercase); macro has 'Position'
long_name_map['position'] = long_name_map.get('Position', 'position')
# JSON has pfxvXX (no %); macro has pfxvXX% — map JSON keys to correct long names
pfxv_pairs = [
    ('pfxvFA', 'pfxvFA%'), ('pfxvFT', 'pfxvFT%'), ('pfxvFC', 'pfxvFC%'),
    ('pfxvFS', 'pfxvFS%'), ('pfxvFO', 'pfxvFO%'), ('pfxvSI', 'pfxvSI%'),
    ('pfxvSL', 'pfxvSL%'), ('pfxvCU', 'pfxvCU%'), ('pfxvKC', 'pfxvKC%'),
    ('pfxvEP', 'pfxvEP%'), ('pfxvCH', 'pfxvCH%'), ('pfxvSC', 'pfxvSC%'),
    ('pfxvKN', 'pfxvKN%'),
]
for json_key, macro_key in pfxv_pairs:
    if macro_key in long_name_map:
        long_name_map[json_key] = long_name_map[macro_key]

# ── 4. DuckDB-safe raw name transformation ────────────────────────────────────
def to_raw_name(name):
    if name.startswith('-'):
        name = 'neg_' + name[1:]
    elif name.startswith('+'):
        name = 'pos_' + name[1:]
    name = name.replace('%', '_pct')
    name = name.replace('/C', '_per_c')   # /C before general /
    name = name.replace('/', '_per_')
    name = name.replace('+', '_plus')
    name = re.sub(r'-$', '_minus', name)  # trailing - (e.g. ERA-, FIP-)
    name = name.replace('-', '_')             # mid-name - (e.g. pfxFA-X, O-Swing)
    name = name.lower()
    name = re.sub(r'_+', '_', name).strip('_')  # clean up before digit check
    if name and name[0].isdigit():
        name = '_' + name  # prefix after strip so it isn't removed
    return name

def needs_quoting(name):
    return not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name)

def struct_accessor(field):
    if needs_quoting(field):
        return f'fg."{field}"'
    return f'fg.{field}'

# ── 5. Integer fields ─────────────────────────────────────────────────────────
integer_fields = {
    'xMLBAMID', 'Season', 'SeasonMin', 'SeasonMax', 'playerid', 'teamid',
    'G', 'AB', 'PA', 'H', '1B', '2B', '3B', 'HR', 'R', 'RBI',
    'BB', 'IBB', 'SO', 'HBP', 'SF', 'SH', 'GDP', 'SB', 'CS',
    'GB', 'FB', 'LD', 'IFFB', 'Pitches', 'Balls', 'Strikes', 'IFH', 'BU', 'BUH',
    'Pull', 'Cent', 'Oppo', 'Soft', 'Med', 'Hard', 'bipCount',
    'TG', 'TPA', 'PH', 'Events', 'Barrels', 'HardHit', 'EBV', 'ESV',
    'PPTV', 'CPTV', 'BPTV', 'DSV', 'DGV', 'BTV',
}

def get_type(key, val):
    if isinstance(val, bool):  return 'BOOLEAN'
    if isinstance(val, str):   return 'VARCHAR'
    if key in integer_fields:  return 'INTEGER'
    return 'DOUBLE'

# ── 6. Build schema JSON string ───────────────────────────────────────────────
schema_parts = [f'        "{k}": "{get_type(k, v)}"' for k, v in schema.items()]
schema_json = '{\n' + ',\n'.join(schema_parts) + '\n        }'

# ── 7. Build renamed CTE select lines ────────────────────────────────────────
renamed_lines = []
for field in fields:
    raw  = to_raw_name(field)
    long = long_name_map.get(field, raw)
    acc  = struct_accessor(field)
    renamed_lines.append((acc, raw, long))

select_lines = []
for i, (acc, raw, long) in enumerate(renamed_lines):
    prefix = '        ' if i == 0 else '      , '
    select_lines.append(f'{prefix}{acc:<40} as {raw}')
    if raw != long:
        select_lines.append(f'      , {acc:<40} as {long}')

renamed_select = '\n'.join(select_lines)

# ── 8. Assemble the complete SQL ──────────────────────────────────────────────
sql = (
    "with source as (\n\n"
    "    select\n"
    "        json_transform(record_content, '" + schema_json + "') as fg\n\n"
    "    from {{ source('fangraphs', 'batting') }}\n\n"
    ")\n\n"
    ", register as (\n\n"
    "    select * from {{ ref('chadwick__register') }}\n\n"
    ")\n\n"
    ", teams as (\n\n"
    "    select * from {{ ref('base_utils__fangraphs_team_ids') }}\n\n"
    ")\n\n"
    ", renamed as (\n\n"
    "    select\n"
    + renamed_select + "\n\n"
    "    from source\n\n"
    ")\n\n"
    ", final as (\n\n"
    "    select\n"
    "        *\n\n"
    "    from renamed\n"
    "    left join register\n"
    "        on renamed.playerid = register.fangraphs_id\n"
    "    left join teams\n"
    "        on renamed.teamid = teams.fangraphs_team_id\n"
    "       and renamed.season = teams.year_id\n\n"
    ")\n\n"
    "select * from final\n"
)

with open('buehrle_dbt/models/sources/fangraphs/fangraphs__batting_v2.sql', 'w') as f:
    f.write(sql)

print(f"Done. {len(fields)} fields processed.")
print(f"Fields with no long-name mapping (raw=long): {[f for f in fields if f not in long_name_map]}")
