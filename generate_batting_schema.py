import re
import json
from collections import OrderedDict

# ── 1. Load field definitions from batting_schema.json ───────────────────────
with open('data/fangraphs/batting/batting_schema.json') as f:
    schema = json.load(f)

# ── 2. Integer fields (from generate_batting_v2.py) ─────────────────────────
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

# ── 3. DuckDB-safe short name transformation ─────────────────────────────────
def to_short_name(name):
    if name.startswith('-'):
        name = 'neg_' + name[1:]
    elif name.startswith('+'):
        name = 'pos_' + name[1:]
    name = name.replace('%', '_pct')
    name = name.replace('/C', '_per_c')
    name = name.replace('/', '_per_')
    name = name.replace('+', '_plus')
    name = re.sub(r'-$', '_minus', name)
    name = name.replace('-', '_')
    name = name.lower()
    name = re.sub(r'_+', '_', name).strip('_')
    if name and name[0].isdigit():
        name = '_' + name
    return name

# ── 4. Parse macro files to get raw_name → column_name per group ────────────
macro_groups = OrderedDict([
    ('context', 'buehrle_dbt/macros/fangraphs_context_columns.sql'),
    ('standard_and_advanced', 'buehrle_dbt/macros/fangraphs_standard_and_advanced_columns.sql'),
    ('batted_ball', 'buehrle_dbt/macros/fangraphs_batted_ball_columns.sql'),
    ('sports_info_solutions', 'buehrle_dbt/macros/fangraphs_sports_info_solutions_columns.sql'),
    ('pitch_info', 'buehrle_dbt/macros/fangraphs_pitch_info_columns.sql'),
    ('statcast', 'buehrle_dbt/macros/fangraphs_statcast_columns.sql'),
    ('value', 'buehrle_dbt/macros/fangraphs_value_columns.sql'),
    ('win_probability', 'buehrle_dbt/macros/fangraphs_win_probability_columns.sql'),
    ('violation', 'buehrle_dbt/macros/fangraphs_violation_columns.sql'),
])

pattern = re.compile(r"'raw_name':\s*'([^']+)'.*?'column_name':\s*'([^']+)'", re.DOTALL)

# Parse each macro file: group_name → list of (raw_name, column_name)
group_columns = OrderedDict()
for group_name, path in macro_groups.items():
    with open(path) as f:
        content = f.read()
    columns = pattern.findall(content)
    group_columns[group_name] = columns

# Build a map: raw_name → (group_name, long_name)
# Handle batting-specific columns and the stat == 'batting' branches
raw_to_group = {}
raw_to_long = {}
for group_name, columns in group_columns.items():
    for raw_name, col_name in columns:
        if raw_name not in raw_to_group:
            raw_to_group[raw_name] = group_name
            raw_to_long[raw_name] = col_name

# ── 5. Manual overrides for JSON key vs macro raw_name discrepancies ─────────
# JSON has 'BB%+' and 'K%+'; macros have 'BB+' and 'K+'
raw_to_group['BB%+'] = raw_to_group.get('BB+', 'standard_and_advanced')
raw_to_long['BB%+'] = raw_to_long.get('BB+', 'walk_percentage_plus')
raw_to_group['K%+'] = raw_to_group.get('K+', 'standard_and_advanced')
raw_to_long['K%+'] = raw_to_long.get('K+', 'strikeout_percentage_plus')

# JSON has 'position' (lowercase); macro has 'Position'
raw_to_group['position'] = raw_to_group.get('Position', 'context')
raw_to_long['position'] = raw_to_long.get('Position', 'position')

# JSON has pfxvXX (no %); macros have pfxvXX% — map JSON keys to correct group/long
pfxv_pairs = [
    ('pfxvFA', 'pfxvFA%'), ('pfxvFT', 'pfxvFT%'), ('pfxvFC', 'pfxvFC%'),
    ('pfxvFS', 'pfxvFS%'), ('pfxvFO', 'pfxvFO%'), ('pfxvSI', 'pfxvSI%'),
    ('pfxvSL', 'pfxvSL%'), ('pfxvCU', 'pfxvCU%'), ('pfxvKC', 'pfxvKC%'),
    ('pfxvEP', 'pfxvEP%'), ('pfxvCH', 'pfxvCH%'), ('pfxvSC', 'pfxvSC%'),
    ('pfxvKN', 'pfxvKN%'),
]
for json_key, macro_key in pfxv_pairs:
    if macro_key in raw_to_long:
        raw_to_group[json_key] = raw_to_group[macro_key]
        raw_to_long[json_key] = raw_to_long[macro_key]

# ── 6. Build the schema JSON ────────────────────────────────────────────────
# For each field in batting_schema.json, assign it to a group
# Fields not in any macro go into an "unmapped" group at the end

# Initialize output structure
output = []
group_entries = OrderedDict()
for group_name in macro_groups.keys():
    group_entries[group_name] = []

unmapped = []

for field_name, field_val in schema.items():
    short_name = to_short_name(field_name)
    long_name = raw_to_long.get(field_name, short_name)
    field_type = get_type(field_name, field_val)

    include_short = True
    include_long = True

    # If short_name == long_name, only need one
    if short_name == long_name:
        include_long = False

    entry = OrderedDict([
        ("raw_name", field_name),
        ("type", field_type),
        ("short_name", short_name),
        ("long_name", long_name),
        ("include_short_name", include_short),
        ("include_long_name", include_long),
    ])

    group = raw_to_group.get(field_name)
    if group and group in group_entries:
        group_entries[group].append(entry)
    else:
        unmapped.append(entry)

# Build final output array
for group_name, entries in group_entries.items():
    if entries:
        output.append(OrderedDict([(group_name, entries)]))

if unmapped:
    output.append(OrderedDict([("unmapped", unmapped)]))

# ── 7. Write output ─────────────────────────────────────────────────────────
with open('data/fangraphs/batting/fangraphs_batting_schema.json', 'w') as f:
    json.dump(output, f, indent=4)

# Print summary
total = sum(len(list(g.values())[0]) for g in output)
print(f"Done. {total} fields across {len(output)} groups.")
for g in output:
    group_name = list(g.keys())[0]
    count = len(g[group_name])
    print(f"  {group_name}: {count} fields")
