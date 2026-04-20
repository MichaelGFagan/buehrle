import csv
from difflib import SequenceMatcher

def fuzzy_match(str1, str2):
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, str1.upper(), str2.upper()).ratio()

# Read missing_teams.csv
missing_teams = []
with open('data/missing_teams.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        missing_teams.append(row)

# Read Lahman Teams.csv and create a lookup by (yearID, teamID)
lahman_teams = {}
with open('data/lahman/Teams.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row['yearID'], row['teamID'])
        lahman_teams[key] = row

# Match missing teams and create output
matched_results = []
unmatched = []

for missing_team in missing_teams:
    team_id_fg = missing_team['team_id']
    team_name = missing_team['team_name']
    year_id = missing_team['year_id']
    
    # Try exact match first on (yearID, teamID)
    key = (year_id, team_name)
    
    if key in lahman_teams:
        # Exact match found
        entry = lahman_teams[key]
        result = {
            'yearID': entry['yearID'],
            'lgID': entry['lgID'],
            'teamID': entry['teamID'],
            'franchID': entry['franchID'],
            'teamIDfg': team_id_fg,
            'teamIDBR': entry.get('teamIDBR', ''),
            'teamIDretro': entry.get('teamIDretro', entry['teamID'])
        }
        matched_results.append(result)
        print(f"✓ Matched '{team_name}' in {year_id} (exact match)")
    else:
        # Try fuzzy matching for the same year
        best_match = None
        best_score = 0
        threshold = 0.6
        
        for (lahman_year, lahman_team_id), lahman_entry in lahman_teams.items():
            if lahman_year == year_id:
                score = fuzzy_match(team_name, lahman_team_id)
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = lahman_entry
        
        if best_match:
            # Fuzzy match found
            entry = best_match
            result = {
                'yearID': entry['yearID'],
                'lgID': entry['lgID'],
                'teamID': entry['teamID'],
                'franchID': entry['franchID'],
                'teamIDfg': team_id_fg,
                'teamIDBR': entry.get('teamIDBR', ''),
                'teamIDretro': entry.get('teamIDretro', entry['teamID'])
            }
            matched_results.append(result)
            print(f"✓ Matched '{team_name}' in {year_id} -> '{entry['teamID']}' (fuzzy match, score: {best_score:.2f})")
        else:
            # No match found - create empty record with just year_id and team_id
            result = {
                'yearID': year_id,
                'lgID': '',
                'teamID': '',
                'franchID': '',
                'teamIDfg': team_id_fg,
                'teamIDBR': '',
                'teamIDretro': ''
            }
            matched_results.append(result)
            unmatched.append((team_name, year_id, team_id_fg))
            print(f"✗ No match found for '{team_name}' in {year_id} (team_id: {team_id_fg}) - creating empty record")

# Write to team_ids_2.csv
with open('data/team_ids_2.csv', 'w', newline='') as f:
    fieldnames = ['yearID', 'lgID', 'teamID', 'franchID', 'teamIDfg', 'teamIDBR', 'teamIDretro']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    
    # Sort by yearID, teamIDfg
    matched_results.sort(key=lambda x: (x['yearID'], x['teamIDfg']))
    writer.writerows(matched_results)

print(f"\n✓ Created data/team_ids_2.csv with {len(matched_results)} entries")
print(f"  Total rows in missing_teams.csv: {len(missing_teams)}")
print(f"  Matched teams: {len(missing_teams) - len(unmatched)}")
print(f"  Unmatched teams: {len(unmatched)}")

if unmatched:
    print(f"\nUnmatched teams (created empty records):")
    for team_name, year_id, team_id in unmatched[:10]:  # Show first 10
        print(f"  - {team_name} ({year_id}) - ID: {team_id}")
    if len(unmatched) > 10:
        print(f"  ... and {len(unmatched) - 10} more")
