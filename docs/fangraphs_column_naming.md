# FanGraphs Column Naming Convention

Column names returned by the FanGraphs API are inconsistent and often contain characters illegal or awkward in SQL (e.g. `BB%`, `1B`, `GB/FB`). The `FangraphsNamingConvention` class in `loaders/fangraphs/check_column_names.py` normalizes these to clean snake_case. It subclasses dlt's built-in `snake_case` naming convention and applies rules before delegating to it.

## Rules (applied in order)

### 1. Hardcoded renames (checked first, bypass all other rules)

| Original | Normalized | Reason |
|---|---|---|
| `1B` | `_1b` | Starts with digit |
| `2B` | `_2b` | Starts with digit |
| `3B` | `_3b` | Starts with digit |
| `-WPA` | `neg_wpa` | Leading `-` rule would mis-apply |
| `+WPA` | `pos_wpa` | Leading `+` not otherwise handled |
| `FB%1` | `fb_pct` | Duplicate of `FB%`; trailing digit disambiguates in FanGraphs |
| `C+SwStr%` | `c_plus_sw_str_pct` | Mixed rules would produce wrong result |
| `CFraming` | `c_framing` | Consecutive-caps rule captures `CF` instead of splitting at `F` |
| `WAROld` | `war_old` | Consecutive-caps rule captures `WARO` instead of splitting at `O` |
| `GDPRuns` | `gdp_runs` | Consecutive-caps rule captures `GDPR` instead of splitting at `R` |
| `rFTeamV` | `r_f_team_v` | Consecutive-caps rule captures `FT` instead of splitting at `T` |
| `rBTeamV` | `r_b_team_v` | Consecutive-caps rule captures `BT` instead of splitting at `T` |
| `CStr%` | `c_str_pct` | Consecutive-caps rule captures `CS` instead of splitting at `S` |
| `CStrikes` | `c_strikes` | Same `CS` issue as `CStr%` |
| `xMLBAMID` | `mlbamid` | The `x` prefix is an artifact; MLBAM ID is the canonical name |
| `ShO` | `sho` | snake_case splits `ShO` into `sh_o` at the trailing cap |
| `E-F` | `era_minus_fip` | ERA minus FIP; requires domain knowledge to expand |
| `TInn` | `t_inn` | Consecutive-caps rule captures `TI` instead of splitting at `I` |

### 2. Prefix detection

These lowercase prefixes are treated as namespace prefixes and separated with an underscore before the rest of the identifier is normalized:

- `pfx` — PITCHf/x data (e.g. `pfxFA-X` → `pfx_fa_x`)
- `sc` — Statcast data (e.g. `scH-Swing%` → `sc_h_swing_pct`)
- `pi` — Pitch Info data (e.g. `piFA%` → `pi_fa_pct`)

`Pitches` is not affected because the prefix check is case-sensitive (lowercase `pi` only).

### 3. Character substitutions

| Character | Position | Replacement | Example |
|---|---|---|---|
| `-` | Leading | `neg_` | `-WPA` → `neg_wpa` (but see hardcoded renames) |
| `-` | Trailing | `_minus` | `ERA-` → `era_minus` |
| `%` | Anywhere | `_pct` | `BB%` → `bb_pct` |
| `+` | Anywhere | `_plus` | `wRC+` → `w_rc_plus` |
| `/` | Anywhere | `_per_` | `GB/FB` → `gb_per_fb` |

Any remaining special characters (e.g. `-` in the middle of an identifier like `O-Swing%`) are passed to dlt's snake_case normalizer, which converts them to underscores.

### 4. Consecutive-capitals rule

Two or more consecutive uppercase letters followed immediately by a lowercase letter are treated as a single abbreviation, with the following lowercase letter starting a new word:

```
CHv  →  CH_v  →  ch_v   (not c_hv)
FBv  →  FB_v  →  fb_v
```

This handles FanGraphs pitch-type abbreviations like `CH` (changeup), `FB` (fastball), `SL` (slider), etc. which are commonly followed by a single-character suffix like `v` (velocity).

**Known limitation:** This rule can misfire when a single-char abbreviation precedes a capitalized full word (e.g. `CFraming`, `WAROld`). These are handled via hardcoded renames above.

### 5. dlt snake_case (applied last)

After the above rules, dlt's built-in snake_case normalizer handles the rest: lowercasing, replacing remaining non-alphanumeric characters with underscores, collapsing consecutive underscores, and stripping leading/trailing underscores.

## Tooling

`loaders/fangraphs/check_column_names.py` fetches a single row from the FanGraphs API and prints a diff of original vs. normalized column names. Change the `stats` param to `bat`, `pit`, or `fld` to check each dataset.

```bash
python loaders/fangraphs/check_column_names.py
```