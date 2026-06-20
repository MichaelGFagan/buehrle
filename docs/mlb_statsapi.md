# MLB Stats API

Notes on the MLB Stats API endpoints, captured as we build loaders against them. The Python wrapper at [MLB-StatsAPI/](../MLB-StatsAPI/) documents endpoint shapes and recommended hydrations — we treat [MLB-StatsAPI/statsapi/endpoints.py](../MLB-StatsAPI/statsapi/endpoints.py) as the canonical param list and the [toddrob99 wiki](https://github.com/toddrob99/MLB-StatsAPI/wiki) as the human-readable companion.

Base URL: `https://statsapi.mlb.com/api/`

Sample API responses live in [loaders/mlb_statsapi/samples/](../loaders/mlb_statsapi/samples/) for reference without re-hitting the API.

## Conventions

- `sportId=1` = MLB. Used everywhere.
- `ver` path param = `v1` for all current endpoints (the live game feed uses `v1.1`).
- Most endpoints accept `fields=` for sparse responses.
- `hydrate=` is the main lever for response richness.
- Date format: `YYYY-MM-DD`.
- Seasons in URLs are strings (`"2026"`), not ints.

---

## seasons

`GET /v1/seasons` — single season (defaults to current)
`GET /v1/seasons/all` — all seasons

### Notes

- `all` is a path segment, not a query param. The wrapper signals this via `path_params` in [endpoints.py](../MLB-StatsAPI/statsapi/endpoints.py).
- Full-history call returns **151 seasons from 1876 to 2026** (as of 2026-05-09).
- Useful for: discovering available seasons, season start/end dates, regular-season boundaries, all-star date, qualifier thresholds (`qualifierPlateAppearances`, `qualifierOutsPitched`).

### Sample

[seasons_all.json](../loaders/mlb_statsapi/samples/seasons_all.json) — `?sportId=1` against `/v1/seasons/all`.

---

## schedule

`GET /v1/schedule`

Returns games for a season, date, or date range. Required: one of `sportId`, `gamePk`, `gamePks`.

### Query params

| Param | Notes |
|---|---|
| `sportId` | `1` for MLB. Required if not querying by gamePk. |
| `season` | One full season per call. Returned ~2,900 games / 3.6 MB JSON for 2026 baseline. |
| `date` | Single date. |
| `startDate` / `endDate` | Date range. Both required if either is set. |
| `teamId` | Filter to a team. |
| `opponentId` | Pair with `teamId` for a matchup. |
| `gameTypes` | Comma-separated. `R`=regular, `S`=spring, `P`=postseason, `F/D/L/W`=postseason rounds, `A`=all-star, `E`=exhibition. Defaults to all. |
| `hydrate` | See below. |
| `fields` | Sparse-fieldset filter. |
| `eventTypes` | Non-game events (e.g. all-star ballot). |
| `venueIds` | Filter by venue. |
| `leagueId` | Filter by league. |
| `gamePk` / `gamePks` | Single game / comma-separated batch. |
| `scheduleType` | Rarely needed. |

### Hydrations

Verified against `/v1/schedule?date=2026-05-08&sportId=1` with a kitchen-sink hydrate string. Each entry shows what the hydration adds at the per-game level.

| Hydration | Adds | Used in schedules loader |
|---|---|---|
| `decisions` | Winner, loser, save pitcher (full bios attached). | yes — keep id+name only |
| `gameInfo` | `attendance`, `firstPitch`, `gameDurationMinutes`. | yes |
| `weather` | `condition`, `temp` (°F string), `wind`. | yes |
| `flags` | No-hitter / perfect-game flags (game-level + per-team). | yes |
| `seriesStatus` | Series result, wins/losses, winning/losing team. | yes |
| `linescore` | Full linescore: line totals, innings array, current state, defense/offense (live only). | partial — line totals + innings sidecar; defense/offense skipped (live-only) |
| `probablePitcher` | Per-team probable pitcher (full bio attached). Variant: `probablePitcher(note,stats)`. | no — pre-game only; null for finished/historical games. Better surfaced via a separate upcoming-games view. |
| `officials` | Umpire crew (4 entries: Home Plate, First/Second/Third Base; postseason adds outfield). | yes — sidecar |
| `team` | Full team metadata per side. **Identical to `/v1/teams/{id}?season=YYYY`** — pure denormalization, no point-in-time signal. | no — defer to teams loader |
| `broadcasts` (or `broadcasts(all)`) | TV/radio/streaming entries, ~13 per game × 27 fields. | no — sidecar deferred |
| `lineups` | Starting lineups per side. | no — sidecar deferred |
| `scoringPlays` | Every scoring play with batter/pitcher. | no — sidecar deferred |
| `venue(timezone)` | Adds timezone to `venue.*`. | not yet |
| `gameInfo` (advanced) | Variants like `gameInfo(scheduledStartTime)` may exist; haven't probed. | n/a |

Hydrations that returned no data on a regular schedule call (likely valid only on team-scoped or other endpoints): `previousSchedule`, `nextSchedule`, `person`, `game(content(...))` (only `content.link` survives at this level).

### Response shape

```
{
  copyright, totalItems, totalEvents, totalGames, totalGamesInProgress,
  dates: [
    { date, games: [
        { gamePk, gameGuid, link, gameType, season, gameDate, officialDate,
          status: { abstractGameState, codedGameState, detailedState, ... },
          teams: {
            away: { team: {...}, leagueRecord: {wins,losses,pct},
                    score, isWinner, splitSquad, seriesNumber,
                    probablePitcher: {...}        # if hydrated
                  },
            home: { ... }
          },
          venue: { id, name, link },
          content: { link },
          doubleHeader, gameNumber, gamesInSeries, seriesGameNumber,
          seriesDescription, tiebreaker, ifNecessary, dayNight,
          scheduledInnings, gamedayType, recordSource,
          # hydration-added:
          linescore, decisions, gameInfo, weather, flags, seriesStatus,
          officials, lineups, broadcasts, scoringPlays
        }
    ]}
  ]
}
```

Per-game leaf-path counts (hydrated): `linescore` 275, `teams` 171, `content` 107, `decisions` 83, `broadcasts` 27, `seriesStatus` 27, `scoringPlays` 24, `lineups` 20, `venue` 9, `officials` 4.

### Notes / gotchas

- One HTTP call returns a full season cleanly (~3.6 MB for 2026). No pagination needed.
- `teams.{home,away}.leagueRecord` reflects record **entering** the game, not after it.
- The `teams.{home,away}.team` subtree is identical to `/v1/teams/{id}?season=YYYY` — denormalization, not point-in-time. Skip in favor of a teams loader keyed on `(team_id, season)`.
- API metadata reports seasons from 1876, but actual schedule rows for the earliest seasons are unverified. We'll find out on first backfill.
- The `seriesStatus` hydration throws a server error specifically on `2014-03-11` — see [statsapi/__init__.py:90-96](../MLB-StatsAPI/statsapi/__init__.py#L90-L96).

### Loader: `mlb_statsapi_schedules`

- `pipeline_name='mlb_statsapi_schedules'`, `dataset_name='mlb_statsapi_schedules'` (per-endpoint schema isolation; full-refresh of schedules won't affect other loaders).
- Hydrate string: `decisions,gameInfo,weather,flags,seriesStatus,linescore,officials`.
- One HTTP call per season (or per date / date range) populates all three tables.

Tables:

| Table | PK | Notes |
|---|---|---|
| `schedules` | `game_pk` | One row per game. ~95 columns. Merge on `game_pk`. |
| `schedules_linescore_innings` | `(game_pk, inning_num)` | One row per inning per game. |
| `schedules_officials` | `(game_pk, official_type)` | One row per umpire (4 per regular game; more in postseason). |

CLI:

```
buehrle load mlb-statsapi-schedules                      # current season (default)
buehrle load mlb-statsapi-schedules --season 2024
buehrle load mlb-statsapi-schedules --start-season 2000 --end-season 2026
buehrle load mlb-statsapi-schedules --date 2026-05-08
buehrle load mlb-statsapi-schedules --start-date 2026-05-01 --end-date 2026-05-08
buehrle load mlb-statsapi-schedules --full-history             # backfill from EARLIEST_SEASON (1876)
buehrle load mlb-statsapi-schedules --full-history --full-refresh  # clean full backfill
```

Season args, date args, and `--full-history` are mutually exclusive. `--full-refresh` is orthogonal: it drops the `mlb_statsapi_schedules` schema before loading via [handle_full_refresh](../loaders/dlt_utils.py#L19), regardless of scope.

`EARLIEST_SEASON = 1876` is hardcoded per the [seasons endpoint](#seasons); update the constant if MLB ever extends back further.

### Deferred sidecar tables

Identified during exploration but not built yet — each maps to a hydration we omit:

| Table | Hydration | When to add |
|---|---|---|
| `schedules_broadcasts` | `broadcasts(all)` | When we need TV/radio/streaming distribution. |
| `schedules_lineups` | `lineups` | When integrating with player-level analysis from the schedule path. |
| `schedules_scoring_plays` | `scoringPlays` | For milestone tracking; may overlap with the `playByPlay` endpoint. |

Plus: team metadata belongs in a future `mlb_statsapi_teams` loader keyed on `(team_id, season)`, and live linescore state (`linescore.defense`, `linescore.offense`) belongs in a live-game-feed loader.

### Sample files

- [schedule_baseline_2026.json](../loaders/mlb_statsapi/samples/schedule_baseline_2026.json) — `?season=2026&sportId=1` (no hydration). 2,928 games, 3.6 MB.
- [schedule_hydrated_2026-05-08.json](../loaders/mlb_statsapi/samples/schedule_hydrated_2026-05-08.json) — `?date=2026-05-08&sportId=1&hydrate=...` (kitchen-sink hydration). 15 games, 1.5 MB.
