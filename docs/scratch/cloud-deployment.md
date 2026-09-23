# Deploy buehrle for a daily cron on a Hetzner VM (DuckLake, remotely reachable)

## Context

Today buehrle runs locally: loaders write to a DuckDB file (or a local DuckLake
catalog via `docker compose`) and are triggered by hand through the TUI or the
`buehrle loads` CLI. We want it to run **unattended, once a day**, on a cloud box
that keeps the data fresh, using the **DuckLake backend**, with the resulting
data **readable from off the box** (laptop / dashboard / other service).

Decisions already made:
- **Host:** a single always-on Hetzner Cloud VM (~$5–11/mo) runs both the daily
  job and the Postgres catalog. Simplest, one vendor, cheapest of the options
  weighed.
- **Backend:** DuckLake (Postgres catalog + Parquet data files).
- **Reachability:** required. This is the load-bearing constraint below.

### The reachability constraint (why object storage is needed)

DuckLake has no query server. A remote reader opens the **Postgres catalog** and
then reads the **Parquet files directly** at the `data_path` recorded in the
catalog. Postgres is reachable over the network, but if the Parquet lives on the
VM's local disk, an off-box DuckDB cannot read it. So a *remotely readable*
DuckLake requires the Parquet on **S3-compatible object storage** (Hetzner
Object Storage; Cloudflare R2 is a fine alternative with zero egress fees).

The current code only supports a **local Parquet directory**, so a small,
contained code change is needed (see next section). This is real scope beyond
"just deploy it" — flagged here so it's explicit.

## Code changes: teach DuckLake to use S3-compatible object storage

All backend wiring lives in `loaders/dlt_utils.py`; loaders/CLI need no changes.

1. **`ducklake_storage_path()`** (`loaders/dlt_utils.py:31`) — today it runs
   `os.path.normpath(raw) + os.sep`, which corrupts a URI (`s3://bucket/x/` →
   `s3:/bucket/x/`, collapsing the `//`). Guard it: if `raw` contains `://`,
   return it unchanged (ensure a single trailing `/`); otherwise keep the
   existing local-path normalization.

2. **`open_read_connection()`** (`loaders/dlt_utils.py:75`, the `ducklake`
   branch) — before `ATTACH`, add `INSTALL httpfs; LOAD httpfs;` and
   `CREATE SECRET (TYPE s3, KEY_ID …, SECRET …, ENDPOINT …, URL_STYLE 'path')`
   so the `DATA_PATH 's3://…'` attach can read the Parquet. Read credentials
   from new env vars.

3. **`make_pipeline()`** (`loaders/dlt_utils.py:58`, the `ducklake` branch) —
   the write path passes `storage=` to `DuckLakeCredentials`. Confirm dlt's
   ducklake destination writes Parquet to the S3 URI and picks up S3 credentials
   (via dlt config/env or an explicit filesystem credential). **This is the main
   unknown** — validate with a one-loader smoke test before the full backfill;
   if dlt needs explicit filesystem creds, wire them here.

4. **New env vars** (document in `AGENTS.md` alongside the existing DuckLake
   ones): `BUEHRLE_DUCKLAKE_STORAGE=s3://<bucket>/ducklake/` plus S3
   `KEY_ID` / `SECRET` / `ENDPOINT` / region. Keep them in an untracked env file
   loaded by the cron/systemd unit — never commit.

### Local deploys stay unaffected

The changes are additive and gated on whether the storage path is a **remote
URI** (contains `://`) or S3 env vars are set. They do not change local behavior:

- **Default DuckDB backend** (the local default): untouched. `resolve_backend()`
  returns `duckdb` unless `BUEHRLE_BACKEND=ducklake`, so none of the modified
  branches run. Local `buehrle load ...` against the `.duckdb` file is unchanged.
- **`ducklake_storage_path()`:** a local path (`data/ducklake/`) has no `://`, so
  it takes the existing `normpath + sep` branch exactly as today. Only URIs take
  the new branch.
- **`open_read_connection()` / `make_pipeline()`:** the `httpfs` + `CREATE
  SECRET` and S3-credential wiring must be **conditional on a remote URI / S3
  env being present**. With a local DuckLake dir and no S3 config, that code is
  skipped and behavior is identical to today.

Design rule that keeps both working: **detect local vs. remote by the `://` in
the storage path, and only apply the S3-specific steps when remote.** A test
should cover both branches of `ducklake_storage_path()`.

## VM setup (one-time)

1. **Provision** a Hetzner Cloud VM, Ubuntu 24.04. Start at **CX22** (2 vCPU /
   4 GB); size up to **CX32** (8 GB) if the Statcast loads hit memory limits.
   Attach SSH key; enable the Hetzner firewall (SSH in, Postgres *not* open to
   the world — see reachability).
2. **Base tooling:** `git`, `uv`, Docker + compose plugin, build-essential.
3. **Chadwick `cwtools`** (only the `retrosheet-events` loader needs it). The
   repo's `buehrle install-chadwick` wraps `brew install chadwick`, which is
   awkward on Linux. On Ubuntu, build from source
   (`chadwickbureau/chadwick`: `./configure && make && sudo make install`) or
   install Homebrew-on-Linux. If you don't need `retrosheet-events` initially,
   skip this and exclude that loader.
4. **Clone repo + `uv sync`.** Package installs `buehrle` on PATH.
5. **Postgres catalog:** `docker compose up -d`. **Change the default
   `buehrle/buehrle` credentials** in `docker-compose.yml` and set
   `BUEHRLE_DUCKLAKE_CATALOG` to match — the defaults are dev-only.
6. **Object storage:** create the bucket + access keys in Hetzner Object Storage
   (or R2); put the keys in the env file.
7. **Static data prereqs:** `buehrle retrosheet-sync` (clones Retrosheet); upload
   the annual **Lahman** CSVs into `data/lahman/` (manual — no scriptable URL;
   it's an annual release, so the daily cron won't refresh it).
8. **Initial backfill:** `BUEHRLE_BACKEND=ducklake buehrle loads --full-refresh`
   (long-running; do it once, off the cron path). Consider
   `BUEHRLE_MAX_CONCURRENCY=2` on a 2-vCPU box to bound memory.

## Daily schedule

Prefer a **systemd timer + service** over a raw crontab line (cleaner env
handling, `journalctl` logs, and it won't overlap). Equivalent behavior to a
cronjob:

- `buehrle-loads.service`: `EnvironmentFile=` the secrets file, `WorkingDirectory=`
  the repo, `ExecStart=/…/uv run buehrle loads`. `buehrle loads` already resolves
  the smart-incremental plan and exits non-zero on any loader failure
  (`loaders/loads.py:176`), which surfaces as a failed unit.
- `buehrle-loads.timer`: `OnCalendar=` a daily off-peak time; `Persistent=true`.
- Logs: `buehrle loads` tees to `logs/loads-<timestamp>.log` by default and
  stdout goes to the journal — no extra redirection needed.

(If you specifically want crontab: one line with `flock` to prevent overlap,
sourcing the env file, `>> /var/log/buehrle.log 2>&1`.)

## Remote reachability

- **Catalog (Postgres):** do **not** expose 5432 to the internet with the dev
  creds. Recommended: put the VM and your reader on **Tailscale** and reach
  Postgres over the tailnet. Alternative: Hetzner firewall allow-list your IPs +
  TLS + strong creds.
- **Data (Parquet):** already reachable via the object-storage S3 API with the
  access keys.
- **Reader snippet** (laptop): `INSTALL ducklake; INSTALL postgres; INSTALL
  httpfs;` → `CREATE SECRET` for S3 → `ATTACH 'ducklake:postgres:<dsn>' AS
  ducklake (DATA_PATH 's3://…', READ_ONLY, METADATA_SCHEMA 'ducklake')` →
  `USE ducklake;`. Mirror `open_read_connection()` in `loaders/dlt_utils.py`.

## Verification

1. **Local, pre-deploy:** with the code changes, point `BUEHRLE_DUCKLAKE_STORAGE`
   at a test bucket and run
   `BUEHRLE_BACKEND=ducklake buehrle load mlb-statsapi-schedules` (fast, one HTTP
   call). Confirm Parquet objects appear in the bucket and
   `BUEHRLE_BACKEND=ducklake buehrle state` reads them back. This validates the
   dlt-writes-to-S3 unknown cheaply.
2. **On the VM:** run the same one-loader smoke test, then a full
   `buehrle loads` run; check `journalctl -u buehrle-loads` and the exit status.
3. **Remote read:** from your laptop, run the reader snippet over Tailscale and
   `SELECT` from a loaded table to prove end-to-end reachability.
4. **Schedule:** `systemctl start buehrle-loads.service` once by hand, confirm
   success, then confirm the timer is enabled and `systemctl list-timers` shows
   the next run.
5. **Regression:** `uv run pytest` still green after the `dlt_utils.py` changes
   (coverage gate is 90%; add a case for the URI-vs-local path branch in
   `ducklake_storage_path`). Confirms local deploys are unaffected.

## Open items to confirm

- Object storage: **Hetzner Object Storage** (same vendor) vs **Cloudflare R2**
  (zero egress on remote reads). Plan assumes Hetzner; R2 is a drop-in if remote
  read volume is high.
- Whether `retrosheet-events` (and thus the Chadwick source build) is needed at
  launch, or can be deferred.
