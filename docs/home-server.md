# Home server setup

This guide sets buehrle up on a spare Linux box at home (written against a Dell
OptiPlex 9020 running Debian 12) so the loaders run every night against DuckLake.
We moved off cloud hosting because Cloudflare blocks data-center IPs from
Baseball Reference and FanGraphs. A home connection usually gets through.

The finished setup looks like this:

- Postgres (the DuckLake catalog) runs in Docker and restarts on boot.
- Parquet files live in `data/ducklake/` on the box's SSD.
- A systemd timer runs `buehrle loads` at about 4 AM. Before each run it pulls new
  Retrosheet files.
- You reach the box over SSH, through Tailscale when you're away from home.

## 1. BIOS

Before wiping the disk, update the BIOS from Dell's support site. Then, in the BIOS
setup (F2 at boot):

- Set **AC Recovery** to "Power On" so the box restarts after a power outage.
- Turn on **Virtualization** (VT-x).

## 2. Install Debian 12

1. Download the [netinst image](https://www.debian.org/distrib/netinst) and write
   it to a USB stick (balenaEtcher or `dd`).
2. Boot from the stick (F12 for the one-time boot menu) and choose the regular
   (non-graphical) install.
3. At "Software selection", **uncheck every desktop environment**. Leave only
   "SSH server" and "standard system utilities" checked.
4. Use the whole disk with the default partitioning.

Then, from your Mac:

```bash
ssh-copy-id <you>@<box-ip>
```

Once key login works, turn off password logins by setting `PasswordAuthentication no`
in `/etc/ssh/sshd_config` and running `sudo systemctl restart ssh`.

Turn on automatic security updates:

```bash
sudo apt install unattended-upgrades
```

### Installing over Wi-Fi

If you install over Wi-Fi and plug into Ethernet later, the Debian installer
only configures the interface you installed with, so Ethernet won't come up on
its own. After you move the box, find the Ethernet interface name with `ip link`
(usually `eno1` on an OptiPlex) and add it to `/etc/network/interfaces`:

```
allow-hotplug eno1
iface eno1 inet dhcp
```

Then run `sudo systemctl restart networking`, or just reboot. Do this while you
still have a keyboard and screen attached.

Wait to set up the DHCP reservation (the fixed IP) until the box is on Ethernet.
Reservations are tied to a network card, and the Wi-Fi and Ethernet cards are
different. Tailscale (step 8) gives the box a fixed name either way.

## 3. System packages

```bash
sudo apt install git curl build-essential tmux
curl -LsSf https://astral.sh/uv/install.sh | sh     # installs uv into ~/.local/bin
```

Install Docker from Docker's own repo, not Debian's package, by following
[Docker's Debian instructions](https://docs.docker.com/engine/install/debian/).
Then let your user run it without sudo:

```bash
sudo usermod -aG docker $USER
```

Log out and back in so the group change and `~/.local/bin` on `PATH` take effect.

## 4. Project and catalog

```bash
git clone https://github.com/MichaelGFagan/buehrle.git
cd buehrle
uv sync
cp .env.example .env
```

Edit `.env` and pick a real Postgres password. It appears twice, once in
`BUEHRLE_POSTGRES_PASSWORD` and once inside `BUEHRLE_DUCKLAKE_CATALOG`, and both
must match. Set it **before** starting Postgres the first time, because Postgres
only reads it when the data volume is empty.

```bash
docker compose up -d --wait
```

The catalog only listens on the box itself (`127.0.0.1`), so nothing else on your
network can reach it.

## 5. Source prerequisites

```bash
uv run buehrle install-chadwick      # builds cwevent from source into ~/.local/bin
uv run buehrle retrosheet-sync       # clones the Retrosheet repo (~4 GB)
```

Lahman has no download URL, so copy the CSVs over from your Mac:

```bash
scp -r data/lahman <you>@<box-ip>:~/buehrle/data/
```

## 6. Initial backfill

Load the env vars into your shell, then smoke-test with one fast loader and one
Cloudflare-protected loader:

```bash
set -a; . ./.env; set +a
uv run buehrle load mlb-statsapi-schedules
uv run buehrle load baseball-reference-war
```

If both succeed, run the full rebuild inside `tmux` so it survives a dropped SSH
connection:

```bash
tmux new -s backfill
set -a; . ./.env; set +a
uv run buehrle loads --full-refresh
```

Detach with `Ctrl-b d` and reattach later with `tmux attach -t backfill`. Expect
this to take hours. Statcast pitches and Retrosheet events are the slow ones.

## 7. Nightly schedule

```bash
./deploy/install-systemd.sh
```

This fills your username and the repo path into the unit files in
`deploy/systemd/`, copies them to `/etc/systemd/system/`, and starts the timer.
Two files do the work:

- `buehrle-loads.service` describes the job: start Postgres if it isn't running,
  pull Retrosheet, then run `uv run buehrle loads` from the repo with `.env` loaded.
- `buehrle-loads.timer` runs that service at 04:00. If the box was off at 04:00,
  it runs once the box comes back up.

Run it once by hand to make sure it works:

```bash
sudo systemctl start buehrle-loads.service    # blocks until the run finishes
systemctl status buehrle-loads.service
```

Checking on it later:

```bash
systemctl list-timers buehrle-loads.timer     # last and next run
journalctl -u buehrle-loads -e                # output from recent runs
ls logs/                                      # per-run logs from `buehrle loads`
```

If you edit the unit files, re-run `./deploy/install-systemd.sh`.

## 8. Remote access

Install [Tailscale](https://tailscale.com/download/linux) on the box and on your
Mac, and sign both into the same account. You can then `ssh <you>@<box-name>`
from anywhere without opening ports on your router.

To query the data, SSH in and use DuckDB on the box. DuckLake keeps the Parquet
files on the box's disk, so a DuckDB session on your Mac can't read them directly.
Reading from another machine would need the Parquet in object storage
(`BUEHRLE_DUCKLAKE_STORAGE=s3://...`; see `AGENTS.md`).
