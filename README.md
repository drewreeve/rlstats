# RL Stats

Small personal project to track the stats of 3 absolute potatoes. Not really
designed for anyone else to use (players are currently hard coded) but if you
really wanted to you could. It's pretty simple really:

* Replay files are parsed by [rrrocket](https://github.com/nickbabcock/rrrocket) into JSON
* JSON gets ingested into SQLite
* Flask frontend to show it all off with some charts courtesy of
  [Chart.js](https://www.chartjs.org/)

## Requirements

Replay files must have a reliable match start time. This means either:
- Rocket League patch 2.43 (September 2024) or later, which includes `MatchStartEpoch`, or
- A pre-2.43 replay saved by BakkesMod, which injects `GameStartTime` into the replay

Pre-2.43 replays saved manually from in-game have neither field and will be skipped during ingest.

## Commands

```bash
uv run pytest                        # Run all tests
uv run pytest tests/test_ingest.py   # Run a specific test file
uv run pytest -k test_match_result   # Run tests matching a pattern
uv run python process.py             # Run rrrocket + ingest new replays into the database
uv run python process.py --force     # Re-process all replays, including already-ingested ones
```

## Configuration

Copy `config/settings.example.toml` to `config/settings.toml` and fill in your settings.

### `[server]` section

| Key | Description | Default |
|---|---|---|
| `upload_password` | Password required to upload replay files. Omit to disable uploads. | *(none)* |
| `secret_key` | Session signing key. Set in production for stable sessions across restarts. | Auto-generated at startup |

### `[[players]]` section

List all tracked players. Each entry needs `platform`, `platform_id`, and `name`.

### Environment variables

| Variable | Description | Default |
|---|---|---|
| `HOST` | Address the server binds to. | `0.0.0.0` |
| `PORT` | Port the server listens on. | `8080` |
| `CONFIG_DIR` | Directory containing `settings.toml`. | `config` |

