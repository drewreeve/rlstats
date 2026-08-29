# Domain Context

This file defines the domain language for the RL Stats project. Use these terms consistently in code, tests, comments, and architecture discussions.

## Player Identity

A **player identity** is the stable `(platform, platform_id)` pair that uniquely identifies a player across all data sources. It is the join key between:
- The end-of-game `PlayerStats` blob in the rrrocket JSON (parsed by `ingest.py`)
- The per-frame `UniqueId` network actor attributes (parsed by `frame_analysis.py`)

A player identity is stable for the lifetime of a player's account on a given platform. It is the means by which we track a player's stats accurately across matches.

**Platform** is a normalized short string: `"steam"`, `"epic"`, `"ps4"`, `"xbox"`, `"switch"`.

**Platform ID** is the platform's own identifier for the account (e.g. a Steam64 ID for Steam players, an Epic Account ID for Epic players).

## Display Name

A **display name** is the short, human-readable label configured for a tracked player in
`players.toml` (e.g. `"Drew"` instead of a full in-game handle). Display names are
preferred over in-game names when writing player records to the DB, to keep graph labels
concise. They are resolved at analysis time and carried in `ReplayAnalysis.tracked_names`.

## Tracked Player

A **tracked player** is a player explicitly listed in `players.toml`. The config is the sole source of truth for tracked status — the `is_tracked` flag in the `players` table is a derived cache of config state, not an independent record. A player is tracked if and only if they appear in the config; removal from the config means they are no longer tracked, regardless of DB state.

## Zone

A **zone** is one of three longitudinal regions of the Rocket League pitch, divided at ±1707 units along the y-axis (one-third of ±5120 uu). Zones are named from the perspective of the tracked team:

- **Defensive zone** — the third containing the tracked team's own goal.
- **Neutral zone** — the middle third.
- **Offensive zone** — the third containing the opponent's goal.

Zone membership is determined by y-coordinate: for team 0, the defensive zone is y < −1707 and the offensive zone is y > +1707; for team 1, the mapping is reversed.

Zone time is tracked both for the ball (on `matches`) and per-player (on `match_players`), measured in seconds.

## Player Match Stats

**Player match stats** are the per-player metrics computed from replay frame analysis: demolitions dealt, demolitions received, movement data (boost per minute, average speed, supersonic percentage, pad pickups), and zone time. They complement the scoreboard stats sourced from the replay's properties blob (goals, assists, saves, shots, score) and are assembled by `FrameAnalysis.per_player()` keyed by player identity.

## Match Perspective

A **match perspective** is the tracked-team-relative view of a match outcome: which side the tracked players were on (`team`), their score (`team_score`) vs. the opponent's (`opponent_score`), the win/loss `result`, and the tracked-side `mvp_identity` (the tracked player with the highest score). It is computed once per replay by `resolve_perspective()` in `ingest.py` and carried on `ReplayAnalysis.perspective`. All four pieces of match-outcome knowledge — team assignment, score reorientation, result derivation, and MVP selection — are quarantined inside that function; callers receive a fully typed `MatchPerspective` dataclass and do not need to know how any of them are computed.

## Offensive Pairing

An **offensive pairing** is a matched (scorer, assister) pair within a single match: a goal and an assist by different players on the same team, where the assist occurred within `PAIRING_WINDOW` seconds of the goal. Only pairings where both players are tracked are recorded. The pairing algorithm is greedy: for each goal (processed in order), it claims the temporally nearest unclaimed assist within the window.

## File Outcome

A **file outcome** is the terminal, per-replay-file result of the ingest pipeline, one of three variants defined in `file_outcome.py`: `Written` (the replay was analyzed and its match row written), `Skipped` (carrying a `SkipReason | None` — no match GUID, missing date, or no tracked players — from `validate_replay()`), or `Failed` (carrying an error message, from a corrupt replay or a DB write failure). It is deliberately payload-free: `analyze_replay()` in `ingest.py` produces `Analyzed | Skipped` (`AnalysisResult`), where `Analyzed` carries the heavy `ReplayAnalysis` through the pipeline; `write_parsed_batch()` in `process.py` maps each `Analyzed` to a terminal `Written` once its match row commits, producing the full `Written | Skipped | Failed` (`FileOutcome`).

`file_outcome.encode()` / `decode()` are the sole owners of the `.replay.ingested` sentinel's on-disk string format — `"written"` or `"skipped:<reason>"` — and `sentinel_path()` builds a replay's sentinel path from it (the `RecordedOutcome = Written | Skipped` alias names the encodable subset). Empty sentinels, written before the written/skipped distinction existed, decode as `Written`; an unrecognized `skipped:<reason>` decodes to `Skipped(None)` rather than raising, so a newer writer can't break an older reader. `Failed` files get no sentinel, so they're retried on the next run; `process_unprocessed()` finds already-ingested files by globbing the sentinel name directly.

`file_outcome.reconcile(sentinel_text, recorded_error)` folds the two durable terminal signals into a `FileOutcome` (sentinel wins over a recorded processing error) or `None` when neither is present. `UploadProcessor.status()` calls it, then layers on what is not the file outcome's concern: in-flight pipeline progress (queued / parsing / batch position) and the vanished-replay-file check.

## Query Layer

The **query layer** (`queries.py`) is the sole seam between the web layer and the database for reads. Every `/api` read route calls one `queries.*` function and returns its result unchanged — no reshaping in route closures, no direct use of the aiosql object.

Row shapes are `*Row` `TypedDict`s whose keys mirror each query's SELECT aliases; composite results (`Streaks`, `GoalTiming`) are frozen dataclasses. The aiosql loader (`db.sql`, reading `sql/*.sql`) and all row reshaping — key renaming, rounding, the `timeline` game-mode branch, empty-state defaults — are implementation details of this module, not part of its interface.

`_rows()` / `_first()` / `_one()` are the single `Any`-to-typed hop: they `cast` each `sqlite3.Row` to its row type. The cast is unchecked; `queries.READ_ROW_TYPES` drives a guard in `tests/test_stats_registry.py` that runs every query against a migrated empty database and asserts its projected columns match the row type's keys — the read-side analogue of the `MatchRow` / `MatchPlayerRow` drift check.

The passthrough per-mode stat reads (just an aiosql query whose rows are cast to a row type) are registered in `queries.STAT_READS` — slug → `(query, row_type)` — rather than a function apiece: callers reach one via `queries.stats(slug, conn, game_mode)`, `server.py` builds one `/api/stats/<slug>` route per entry, and `READ_ROW_TYPES` derives its stat half from the same dict. Slugs are therefore public API. Reads that reshape (`timeline`, `streaks`, `goal_timing`) or compose (`matches`, `match_detail`) stay hand-written `queries.*` functions with their row types listed explicitly in `_NON_STAT_READ_ROW_TYPES`.
