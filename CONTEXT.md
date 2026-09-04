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

A **match perspective** is the tracked-team-relative view of a match outcome: which side the tracked players were on (`team`), their score (`team_score`) vs. the opponent's (`opponent_score`), the win/loss `result`, and the tracked-side `mvp_identity` (the tracked player with the highest score). It is computed once per replay by `resolve_perspective()` in `ingest.py`, wrapped in the **Replay Context** (below), and carried on `ReplayAnalysis.context.perspective`. All four pieces of match-outcome knowledge — team assignment, score reorientation, result derivation, and MVP selection — are quarantined inside that function; callers receive a fully typed `MatchPerspective` dataclass and do not need to know how any of them are computed.

## Replay Context

A **replay context** is the tracked-team-relative view of a *parsed replay*, assembled once before frame analysis — the Match Perspective plus everything else a frame consumer needs up front. A `ReplayContext` (frozen, in `ingest.py`) carries five fields: the bot-filtered, identity-keyed `player_stats` blob; the `perspective` (a `MatchPerspective`); the detected `game_mode`; a `player_names` map from every player identity to its preferred display name — the configured `players.toml` name, else the in-game `Name`, else `"Unknown"`; and `tracked_identities`, the frozenset of tracked players **present in this match** (not the whole config).

It is built by `build_replay_context(replay, tracked_players)` — the single owner of that preamble, previously hand-rebuilt in `analyze_replay`, `replay_view.build_replay_frames`, and the `test_replay_frames` integration helper (with the name rule alone existing in three forms). `resolve_perspective()` stays dict-pure; the context wrapper does the `props.get("Team0Score", …)` extraction. `ReplayAnalysis` carries the whole `ReplayContext` as `.context`; `_write_match` and `_upsert_players` read match outcome, player names, and tracked status through it.

The pure frame reshapers — `analyze_frames()` and `extract_replay_frames()` — take the context's fields **unpacked** (`tracked_team`, `tracked_identities`, `player_names`, `game_mode`), never the `ReplayContext` object itself: they live in `frame_analysis.py` / `replay_frames.py`, which `ingest` imports, so depending on `ReplayContext` would be circular.

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

## Replay Wire

The **replay wire** is the contract between `replay_frames.py` and the browser replay viewer (`static/replay-core.js`, `static/replay.js`): the shape of the `/api/matches/{id}/replay` metadata JSON, the `/api/matches/{id}/replay-frames.bin` position buffer, and the `/api/matches/{id}/replay-boost.bin` boost buffer. `replay_frames.py` is its sole owner.

`ReplayFrames.meta_dict()` is the one serialization entry point — `server.py`'s route and `tests/e2e/dump_fixture.py` both call it rather than assembling the dict. It is *not* the shape declaration: that is the hand-written `WIRE_META_KEYS`, `WIRE_SLOT_FIELDS`, `WIRE_GOAL_FIELDS` and `WIRE_TUPLE_WIDTHS` manifest. Those sets are stated independently of the `ActorSlot` / `GoalMarker` dataclasses on purpose — a set derived from `fields()` would follow a rename and never catch it.

The positions `.bin` is `F·N·7` little-endian `float32`, row-major `[frame][slot][x, y, z, qx, qy, qz, qw]`. `pose_offset(slot_count, frame, slot)` and `packed_buffer_bytes(frame_count, slot_count)` are the only expressions of that geometry on the Python side (previously hand-inlined in `_densify` / `_resolve_pickups` and three test sites). `FLOATS_PER_POSE` and `pose_offset` have verbatim twins in `static/replay-core.js` (`FLOATS_PER_POSE`, `poseOffset`); there is no build step binding the two languages, so each carries a keep-in-lockstep comment and `tests/js/replay-core.test.js` pins the constant. This hand-mirror is safe where the boost-pad coordinates' would not be: `7` is a position vec3 plus an orientation quaternion — frozen by the format — whereas pad coordinates are arbitrary map data that changes per arena.

The boost `.bin` is `F·N` raw `uint8` (the network's native 0-255 `ReplicatedBoost` scale), row-major `[frame][slot]` — one byte per pose, no `FLOATS_PER_POSE` stride. `boost_offset(slot_count, frame, slot)` mirrors `pose_offset`'s geometry (and has a verbatim `boostOffset` twin in `replay-core.js`). It is densified by `_densify_boost`, which mirrors `_densify`'s shape but not its interpolation rule: boost drains near-linearly (wall-clock lerp, like pose) but a *pickup* — the value going up — is close to instantaneous, so an increase is held at the lower value and only snaps at the closing real sample, exactly like a kickoff reset (`seg.resets`, which also holds rather than lerps boost — a decrease that lands on a reset frame is a round reset, not a drain, and must not lerp either). A slot whose boost component never resolves (no `TAGame.CarComponent_Boost_TA:ReplicatedBoost` / `TAGame.CarComponent_TA:Vehicle` network data linking it to a car) carries `has_boost: False` on its `ActorSlot`, with its buffer rows left at `0`; the client must check that flag rather than treating an all-zero row as an empty tank.

`tests/test_replay_wire.py` is the drift guard, the replay-side analogue of `tests/test_stats_registry.py` for the Query Layer: it runs a parsed replay through `extract_replay_frames()`, serializes with `jsonable_encoder` exactly as the route does, asserts every emitted key set and packed-row width against the manifest, checks the manifest against the dataclass fields in reverse, and checks the buffer length against `packed_buffer_bytes()`. A renamed field or a changed tuple width fails there — fast, no browser. `test_server.py` keeps only the HTTP-level checks (status, content type, `.bin` length); the semantic invariants (countdown-tick relations, dead-period structure, goal-frame bounds) live in `test_replay_frames.py`.

Changing the wire: update the dataclass and the manifest, update the `replay-core.js` / `replay.js` consumers, and regenerate `tests/data/replay-viewer/{meta.json,frames.bin}` with `tests/e2e/dump_fixture.py`.
