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
