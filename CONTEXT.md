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

## Offensive Pairing

An **offensive pairing** is a matched (scorer, assister) pair within a single match: a goal and an assist by different players on the same team, where the assist occurred within `PAIRING_WINDOW` seconds of the goal. Only pairings where both players are tracked are recorded. The pairing algorithm is greedy: for each goal (processed in order), it claims the temporally nearest unclaimed assist within the window.
