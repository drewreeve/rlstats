# Replay Ingestion Pipeline
# rrrocket JSON -> SQLite

import json
import sqlite3
from itertools import pairwise
from pathlib import Path
from typing import Any

from db import apply_migrations

TRACKED_PLAYERS = {
    ("steam", "76561197969365901"): "Drew",
    ("steam", "76561198008422893"): "Steve",
    ("steam", "76561197964215253"): "Jeff",
}

PLATFORM_MAP = {
    "OnlinePlatform_Steam": "steam",
    "OnlinePlatform_Epic": "epic",
    "OnlinePlatform_PS4": "ps4",
    "OnlinePlatform_Switch": "switch",
    "OnlinePlatform_NNX": "switch",
    "OnlinePlatform_Xbox": "xbox",  # Not sure if this is needed now
    "OnlinePlatform_Dingo": "xbox",
}

DB_PATH = Path("db/rl_stats.sqlite")
PARSED_REPLAY_DIR = Path("replays")


def _extract_platform_id(player: dict[str, Any]) -> tuple[str, str] | None:
    platform_value = player.get("Platform", {}).get("value", "")
    platform = PLATFORM_MAP.get(platform_value)
    if not platform:
        return None

    if platform == "epic":
        epic_id = player.get("PlayerID", {}).get("fields", {}).get("EpicAccountId", "")
        if epic_id:
            return (platform, epic_id)
        return None

    online_id = player.get("OnlineID", "0")
    if online_id and online_id != "0":
        return (platform, online_id)
    return None


def get_or_create_player(
    conn: sqlite3.Connection, platform: str, platform_id: str, name: str
) -> int:
    tracked = 1 if (platform, platform_id) in TRACKED_PLAYERS else 0
    return conn.execute(
        """INSERT INTO players (platform, platform_id, name, is_tracked) VALUES (?, ?, ?, ?)
           ON CONFLICT(platform, platform_id) DO UPDATE SET name = excluded.name
           RETURNING id""",
        (platform, platform_id, name, tracked),
    ).fetchone()[0]


def _normalize_played_at(raw_played_at: Any) -> str | None:
    if not raw_played_at:
        return None
    try:
        # rrrocket format: YYYY-MM-DD HH-MM-SS
        date_part, time_part = raw_played_at.split(" ")
        h, m, s = time_part.split("-")
        return f"{date_part} {h}:{m}:{s}"
    except ValueError, AttributeError:
        return None


def _detect_game_mode(team_size: Any, map_name: Any) -> str | None:
    if team_size == 3:
        return "3v3"
    if team_size == 2 and map_name and "hoop" in map_name.lower():
        return "hoops"
    if team_size == 2:
        return "2v2"
    return None


def _tracked_player_stats(props: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        p for p in props.get("PlayerStats", [])
        if (identity := _extract_platform_id(p)) and identity in TRACKED_PLAYERS
    ]


def _resolve_team_scores(
    tracked_players: list[dict[str, Any]], team0_score: Any, team1_score: Any
) -> tuple[Any, Any, Any]:
    tracked_teams = {p.get("Team") for p in tracked_players}
    team = tracked_teams.pop() if tracked_teams else None

    if team == 0:
        return team, team0_score, team1_score
    if team == 1:
        return team, team1_score, team0_score
    return team, None, None


def _resolve_result(team_score: Any, opponent_score: Any) -> str | None:
    if team_score is None or opponent_score is None:
        return None
    if team_score > opponent_score:
        return "win"
    if team_score < opponent_score:
        return "loss"
    return None


def _resolve_mvp_player_id(
    conn: sqlite3.Connection, tracked_players: list[dict[str, Any]]
) -> int | None:
    if not tracked_players:
        return None
    mvp_stats = max(tracked_players, key=lambda p: p.get("Score", 0))
    identity = _extract_platform_id(mvp_stats)
    if not identity:
        return None
    platform, platform_id = identity
    mvp_name = TRACKED_PLAYERS[identity]
    return get_or_create_player(conn, platform, platform_id, mvp_name)


# TODO Improve accuracy
# Currently the scoring team gets slightly inflated possession since time
# spent watching the replay is considered part of their possession.
def _calculate_possession(
    replay: dict, tracked_team: int | None
) -> tuple[float | None, float | None]:
    """Calculate ball possession seconds per team from network frame data.

    Returns (team_possession_seconds, opponent_possession_seconds) or (None, None)
    if network data is unavailable.
    """
    if tracked_team is None:
        return None, None

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return None, None

    try:
        hit_team_obj_id = objects.index("TAGame.Ball_TA:HitTeamNum")
    except ValueError:
        return None, None

    # Collect (time, team) for each HitTeamNum update
    touches: list[tuple[float, int]] = []
    for frame in frames:
        for actor in frame.get("updated_actors", []):
            if actor.get("object_id") == hit_team_obj_id:
                team_num = actor.get("attribute", {}).get("Byte")
                if team_num is not None:
                    touches.append((frame["time"], team_num))

    if not touches:
        return None, None

    # Calculate possession between consecutive touches
    possession = {0: 0.0, 1: 0.0}
    for (t_start, team_num), (t_end, _) in pairwise(touches):
        possession[team_num] += t_end - t_start

    # Last touch to end of match
    last_time, last_team = touches[-1]
    match_end = frames[-1]["time"]
    possession[last_team] += match_end - last_time

    team_poss = possession.get(tracked_team, 0.0)
    opp_poss = possession.get(1 - tracked_team, 0.0)
    return round(team_poss, 2), round(opp_poss, 2)


def _calculate_ball_thirds(
    replay: dict, tracked_team: int | None
) -> tuple[float | None, float | None, float | None]:
    """Calculate time the ball spends in each third of the field.

    Returns (defensive_seconds, neutral_seconds, offensive_seconds) from the
    tracked team's perspective, or (None, None, None) if data is unavailable.
    """
    if tracked_team is None:
        return None, None, None

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return None, None, None

    # Find ball archetype to identify ball actors
    try:
        ball_archetype = objects.index("Archetypes.Ball.Ball_Default")
    except ValueError:
        return None, None, None

    # Find the RigidBody replication object ID
    try:
        rb_obj_id = objects.index("TAGame.RBActor_TA:ReplicatedRBState")
    except ValueError:
        return None, None, None

    # Track all ball actor IDs (ball is recreated after goals) and collect
    # (time, y) samples from position updates
    ball_actor_ids: set[int] = set()
    samples: list[tuple[float, float]] = []
    for frame in frames:
        for new_actor in frame.get("new_actors", []):
            if new_actor.get("object_id") == ball_archetype:
                ball_actor_ids.add(new_actor.get("actor_id"))
        for actor in frame.get("updated_actors", []):
            if (
                actor.get("actor_id") in ball_actor_ids
                and actor.get("object_id") == rb_obj_id
            ):
                loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
                if loc and "y" in loc:
                    samples.append((frame["time"], loc["y"]))

    if len(samples) < 2:
        return None, None, None

    # Y boundary for thirds: field is ~10240 units long, split at ±1707
    THIRD_BOUNDARY = 1707

    # Accumulate time in each zone
    zones = {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
    for (t_start, y), (t_end, _) in pairwise(samples):
        dt = t_end - t_start
        if dt <= 0:
            continue
        if y < -THIRD_BOUNDARY:
            zones["negative"] += dt
        elif y > THIRD_BOUNDARY:
            zones["positive"] += dt
        else:
            zones["neutral"] += dt

    # Map to team-relative: team 0 defends negative Y, team 1 defends positive Y
    if tracked_team == 0:
        defensive = zones["negative"]
        offensive = zones["positive"]
    else:
        defensive = zones["positive"]
        offensive = zones["negative"]

    return round(defensive, 2), round(zones["neutral"], 2), round(offensive, 2)


NETWORK_PLATFORM_MAP = {
    "Steam": "steam",
    "Epic": "epic",
    "PlayStation": "ps4",
    "PsyNet": "switch",
    "Xbox": "xbox",
}


def _resolve_network_identity(
    uid: dict[str, Any],
) -> tuple[str, str] | None:
    """Resolve a UniqueId attribute from network frames to (platform, platform_id).

    Handles both string remote_ids (Steam, Epic) and dict remote_ids with
    an 'online_id' field (PlayStation, PsyNet).
    """
    remote = uid.get("remote_id", {})
    if not remote:
        return None
    platform_key = next(iter(remote))
    platform = NETWORK_PLATFORM_MAP.get(platform_key)
    if not platform:
        return None
    value = remote[platform_key]
    if isinstance(value, dict):
        platform_id = value.get("online_id")
    else:
        platform_id = value
    if not platform_id:
        return None
    return (platform, str(platform_id))


def _extract_demolitions(replay: dict) -> dict[tuple[str, str], int]:
    """Extract per-player demolition counts from network frame data.

    Returns a dict mapping (platform, platform_id) -> demo count,
    or {} if network data is unavailable.
    """
    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return {}

    try:
        demo_obj_id = objects.index("TAGame.PRI_TA:MatchDemolishes")
        uid_obj_id = objects.index("Engine.PlayerReplicationInfo:UniqueId")
    except ValueError:
        return {}

    actor_identity: dict[int, tuple[str, str]] = {}
    actor_demos: dict[int, int] = {}

    for frame in frames:
        for actor in frame.get("updated_actors", []):
            obj_id = actor.get("object_id")
            if obj_id == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    actor_identity[actor["actor_id"]] = identity
            elif obj_id == demo_obj_id:
                val = actor.get("attribute", {}).get("Int", 0)
                aid = actor["actor_id"]
                actor_demos[aid] = max(actor_demos.get(aid, 0), val)

    result: dict[tuple[str, str], int] = {}
    for aid, count in actor_demos.items():
        identity = actor_identity.get(aid)
        if identity:
            result[identity] = count
    return result


BIG_PAD_POSITIONS = {
    "standard": [
        (-3072, -4096),
        (3072, -4096),
        (-3584, 0),
        (3584, 0),
        (-3072, 4096),
        (3072, 4096),
    ],
    "hoops": [
        (-2176, -2880),
        (2176, -2880),
        (-2400, 0),
        (2400, 0),
        (-2176, 2880),
        (2176, 2880),
    ],
}
# Official hitbox radius is 208uu (wiki.rlbot.org); we use 400 to allow for
# car size and positional jitter in replay network frames.
BIG_PAD_RADIUS = 400
BIG_PAD_RADIUS_SQ = BIG_PAD_RADIUS**2


def _extract_boost_stats(
    replay: dict, tracked_team: int | None, game_mode: str | None
) -> tuple[int | None, int | None, int | None, int | None]:
    """Extract per-team boost collected and boost stolen from network frame data.

    Returns (team_collected, opp_collected, team_stolen, opp_stolen)
    or (None, None, None, None) if data is unavailable.
    """
    if tracked_team is None:
        return None, None, None, None

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return None, None, None, None

    try:
        team_paint_obj_id = objects.index("TAGame.Car_TA:TeamPaint")
        rb_obj_id = objects.index("TAGame.RBActor_TA:ReplicatedRBState")
        pickup_obj_id = objects.index("TAGame.VehiclePickup_TA:NewReplicatedPickupData")
    except ValueError:
        return None, None, None, None

    map_key = "hoops" if game_mode == "hoops" else "standard"
    big_pads = BIG_PAD_POSITIONS[map_key]

    actor_team: dict[int, int] = {}
    actor_position: dict[int, tuple[float, float]] = {}
    last_pickup_state: dict[int, int] = {}
    collected = {0: 0, 1: 0}
    stolen = {0: 0, 1: 0}

    for frame in frames:
        # Clear stale state when actors are deleted and their IDs recycled
        for aid in frame.get("deleted_actors", []):
            actor_team.pop(aid, None)
            actor_position.pop(aid, None)
            last_pickup_state.pop(aid, None)

        for actor in frame.get("updated_actors", []):
            obj_id = actor.get("object_id")
            aid = actor["actor_id"]

            if obj_id == team_paint_obj_id:
                team = actor.get("attribute", {}).get("TeamPaint", {}).get("team")
                if team is not None:
                    actor_team[aid] = team

            elif obj_id == rb_obj_id:
                loc = actor.get("attribute", {}).get("RigidBody", {}).get("location")
                if loc and "x" in loc and "y" in loc:
                    actor_position[aid] = (loc["x"], loc["y"])

            elif obj_id == pickup_obj_id:
                pickup = actor.get("attribute", {}).get("PickupNew", {})
                picked_up_state = pickup.get("picked_up")
                # PickupNew is a replicated state/counter (not bool). Count only on
                # state change to avoid double-counting repeated updates.
                # 255 = no-pickup sentinel in replicated state
                if picked_up_state is None or picked_up_state == 255:
                    continue
                if last_pickup_state.get(aid) == picked_up_state:
                    continue
                last_pickup_state[aid] = picked_up_state

                instigator = pickup.get("instigator")
                if instigator is None:
                    continue
                team = actor_team.get(instigator)
                # Prefer pickup actor position (pad location); fallback to instigator.
                pos = actor_position.get(aid)
                if pos is None:
                    pos = actor_position.get(instigator)
                if team is None or pos is None:
                    continue

                x, y = pos
                # Classify big vs small by proximity to known big pad positions
                is_big = any(
                    (x - bx) ** 2 + (y - by) ** 2 <= BIG_PAD_RADIUS_SQ
                    for bx, by in big_pads
                )
                boost_value = 100 if is_big else 12
                collected[team] += boost_value

                # Stolen = pickup on opponent's half (not center)
                # Team 0 defends Y<0, Team 1 defends Y>0
                if (team == 0 and y > 0) or (team == 1 and y < 0):
                    stolen[team] += boost_value

    if collected[0] == 0 and collected[1] == 0:
        return None, None, None, None

    team_collected = collected[tracked_team]
    opp_collected = collected[1 - tracked_team]
    team_stolen = stolen[tracked_team]
    opp_stolen = stolen[1 - tracked_team]
    return team_collected, opp_collected, team_stolen, opp_stolen


def _extract_match_events(
    replay: dict, tracked_team: int | None
) -> list[tuple[str, float, str, str, int]]:
    """Extract individual match events from network frame data.

    Returns list of (event_type, game_seconds, platform, platform_id, team) tuples.
    """
    if tracked_team is None:
        return []

    objects = replay.get("objects")
    frames = replay.get("network_frames", {}).get("frames")
    if not objects or not frames:
        return []

    # Resolve object IDs
    try:
        sr_obj_id = objects.index("TAGame.GameEvent_Soccar_TA:SecondsRemaining")
        uid_obj_id = objects.index("Engine.PlayerReplicationInfo:UniqueId")
        team_obj_id = objects.index("Engine.PlayerReplicationInfo:Team")
    except ValueError:
        return []

    counter_names = {
        "TAGame.PRI_TA:MatchGoals": "goal",
        "TAGame.PRI_TA:MatchShots": "shot",
        "TAGame.PRI_TA:MatchSaves": "save",
        "TAGame.PRI_TA:MatchDemolishes": "demo",
    }
    counter_obj_ids: dict[int, str] = {}
    for obj_name, event_type in counter_names.items():
        try:
            counter_obj_ids[objects.index(obj_name)] = event_type
        except ValueError:
            pass

    if not counter_obj_ids:
        return []

    # Build game clock: list of (frame_time, seconds_remaining)
    clock_updates: list[tuple[float, int]] = []
    # Build actor identity and team mappings, track counters — all in one pass
    actor_identity: dict[int, tuple[str, str]] = {}
    actor_team_actor: dict[int, int] = {}
    actor_counters: dict[int, dict[str, int]] = {}
    raw_events: list[tuple[str, float, int]] = []  # (event_type, frame_time, actor_id)

    for frame in frames:
        ft = frame["time"]
        for actor in frame.get("updated_actors", []):
            obj_id = actor.get("object_id")
            aid = actor["actor_id"]

            if obj_id == sr_obj_id:
                sr = actor.get("attribute", {}).get("Int")
                if sr is not None:
                    clock_updates.append((ft, sr))

            elif obj_id == uid_obj_id:
                uid = actor.get("attribute", {}).get("UniqueId", {})
                identity = _resolve_network_identity(uid)
                if identity:
                    actor_identity[aid] = identity

            elif obj_id == team_obj_id:
                team_actor = (
                    actor.get("attribute", {}).get("ActiveActor", {}).get("actor")
                )
                if team_actor is not None:
                    actor_team_actor[aid] = team_actor

            elif obj_id in counter_obj_ids:
                event_type = counter_obj_ids[obj_id]
                val = actor.get("attribute", {}).get("Int", 0)
                if aid not in actor_counters:
                    actor_counters[aid] = {}
                prev = actor_counters[aid].get(event_type, 0)
                if val > prev:
                    for _ in range(val - prev):
                        raw_events.append((event_type, ft, aid))
                actor_counters[aid][event_type] = val

    if not clock_updates or not raw_events:
        return []

    # Determine game_start (first SecondsRemaining value, typically 300)
    game_start = clock_updates[0][1]

    # Pre-compute monotonic game_seconds from clock updates.
    # In overtime, SecondsRemaining counts up from 0 instead of down,
    # so we detect the transition and add to game_start instead of subtracting.
    seen_zero = False
    clock_game_seconds: list[tuple[float, float]] = []
    for c_ft, sr in clock_updates:
        if sr == 0:
            seen_zero = True
        if seen_zero and sr > 0:
            clock_game_seconds.append((c_ft, game_start + sr))
        else:
            clock_game_seconds.append((c_ft, game_start - sr))

    # Convert frame_time -> game_seconds using pre-computed values
    def frame_to_game_seconds(ft: float) -> float:
        _, best_gs = clock_game_seconds[0]
        for c_ft, c_gs in clock_game_seconds:
            if c_ft <= ft:
                best_gs = c_gs
            else:
                break
        return best_gs

    # Resolve team actor -> team number
    # Tracked players' actor IDs point to one team actor = tracked_team
    tracked_team_actor = None
    for aid, identity in actor_identity.items():
        if identity in TRACKED_PLAYERS and aid in actor_team_actor:
            tracked_team_actor = actor_team_actor[aid]
            break

    if tracked_team_actor is None:
        return []

    def resolve_team(aid: int) -> int | None:
        ta = actor_team_actor.get(aid)
        if ta is None:
            return None
        return tracked_team if ta == tracked_team_actor else (1 - tracked_team)

    # Build final events
    events: list[tuple[str, float, str, str, int]] = []
    for event_type, ft, aid in raw_events:
        identity = actor_identity.get(aid)
        team = resolve_team(aid)
        if identity is None or team is None:
            continue
        gs = frame_to_game_seconds(ft)
        events.append((event_type, gs, identity[0], identity[1], team))

    return events


def _upsert_match(
    conn: sqlite3.Connection,
    *,
    replay_hash: str,
    played_at_sql: str | None,
    duration: int | None,
    forfeit: int,
    team_size: int | None,
    team: int | None,
    team_score: int | None,
    opponent_score: int | None,
    result: str | None,
    mvp_player_id: int | None,
    map_name: str | None,
    game_mode: str | None,
    team_possession_seconds: float | None,
    opponent_possession_seconds: float | None,
    defensive_third_seconds: float | None,
    neutral_third_seconds: float | None,
    offensive_third_seconds: float | None,
    team_boost_collected: int | None,
    opponent_boost_collected: int | None,
    team_boost_stolen: int | None,
    opponent_boost_stolen: int | None,
) -> int:
    return conn.execute(
        """
        INSERT INTO matches (
            replay_hash,
            played_at, duration_seconds, forfeit, team_size,
            team, team_score, opponent_score, result, team_mvp_player_id,
            map_name, game_mode,
            team_possession_seconds, opponent_possession_seconds,
            defensive_third_seconds, neutral_third_seconds, offensive_third_seconds,
            team_boost_collected, opponent_boost_collected,
            team_boost_stolen, opponent_boost_stolen
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(replay_hash) DO UPDATE SET
            played_at = excluded.played_at,
            duration_seconds = excluded.duration_seconds,
            forfeit = excluded.forfeit,
            team_size = excluded.team_size,
            team = excluded.team,
            team_score = excluded.team_score,
            opponent_score = excluded.opponent_score,
            result = excluded.result,
            team_mvp_player_id = excluded.team_mvp_player_id,
            map_name = excluded.map_name,
            game_mode = excluded.game_mode,
            team_possession_seconds = excluded.team_possession_seconds,
            opponent_possession_seconds = excluded.opponent_possession_seconds,
            defensive_third_seconds = excluded.defensive_third_seconds,
            neutral_third_seconds = excluded.neutral_third_seconds,
            offensive_third_seconds = excluded.offensive_third_seconds,
            team_boost_collected = excluded.team_boost_collected,
            opponent_boost_collected = excluded.opponent_boost_collected,
            team_boost_stolen = excluded.team_boost_stolen,
            opponent_boost_stolen = excluded.opponent_boost_stolen
        RETURNING id
        """,
        (
            replay_hash,
            played_at_sql,
            duration,
            forfeit,
            team_size,
            team,
            team_score,
            opponent_score,
            result,
            mvp_player_id,
            map_name,
            game_mode,
            team_possession_seconds,
            opponent_possession_seconds,
            defensive_third_seconds,
            neutral_third_seconds,
            offensive_third_seconds,
            team_boost_collected,
            opponent_boost_collected,
            team_boost_stolen,
            opponent_boost_stolen,
        ),
    ).fetchone()[0]


def _upsert_match_players(
    conn: sqlite3.Connection,
    match_id: int,
    all_players: list[dict[str, Any]],
    demolitions: dict[tuple[str, str], int],
):
    for player in all_players:
        if player.get("bBot"):
            continue
        identity = _extract_platform_id(player)
        if not identity:
            continue
        platform, platform_id = identity
        name = player.get("Name", "Unknown")
        # Use tracked name if this is a tracked player
        tracked_name = TRACKED_PLAYERS.get(identity)
        if tracked_name:
            name = tracked_name
        player_id = get_or_create_player(conn, platform, platform_id, name)
        demos = demolitions.get(identity, 0)

        conn.execute(
            """
            INSERT INTO match_players (
                match_id, player_id, team,
                goals, assists, saves, shots, score, demos
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, player_id) DO UPDATE SET
                team = excluded.team,
                goals = excluded.goals,
                assists = excluded.assists,
                saves = excluded.saves,
                shots = excluded.shots,
                score = excluded.score,
                demos = excluded.demos
            """,
            (
                match_id,
                player_id,
                player.get("Team"),
                player.get("Goals", 0),
                player.get("Assists", 0),
                player.get("Saves", 0),
                player.get("Shots", 0),
                player.get("Score", 0),
                demos,
            ),
        )


def ingest_match(conn: sqlite3.Connection, replay: dict):
    props = replay.get("properties", {})

    replay_hash = props.get("MatchGUID") or props.get("MatchGuid")
    if not replay_hash:
        return

    played_at_sql = _normalize_played_at(props.get("Date"))
    duration = props.get("TotalSecondsPlayed")
    forfeit = 1 if props.get("bForfeit") else 0
    team_size = props.get("TeamSize")
    map_name = props.get("MapName")
    game_mode = _detect_game_mode(team_size, map_name)

    team0_score = props.get("Team0Score", 0)
    team1_score = props.get("Team1Score", 0)
    tracked_players = _tracked_player_stats(props)
    team, team_score, opponent_score = _resolve_team_scores(
        tracked_players, team0_score, team1_score
    )
    result = _resolve_result(team_score, opponent_score)
    if result is None:
        return

    mvp_player_id = _resolve_mvp_player_id(conn, tracked_players)
    team_poss, opp_poss = _calculate_possession(replay, team)
    def_thirds, neu_thirds, off_thirds = _calculate_ball_thirds(replay, team)
    team_boost, opp_boost, team_stolen, opp_stolen = _extract_boost_stats(
        replay, team, game_mode
    )

    match_id = _upsert_match(
        conn,
        replay_hash=replay_hash,
        played_at_sql=played_at_sql,
        duration=duration,
        forfeit=forfeit,
        team_size=team_size,
        team=team,
        team_score=team_score,
        opponent_score=opponent_score,
        result=result,
        mvp_player_id=mvp_player_id,
        map_name=map_name,
        game_mode=game_mode,
        team_possession_seconds=team_poss,
        opponent_possession_seconds=opp_poss,
        defensive_third_seconds=def_thirds,
        neutral_third_seconds=neu_thirds,
        offensive_third_seconds=off_thirds,
        team_boost_collected=team_boost,
        opponent_boost_collected=opp_boost,
        team_boost_stolen=team_stolen,
        opponent_boost_stolen=opp_stolen,
    )
    demolitions = _extract_demolitions(replay)
    all_players = props.get("PlayerStats", [])
    _upsert_match_players(conn, match_id, all_players, demolitions)

    # Build identity -> player_id map (players were just upserted above)
    player_id_map: dict[tuple[str, str], int] = {}
    for player in all_players:
        identity = _extract_platform_id(player)
        if identity:
            row = conn.execute(
                "SELECT id FROM players WHERE platform = ? AND platform_id = ?",
                (identity[0], identity[1]),
            ).fetchone()
            if row:
                player_id_map[identity] = row[0]

    # Clear old events before re-inserting
    conn.execute("DELETE FROM match_events WHERE match_id = ?", (match_id,))
    match_events = _extract_match_events(replay, team)
    for event_type, game_seconds, platform, platform_id, ev_team in match_events:
        player_id = player_id_map.get((platform, platform_id))
        if player_id is None:
            continue
        conn.execute(
            "INSERT INTO match_events (match_id, event_type, game_seconds, player_id, team) VALUES (?, ?, ?, ?, ?)",
            (match_id, event_type, game_seconds, player_id, ev_team),
        )


def ingest_all():
    DB_PATH.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    apply_migrations(conn)

    for path in sorted(PARSED_REPLAY_DIR.glob("*.json")):
        with open(path, "r", encoding="utf-8") as f:
            replay = json.load(f)
        ingest_match(conn, replay)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    ingest_all()
