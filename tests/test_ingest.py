import copy
import sqlite3
from typing import cast

import pytest

from frame_analysis import analyze_frames
from ingest import (
    OffensivePairing,
    SkipReason,
    analyze_replay,
    correlate_pairings,
    get_or_create_player,
    sync_tracked_players,
    validate_replay,
    write_match,
)
from player_identity import PlayerIdentity
from rrrocket_schema import ReplayJSON, ReplayProperties
from tests.fixtures import TRACKED_PLAYERS, cached_db, in_memory_db, load_replay

ALL_FIXTURES = [
    "zero_score.json",
    "match.json",
    "forefeit.json",
    "team_size_2.json",
    "hoops.json",
]


def ingest_fixture(fixture: str) -> sqlite3.Connection:
    return cached_db(fixture)


@pytest.fixture()
def conn_no_network() -> sqlite3.Connection:
    conn = in_memory_db()
    replay = cast(
        ReplayJSON,
        {
            k: v
            for k, v in load_replay("match.json").items()
            if k not in ("network_frames", "objects")
        },
    )
    analysis = analyze_replay(replay, TRACKED_PLAYERS)
    assert analysis is not None
    write_match(conn, analysis)
    return conn


def ingest_all_fixtures():
    return cached_db(*ALL_FIXTURES)


# -- Per-fixture: result and scores --


@pytest.mark.parametrize(
    "fixture,expected_result,expected_team,expected_opp",
    [
        ("zero_score.json", "loss", 0, 2),
        ("match.json", "win", 5, 4),
        ("forefeit.json", "win", 4, 0),
        ("team_size_2.json", "win", 1, 0),
        ("hoops.json", "win", 5, 2),
    ],
)
def test_match_result_and_scores(
    fixture: str, expected_result: str, expected_team: int, expected_opp: int
):
    conn = ingest_fixture(fixture)
    row = conn.execute(
        "SELECT result, team_score, opponent_score FROM matches"
    ).fetchone()

    assert row == (expected_result, expected_team, expected_opp)


# -- Per-fixture: team MVP --


@pytest.mark.parametrize(
    "fixture,expected_mvp",
    [
        ("zero_score.json", "Jeff"),
        ("match.json", "Jeff"),
        ("forefeit.json", "Drew"),
        ("team_size_2.json", "Drew"),
        ("hoops.json", "Jeff"),
    ],
)
def test_team_mvp(fixture: str, expected_mvp: str):
    conn = ingest_fixture(fixture)
    mvp_name = conn.execute("""
        SELECT p.name
        FROM matches m
        JOIN players p ON p.id = m.team_mvp_player_id
    """).fetchone()[0]

    assert mvp_name == expected_mvp


# -- Per-fixture: player stats (all players, not just tracked) --


@pytest.mark.parametrize(
    "fixture,expected_stats",
    [
        (
            "zero_score.json",
            [
                ("AxDiabetic", 1, 0, 1, 2, 340),
                ("Drew", 0, 0, 0, 2, 182),
                ("Jeff", 0, 0, 2, 2, 340),
                ("Steve", 0, 0, 0, 0, 104),
                ("WizardsNeverrDie", 0, 0, 1, 0, 152),
                ("think_charlie", 1, 1, 1, 3, 383),
            ],
        ),
        (
            "match.json",
            [
                ("BLM_SCAM", 2, 1, 1, 3, 466),
                ("Drew", 2, 0, 0, 3, 420),
                ("Jeff", 2, 2, 0, 2, 448),
                ("Softycooks", 1, 2, 0, 1, 346),
                ("Steve", 1, 1, 0, 2, 208),
                ("stm4000", 1, 0, 2, 3, 362),
            ],
        ),
        (
            "forefeit.json",
            [
                ("Drew", 2, 1, 0, 4, 388),
                ("Jeff", 2, 2, 0, 1, 350),
                ("Steve", 0, 0, 0, 2, 64),
                ("ThatOneFatPanda1", 0, 0, 0, 0, 4),
                ("Turtle.Space", 0, 0, 0, 0, 28),
                ("Yorozuya Kiyo", 0, 0, 2, 0, 168),
            ],
        ),
        (
            "team_size_2.json",
            [
                ("BlurredVision33", 0, 0, 1, 2, 178),
                ("DQRW", 0, 0, 1, 1, 220),
                ("Drew", 0, 1, 2, 4, 425),
                ("Steve", 1, 0, 1, 2, 327),
            ],
        ),
        (
            "hoops.json",
            [
                ("Drew", 2, 1, 0, 5, 511),
                ("Jeff", 3, 1, 1, 5, 696),
                ("REpurk", 1, 1, 0, 4, 326),
                ("temp-**********", 1, 0, 1, 3, 300),
            ],
        ),
    ],
)
def test_player_stats_per_match(
    fixture: str, expected_stats: list[tuple[str, int, int, int, int, int]]
):
    conn = ingest_fixture(fixture)
    rows = conn.execute("""
        SELECT p.name, mp.goals, mp.assists, mp.saves, mp.shots, mp.score
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        ORDER BY p.name
    """).fetchall()

    assert rows == expected_stats


def test_camelcase_match_guid():
    conn = ingest_fixture("camelcase_match_guid.json")
    replay_hash = conn.execute("SELECT replay_hash FROM matches").fetchone()[0]
    assert replay_hash is not None


def test_possession_tracking():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT team_possession_seconds, opponent_possession_seconds FROM matches"
    ).fetchone()
    team_poss, opp_poss = row

    # Possession values should be populated
    assert team_poss is not None
    assert opp_poss is not None
    assert team_poss > 0
    assert opp_poss > 0

    # Both teams had distinct possession (not the same constant)
    assert team_poss != opp_poss

    # Total possession should be in a tighter range for a ~5-minute match
    total_poss = team_poss + opp_poss
    assert 200 < total_poss < 450


def test_possession_none_without_network_data(conn_no_network: sqlite3.Connection):
    row = conn_no_network.execute(
        "SELECT team_possession_seconds, opponent_possession_seconds FROM matches"
    ).fetchone()
    assert row == (None, None)


def test_demolitions_stored_in_match_players():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT p.name, mp.demos
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        ORDER BY p.name
    """).fetchall()

    demo_counts = {name: demos for name, demos in rows}
    assert demo_counts["Drew"] == 1
    assert demo_counts["Jeff"] == 1
    assert demo_counts["Softycooks"] == 2
    assert demo_counts["stm4000"] == 1
    assert demo_counts["Steve"] == 0


def test_demos_received_stored_in_match_players():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT p.name, mp.demos_received
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        ORDER BY p.name
    """).fetchall()

    recv_counts = {name: demos_received for name, demos_received in rows}
    assert recv_counts["Drew"] == 1
    assert recv_counts["Jeff"] == 1
    assert recv_counts["Steve"] == 1
    assert recv_counts["Softycooks"] == 1
    assert recv_counts["stm4000"] == 1
    assert recv_counts["BLM_SCAM"] == 0


def test_ball_zones_tracking():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT defensive_zone_seconds, neutral_zone_seconds, offensive_zone_seconds, duration_seconds FROM matches"
    ).fetchone()
    def_s, neu_s, off_s, duration = row

    assert def_s is not None
    assert neu_s is not None
    assert off_s is not None

    # Ball must have visited all three zones during a full match
    assert def_s > 0
    assert neu_s > 0
    assert off_s > 0

    # Total ball zone time should closely track match duration: the ball is
    # always in some zone during play, and kickoff dead time is excluded via
    # the is_playing gate + dt threshold. A large deviation here indicates
    # the gate is broken (e.g. counting pre/post-match frame time).
    total = def_s + neu_s + off_s
    assert abs(total - duration) / duration < 0.05

    # No single zone should implausibly dominate (>80% of total)
    assert def_s < total * 0.8
    assert neu_s < total * 0.8
    assert off_s < total * 0.8


def test_ball_zones_none_without_network_data(conn_no_network: sqlite3.Connection):
    row = conn_no_network.execute(
        "SELECT defensive_zone_seconds, neutral_zone_seconds, offensive_zone_seconds FROM matches"
    ).fetchone()
    assert row == (None, None, None)


def test_match_events_stored_in_db():
    conn = ingest_fixture("match.json")
    rows = conn.execute(
        "SELECT event_type, game_seconds, team FROM match_events ORDER BY game_seconds"
    ).fetchall()

    assert len(rows) > 0
    goals = [r for r in rows if r[0] == "goal"]
    assert len(goals) == 9


def test_match_events_have_valid_players():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT me.event_type, p.name
        FROM match_events me
        JOIN players p ON me.player_id = p.id
        WHERE me.event_type = 'goal'
        ORDER BY me.game_seconds
    """).fetchall()

    assert len(rows) == 9
    # All goal scorers should have real names
    for _, name in rows:
        assert name != "Unknown"


def test_overtime_goals_positioned_after_regulation():
    replay = load_replay("overtime.json")
    fa = analyze_frames(replay, 0, set(TRACKED_PLAYERS.keys()), 300, "3v3")
    events = fa.match_events

    goals = [e for e in events if e[0] == "goal"]
    assert len(goals) == 3

    # Goals should be in chronological order
    goal_times = [g[1] for g in goals]
    assert goal_times == sorted(goal_times)

    # The overtime goal must be past regulation (>300 game_seconds)
    assert goal_times[-1] > 300


def test_assist_events_in_frame_analysis():
    replay = load_replay("match.json")
    fa = analyze_frames(replay, 0, set(TRACKED_PLAYERS.keys()), 300, "3v3")
    assists = [e for e in fa.match_events if e[0] == "assist"]
    assert len(assists) == 6


def test_offensive_pairings_stored():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT p_scorer.name, p_assister.name, op.game_seconds
        FROM offensive_pairings op
        JOIN players p_scorer ON op.scorer_player_id = p_scorer.id
        JOIN players p_assister ON op.assister_player_id = p_assister.id
        ORDER BY op.game_seconds
    """).fetchall()
    assert len(rows) > 0
    for scorer, assister, _ in rows:
        assert scorer in ("Drew", "Steve", "Jeff")
        assert assister in ("Drew", "Steve", "Jeff")
        assert scorer != assister


def test_offensive_pairings_only_tracked_players():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT scorer_player_id, assister_player_id FROM offensive_pairings
    """).fetchall()
    tracked_ids = {
        r[0]
        for r in conn.execute("SELECT id FROM players WHERE is_tracked = 1").fetchall()
    }
    for scorer_id, assister_id in rows:
        assert scorer_id in tracked_ids
        assert assister_id in tracked_ids


def test_offensive_pairings_idempotent():
    conn = in_memory_db()
    replay = load_replay("match.json")
    analysis = analyze_replay(replay, TRACKED_PLAYERS)
    assert analysis is not None
    write_match(conn, analysis)
    count1 = conn.execute("SELECT COUNT(*) FROM offensive_pairings").fetchone()[0]
    write_match(conn, analysis)
    count2 = conn.execute("SELECT COUNT(*) FROM offensive_pairings").fetchone()[0]
    assert count1 == count2


def _ev(
    event_type: str, game_seconds: float, platform: str, platform_id: str, team: int
) -> tuple[str, float, str, str, int]:
    return (event_type, game_seconds, platform, platform_id, team)


def test_correlate_pairings_basic():
    events = [
        _ev("goal", 10.0, "steam", "A", 0),
        _ev("assist", 9.5, "steam", "B", 0),
    ]
    result = correlate_pairings(events)
    assert result == [
        OffensivePairing(
            scorer=PlayerIdentity("steam", "A"),
            assister=PlayerIdentity("steam", "B"),
            game_seconds=10.0,
            team=0,
        )
    ]


def test_correlate_pairings_nearest_wins():
    events = [
        _ev("goal", 10.0, "steam", "A", 0),
        _ev("assist", 8.5, "steam", "B", 0),  # 1.5s away — outside default window
        _ev("assist", 9.5, "steam", "C", 0),  # 0.5s away — wins
    ]
    result = correlate_pairings(events)
    assert len(result) == 1
    assert result[0].assister == ("steam", "C")


def test_correlate_pairings_no_double_counting():
    events = [
        _ev("goal", 10.0, "steam", "A", 0),
        _ev("goal", 10.1, "steam", "B", 0),
        _ev("assist", 9.9, "steam", "C", 0),  # can only pair with one goal
    ]
    result = correlate_pairings(events)
    assert len(result) == 1


def test_correlate_pairings_outside_window():
    events = [
        _ev("goal", 10.0, "steam", "A", 0),
        _ev("assist", 8.9, "steam", "B", 0),  # 1.1s away — outside 1.0s window
    ]
    assert correlate_pairings(events) == []


def test_correlate_pairings_same_player_excluded():
    events = [
        _ev("goal", 10.0, "steam", "A", 0),
        _ev("assist", 9.8, "steam", "A", 0),  # same player — excluded
    ]
    assert correlate_pairings(events) == []


def test_correlate_pairings_cross_team_excluded():
    events = [
        _ev("goal", 10.0, "steam", "A", 0),
        _ev("assist", 9.8, "steam", "B", 1),  # different team — excluded
    ]
    assert correlate_pairings(events) == []


def test_correlate_pairings_empty():
    assert correlate_pairings([]) == []


def test_boost_stats_tracking():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT team_boost_collected, opponent_boost_collected, team_boost_stolen, opponent_boost_stolen FROM matches"
    ).fetchone()
    assert row == (6276, 8472, 2212, 3468)


def test_boost_stats_none_without_network_data(conn_no_network: sqlite3.Connection):
    row = conn_no_network.execute(
        "SELECT team_boost_collected, opponent_boost_collected, team_boost_stolen, opponent_boost_stolen FROM matches"
    ).fetchone()
    assert row == (None, None, None, None)


def test_player_movement_stats_tracking():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT p.name, mp.boost_per_minute, mp.avg_speed, mp.time_supersonic_pct,
               mp.small_pads, mp.large_pads, mp.stolen_small_pads, mp.stolen_large_pads
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        ORDER BY p.name
    """).fetchall()

    assert len(rows) == 6
    for name, bpm, avg_spd, supersonic, _, _, _, _ in rows:
        assert bpm is not None, f"{name} boost_per_minute is null"
        assert avg_spd is not None, f"{name} avg_speed is null"
        assert supersonic is not None, f"{name} time_supersonic_pct is null"
        assert bpm >= 0, f"{name} boost_per_minute negative"
        assert avg_spd >= 0, f"{name} avg_speed negative"
        assert 0 <= supersonic <= 100, f"{name} supersonic% out of range"

    tracked = [r for r in rows if r[0] in {"Drew", "Jeff", "Steve"}]
    assert len(tracked) == 3

    # Tracked players must have distinct avg_speed values (not all the same constant)
    tracked_speeds = [avg_spd for _, _, avg_spd, *_ in tracked]
    assert len(set(tracked_speeds)) > 1, "All tracked players have identical avg_speed"

    # At least one tracked player went supersonic in a real match
    assert any(supersonic > 0 for _, _, _, supersonic, *_ in tracked), (
        "No tracked player has supersonic time"
    )

    # At least one tracked player collected boost pads
    assert any(small + large > 0 for _, _, _, _, small, large, _, _ in tracked), (
        "No tracked player collected any boost pads"
    )

    # Stolen pads are populated and within bounds
    for name, _, _, _, small, large, stolen_small, stolen_large in tracked:
        assert stolen_small is not None, f"{name} stolen_small_pads is null"
        assert stolen_large is not None, f"{name} stolen_large_pads is null"
        assert stolen_small <= small, f"{name} stolen_small > small"
        assert stolen_large <= large, f"{name} stolen_large > large"

    assert any(
        stolen_small + stolen_large > 0
        for _, _, _, _, _, _, stolen_small, stolen_large in tracked
    ), "No tracked player has stolen pads"


def test_player_movement_stats_none_without_network_data(
    conn_no_network: sqlite3.Connection,
):
    rows = conn_no_network.execute(
        "SELECT boost_per_minute, avg_speed, time_supersonic_pct FROM match_players"
    ).fetchall()
    for row in rows:
        assert row == (None, None, None)


def test_actor_id_recycling_separates_boost_consumption():
    """When a boost component actor ID is recycled for a different player's car,
    each player should only get their own boost consumption attributed."""
    # Minimal replay with two players whose boost component shares actor ID 10.
    # Player A (car 1) uses boost, then actor 10 is deleted and recycled for
    # Player B (car 2) who also uses boost.
    objects = [
        "Archetypes.Car.Car_Default",  # 0 - car archetype
        "Archetypes.Ball.Ball_Default",  # 1 - ball archetype
        "Archetypes.CarComponents.CarComponent_Boost",  # 2 - boost comp archetype
        "TAGame.RBActor_TA:ReplicatedRBState",  # 3 - rigid body
        "TAGame.CarComponent_Boost_TA:ReplicatedBoost",  # 4 - boost amount
        "TAGame.CarComponent_TA:Vehicle",  # 5 - component->car link
        "Engine.Pawn:PlayerReplicationInfo",  # 6 - car->PRI link
        "Engine.PlayerReplicationInfo:UniqueId",  # 7 - PRI->identity
        "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam",  # 8
        "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",  # 9
        "TAGame.VehiclePickup_TA:NewReplicatedPickupData",  # 10
        "TAGame.Car_TA:TeamPaint",  # 11
    ]
    frames = [
        # Frame 0: Countdown finishes -> play begins
        {
            "time": 0.0,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [
                {"actor_id": 200, "object_id": 9, "attribute": {"Int": 0}},
            ],
            "deleted_actors": [],
        },
        # Frame 1: Create car 1 (player A) and car 2 (player B)
        {
            "time": 0.01,
            "delta": 0.033,
            "new_actors": [
                {"actor_id": 1, "object_id": 0},  # car 1
                {"actor_id": 2, "object_id": 0},  # car 2
            ],
            "updated_actors": [],
            "deleted_actors": [],
        },
        # Frame 1: Set up PRI and identity for both cars
        {
            "time": 0.033,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [
                # car 1 -> PRI 101
                {
                    "actor_id": 1,
                    "object_id": 6,
                    "attribute": {"ActiveActor": {"actor": 101}},
                },
                # car 2 -> PRI 102
                {
                    "actor_id": 2,
                    "object_id": 6,
                    "attribute": {"ActiveActor": {"actor": 102}},
                },
                # PRI 101 = player A (steam AAA)
                {
                    "actor_id": 101,
                    "object_id": 7,
                    "attribute": {"UniqueId": {"remote_id": {"Steam": "AAA"}}},
                },
                # PRI 102 = player B (steam BBB)
                {
                    "actor_id": 102,
                    "object_id": 7,
                    "attribute": {"UniqueId": {"remote_id": {"Steam": "BBB"}}},
                },
            ],
            "deleted_actors": [],
        },
        # Frame 2: Create boost component 10, link to car 1 (player A)
        {
            "time": 0.066,
            "delta": 0.033,
            "new_actors": [{"actor_id": 10, "object_id": 2}],
            "updated_actors": [
                {
                    "actor_id": 10,
                    "object_id": 5,
                    "attribute": {"ActiveActor": {"actor": 1}},
                },
                # Initial boost = 85 (~33%)
                {
                    "actor_id": 10,
                    "object_id": 4,
                    "attribute": {"ReplicatedBoost": {"boost_amount": 85}},
                },
            ],
            "deleted_actors": [],
        },
        # Frame 3: Player A uses some boost (85 -> 50 = 35 consumed)
        {
            "time": 0.1,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [
                {
                    "actor_id": 10,
                    "object_id": 4,
                    "attribute": {"ReplicatedBoost": {"boost_amount": 50}},
                },
            ],
            "deleted_actors": [],
        },
        # Frame 4: Delete boost component 10 (goal scored, etc.)
        {
            "time": 0.133,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [],
            "deleted_actors": [10],
        },
        # Frame 5: Recycle actor ID 10 as boost component for car 2 (player B)
        {
            "time": 0.166,
            "delta": 0.033,
            "new_actors": [{"actor_id": 10, "object_id": 2}],
            "updated_actors": [
                {
                    "actor_id": 10,
                    "object_id": 5,
                    "attribute": {"ActiveActor": {"actor": 2}},
                },
                {
                    "actor_id": 10,
                    "object_id": 4,
                    "attribute": {"ReplicatedBoost": {"boost_amount": 100}},
                },
            ],
            "deleted_actors": [],
        },
        # Frame 6: Player B uses boost (100 -> 20 = 80 consumed)
        {
            "time": 0.2,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [
                {
                    "actor_id": 10,
                    "object_id": 4,
                    "attribute": {"ReplicatedBoost": {"boost_amount": 20}},
                },
            ],
            "deleted_actors": [],
        },
    ]

    replay = cast(
        ReplayJSON, {"objects": objects, "network_frames": {"frames": frames}}
    )
    fa = analyze_frames(
        replay, tracked_team=0, tracked_identities=set(), duration=300, game_mode="3v3"
    )
    stats = fa.movement_stats

    player_a = stats.get(("steam", "AAA"))
    player_b = stats.get(("steam", "BBB"))
    assert player_a is not None, "Player A missing from stats"
    assert player_b is not None, "Player B missing from stats"

    # Player A consumed 35 units, Player B consumed 80 units (0-255 scale).
    # Without the recycling fix, Player B would get 35+80=115.
    a_bpm = player_a["boost_per_minute"]
    b_bpm = player_b["boost_per_minute"]
    assert b_bpm > a_bpm, (
        f"Player B ({b_bpm}) should have higher boost/min than A ({a_bpm})"
    )


def test_boost_attributed_when_car_and_boost_comp_deleted_same_frame():
    """When a car and its boost component are both in deleted_actors for the same frame,
    with the car listed first, boost consumed before deletion must still be attributed."""
    objects = [
        "Archetypes.Car.Car_Default",  # 0 - car archetype
        "Archetypes.Ball.Ball_Default",  # 1 - ball archetype
        "Archetypes.CarComponents.CarComponent_Boost",  # 2 - boost comp archetype
        "TAGame.RBActor_TA:ReplicatedRBState",  # 3 - rigid body
        "TAGame.CarComponent_Boost_TA:ReplicatedBoost",  # 4 - boost amount
        "TAGame.CarComponent_TA:Vehicle",  # 5 - component->car link
        "Engine.Pawn:PlayerReplicationInfo",  # 6 - car->PRI link
        "Engine.PlayerReplicationInfo:UniqueId",  # 7 - PRI->identity
        "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",  # 8 - countdown
        "TAGame.VehiclePickup_TA:NewReplicatedPickupData",  # 9 - boost pads (required by movement handler)
    ]
    frames = [
        # Frame 0: Create car 1, link to PRI 3, set identity, start play (countdown=0)
        {
            "time": 0.0,
            "delta": 0.033,
            "new_actors": [
                {
                    "actor_id": 1,
                    "object_id": 0,
                },  # car (car archetype → added to car_actors)
            ],
            "updated_actors": [
                {
                    "actor_id": 1,
                    "object_id": 6,
                    "attribute": {"ActiveActor": {"actor": 3}},
                },  # car 1 -> PRI actor 3
                {
                    "actor_id": 3,
                    "object_id": 7,
                    "attribute": {"UniqueId": {"remote_id": {"Steam": "AAA"}}},
                },  # PRI 3 -> identity
                {
                    "actor_id": 200,
                    "object_id": 8,
                    "attribute": {"Int": 0},
                },  # countdown = 0 → is_playing = True
            ],
            "deleted_actors": [],
        },
        # Frame 1: Create boost component 10, link to car 1
        {
            "time": 0.033,
            "delta": 0.033,
            "new_actors": [{"actor_id": 10, "object_id": 2}],
            "updated_actors": [
                {
                    "actor_id": 10,
                    "object_id": 5,
                    "attribute": {"ActiveActor": {"actor": 1}},
                },  # boost comp -> car 1
                {
                    "actor_id": 10,
                    "object_id": 4,
                    "attribute": {"ReplicatedBoost": {"boost_amount": 85}},
                },  # initial boost
            ],
            "deleted_actors": [],
        },
        # Frame 2: Player uses boost (85 -> 50 = 35 consumed)
        {
            "time": 0.066,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [
                {
                    "actor_id": 10,
                    "object_id": 4,
                    "attribute": {"ReplicatedBoost": {"boost_amount": 50}},
                },
            ],
            "deleted_actors": [],
        },
        # Frame 3: Car and boost component deleted in same frame, car listed first
        {
            "time": 0.1,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [],
            "deleted_actors": [1, 10],
        },
    ]

    replay = cast(
        ReplayJSON, {"objects": objects, "network_frames": {"frames": frames}}
    )
    fa = analyze_frames(
        replay, tracked_team=0, tracked_identities=set(), duration=300, game_mode="3v3"
    )
    stats = fa.movement_stats

    player_a = stats.get(("steam", "AAA"))
    assert player_a is not None, "Player A missing from stats"
    assert player_a["boost_per_minute"] > 0, (
        "boost should be attributed even when car and boost comp deleted in same frame"
    )


def test_demos_received_when_demolish_and_deletion_same_frame():
    """ReplicatedDemolishExtended update and victim car deletion in the same frame must
    still count as a demo, even though deleted_actors is processed before updated_actors."""
    objects = [
        "Archetypes.Car.Car_Default",  # 0 - car archetype
        "Engine.Pawn:PlayerReplicationInfo",  # 1 - car->PRI link
        "Engine.PlayerReplicationInfo:UniqueId",  # 2 - PRI->identity
        "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",  # 3 - countdown
        "TAGame.Car_TA:ReplicatedDemolishExtended",  # 4 - demo notification
    ]
    frames = [
        # Frame 0: Create victim car 1 + attacker car 2, wire identities, start play
        {
            "time": 0.0,
            "delta": 0.033,
            "new_actors": [
                {"actor_id": 1, "object_id": 0},  # victim car
                {"actor_id": 2, "object_id": 0},  # attacker car
            ],
            "updated_actors": [
                {
                    "actor_id": 1,
                    "object_id": 1,
                    "attribute": {"ActiveActor": {"actor": 10}},
                },  # victim car -> PRI 10
                {
                    "actor_id": 10,
                    "object_id": 2,
                    "attribute": {"UniqueId": {"remote_id": {"Steam": "VICTIM"}}},
                },
                {
                    "actor_id": 2,
                    "object_id": 1,
                    "attribute": {"ActiveActor": {"actor": 20}},
                },  # attacker car -> PRI 20
                {
                    "actor_id": 20,
                    "object_id": 2,
                    "attribute": {"UniqueId": {"remote_id": {"Steam": "ATTACKER"}}},
                },
                {
                    "actor_id": 200,
                    "object_id": 3,
                    "attribute": {"Int": 0},
                },  # countdown=0 → is_playing=True
            ],
            "deleted_actors": [],
        },
        # Frame 1: demolish update on attacker car 2 AND victim car 1 deleted in same frame
        {
            "time": 0.033,
            "delta": 0.033,
            "new_actors": [],
            "updated_actors": [
                {
                    "actor_id": 2,
                    "object_id": 4,
                    "attribute": {
                        "DemolishExtended": {
                            "self_demolish": False,
                            "victim": {"active": True, "actor": 1},
                            "attacker": {"active": True, "actor": 2},
                        }
                    },
                },
            ],
            "deleted_actors": [1],
        },
    ]

    replay = cast(
        ReplayJSON, {"objects": objects, "network_frames": {"frames": frames}}
    )
    fa = analyze_frames(
        replay, tracked_team=0, tracked_identities=set(), duration=300, game_mode="3v3"
    )

    assert fa.demos_received.get(("steam", "VICTIM")) == 1, (
        "demo should be counted even when victim car is deleted in the same frame as the demolish notification"
    )


def test_player_name_updates_on_change():
    conn = in_memory_db()
    get_or_create_player(conn, "steam", "123", "OldName", False)
    row = conn.execute("SELECT name FROM players WHERE platform_id = '123'").fetchone()
    assert row[0] == "OldName"

    get_or_create_player(conn, "steam", "123", "NewName", False)
    row = conn.execute("SELECT name FROM players WHERE platform_id = '123'").fetchone()
    assert row[0] == "NewName"


# -- played_at from MatchStartEpoch --


def test_played_at_derived_from_match_start_epoch():
    analysis = analyze_replay(load_replay("match.json"), TRACKED_PLAYERS)
    assert analysis is not None
    assert analysis.played_at_sql == "2026-02-08 23:27:57"


def test_analyze_replay_rejects_when_no_date_source_available():
    replay = copy.deepcopy(load_replay("match.json"))
    del cast(ReplayProperties, replay.get("properties"))["MatchStartEpoch"]
    replay["debug_info"] = []
    assert analyze_replay(replay, TRACKED_PLAYERS) is None


def _replay_with_bakkesmod_time(game_start_time: str) -> ReplayJSON:
    replay = copy.deepcopy(load_replay("match.json"))
    del cast(ReplayProperties, replay.get("properties"))["MatchStartEpoch"]
    replay["debug_info"] = [
        {"frame": 0, "user": "GameStartTime", "text": game_start_time}
    ]
    return replay


def test_played_at_falls_back_to_bakkesmod_game_start_time():
    analysis = analyze_replay(
        _replay_with_bakkesmod_time("2024-08-10T02:37:59-0400"), TRACKED_PLAYERS
    )
    assert analysis is not None
    assert analysis.played_at_sql == "2024-08-10 06:37:59"


def test_match_start_epoch_takes_precedence_over_bakkesmod():
    replay = copy.deepcopy(load_replay("match.json"))
    replay["debug_info"] = [
        {"frame": 0, "user": "GameStartTime", "text": "2020-01-01T00:00:00+0000"}
    ]
    analysis = analyze_replay(replay, TRACKED_PLAYERS)
    assert analysis is not None
    assert analysis.played_at_sql == "2026-02-08 23:27:57"


# -- validate_replay --


def test_validate_replay_passes():
    assert validate_replay(load_replay("match.json"), TRACKED_PLAYERS) is None


def test_validate_replay_missing_guid():
    assert (
        validate_replay({"properties": {}}, TRACKED_PLAYERS) == SkipReason.NO_MATCH_GUID
    )


def test_validate_replay_missing_date():
    replay: ReplayJSON = {"properties": {"MatchGUID": "x"}}
    assert validate_replay(replay, TRACKED_PLAYERS) == SkipReason.MISSING_DATE


def test_validate_replay_no_tracked_players():
    replay: ReplayJSON = {
        "properties": {
            "MatchGUID": "x",
            "MatchStartEpoch": 1000000,
            "PlayerStats": [],
            "Team0Score": 2,
            "Team1Score": 1,
        }
    }
    assert validate_replay(replay, TRACKED_PLAYERS) == SkipReason.NO_TRACKED_PLAYERS


def _fetch_player(
    conn: sqlite3.Connection, identity: PlayerIdentity
) -> tuple[str, int] | None:
    row = conn.execute(
        "SELECT name, is_tracked FROM players WHERE platform=? AND platform_id=?",
        (identity.platform, identity.platform_id),
    ).fetchone()
    return (row[0], row[1]) if row else None


def test_sync_tracked_players_creates_missing_player():
    conn = in_memory_db()
    identity = PlayerIdentity(platform="steam", platform_id="999")
    sync_tracked_players(conn, {identity: "NewPlayer"})
    assert _fetch_player(conn, identity) == ("NewPlayer", 1)


def test_sync_tracked_players_removes_stale_tracked_player():
    conn = in_memory_db()
    identity = PlayerIdentity(platform="steam", platform_id="999")
    sync_tracked_players(conn, {identity: "NewPlayer"})
    sync_tracked_players(conn, {})
    assert _fetch_player(conn, identity) == ("NewPlayer", 0)


def test_sync_tracked_players_promotes_untracked_player():
    conn = in_memory_db()
    identity = PlayerIdentity(platform="steam", platform_id="999")
    conn.execute(
        "INSERT INTO players (platform, platform_id, name, is_tracked) VALUES (?, ?, ?, 0)",
        (identity.platform, identity.platform_id, "OldName"),
    )
    sync_tracked_players(conn, {identity: "NewName"})
    assert _fetch_player(conn, identity) == ("NewName", 1)
