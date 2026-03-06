import pytest
from ingest import ingest_match, get_or_create_player, TRACKED_PLAYERS
from frame_analysis import _extract_demolitions, _extract_match_events, _extract_boost_stats, _extract_player_movement_stats
from tests.fixtures import in_memory_db, load_replay, cached_db

ALL_FIXTURES = ["zero_score.json", "match.json", "forefeit.json", "team_size_2.json", "hoops.json"]


def ingest_fixture(fixture):
    return cached_db(fixture)


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
def test_match_result_and_scores(fixture, expected_result, expected_team, expected_opp):
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
def test_team_mvp(fixture, expected_mvp):
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
def test_player_stats_per_match(fixture, expected_stats):
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


def test_epic_player_stored_with_correct_platform():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT platform, platform_id FROM players WHERE name = 'stm4000'"
    ).fetchone()
    assert row == ("epic", "23ce79e90944478599a96ed5402a99e6")


def test_ps4_player_stored_with_correct_platform():
    conn = ingest_fixture("zero_score.json")
    row = conn.execute(
        "SELECT platform, platform_id FROM players WHERE name = 'think_charlie'"
    ).fetchone()
    assert row == ("ps4", "8532790116262235057")


def test_steam_player_stored_with_correct_platform():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT platform, platform_id FROM players WHERE name = 'Drew'"
    ).fetchone()
    assert row == ("steam", "76561197969365901")


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

    # Total possession should be in a reasonable range
    # (exceeds TotalSecondsPlayed due to goal replays/countdowns in frame times)
    total_poss = team_poss + opp_poss
    assert 60 < total_poss < 600


def test_possession_none_without_network_data():
    """Replays without network_frames should have null possession."""
    conn = in_memory_db()
    replay = load_replay("match.json")
    # Strip network data (copy to avoid mutating cached replay)
    replay = {k: v for k, v in replay.items() if k not in ("network_frames", "objects")}
    ingest_match(conn, replay)
    row = conn.execute(
        "SELECT team_possession_seconds, opponent_possession_seconds FROM matches"
    ).fetchone()
    assert row == (None, None)


def test_extract_demolitions():
    replay = load_replay("match.json")
    demos = _extract_demolitions(replay)

    # Should return a dict with (platform, platform_id) keys
    assert isinstance(demos, dict)
    assert len(demos) > 0

    # Known players from match.json with demos
    # Jeff (Steam:76561197964215253) = 1 demo
    # Drew (Steam:76561197969365901) = 1 demo
    assert demos[("steam", "76561197964215253")] == 1  # Jeff
    assert demos[("steam", "76561197969365901")] == 1  # Drew


def test_extract_demolitions_without_network_data():
    demos = _extract_demolitions({"properties": {}})
    assert demos == {}


def test_demolitions_stored_in_match_players():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT p.name, mp.demos
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        WHERE mp.demos > 0
        ORDER BY p.name
    """).fetchall()

    # Drew and Jeff each have 1 demo; stm4000 has 1; BLM_SCAM has 2
    names = [r[0] for r in rows]
    assert "Drew" in names
    assert "Jeff" in names


def test_ball_thirds_tracking():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT defensive_third_seconds, neutral_third_seconds, offensive_third_seconds FROM matches"
    ).fetchone()
    def_s, neu_s, off_s = row

    assert def_s is not None
    assert neu_s is not None
    assert off_s is not None
    assert def_s >= 0
    assert neu_s >= 0
    assert off_s >= 0

    # Total should be in a reasonable range for a match
    total = def_s + neu_s + off_s
    assert 60 < total < 600


def test_ball_thirds_none_without_network_data():
    """Replays without network_frames should have null ball thirds."""
    conn = in_memory_db()
    replay = load_replay("match.json")
    replay = {k: v for k, v in replay.items() if k not in ("network_frames", "objects")}
    ingest_match(conn, replay)
    row = conn.execute(
        "SELECT defensive_third_seconds, neutral_third_seconds, offensive_third_seconds FROM matches"
    ).fetchone()
    assert row == (None, None, None)


def test_extract_match_events():
    replay = load_replay("match.json")
    # tracked_team is 0 for match.json (Drew/Steve/Jeff are team 0)
    events = _extract_match_events(replay, 0, set(TRACKED_PLAYERS.keys()))

    assert isinstance(events, list)
    assert len(events) > 0

    # Each event is (event_type, game_seconds, platform, platform_id, team)
    for ev in events:
        assert ev[0] in ("goal", "shot", "save", "demo")
        assert isinstance(ev[1], (int, float))
        assert ev[1] >= 0
        assert ev[4] in (0, 1)

    # Count goals — should match the 5-4 score (9 total)
    goals = [e for e in events if e[0] == "goal"]
    assert len(goals) == 9

    team_goals = [e for e in goals if e[4] == 0]
    opp_goals = [e for e in goals if e[4] == 1]
    assert len(team_goals) == 5
    assert len(opp_goals) == 4


def test_extract_match_events_without_network_data():
    events = _extract_match_events({"properties": {}}, 0, set(TRACKED_PLAYERS.keys()))
    assert events == []


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
    events = _extract_match_events(replay, 0, set(TRACKED_PLAYERS.keys()))

    goals = [e for e in events if e[0] == "goal"]
    assert len(goals) == 3

    # Goals should be in chronological order
    goal_times = [g[1] for g in goals]
    assert goal_times == sorted(goal_times)

    # The overtime goal must be past regulation (>300 game_seconds)
    assert goal_times[-1] > 300


def test_boost_stats_tracking():
    conn = ingest_fixture("match.json")
    row = conn.execute(
        "SELECT team_boost_collected, opponent_boost_collected, team_boost_stolen, opponent_boost_stolen FROM matches"
    ).fetchone()
    team_collected, opp_collected, team_stolen, opp_stolen = row

    assert row == (6276, 8472, 2212, 3468)
    assert team_collected is not None
    assert opp_collected is not None
    assert team_stolen is not None
    assert opp_stolen is not None
    assert team_collected > 0
    assert opp_collected > 0
    assert team_stolen >= 0
    assert opp_stolen >= 0
    assert team_stolen <= team_collected
    assert opp_stolen <= opp_collected


def test_boost_stats_none_without_network_data():
    conn = in_memory_db()
    replay = load_replay("match.json")
    replay = {k: v for k, v in replay.items() if k not in ("network_frames", "objects")}
    ingest_match(conn, replay)
    row = conn.execute(
        "SELECT team_boost_collected, opponent_boost_collected, team_boost_stolen, opponent_boost_stolen FROM matches"
    ).fetchone()
    assert row == (None, None, None, None)


def test_boost_stats_dedupes_repeated_pickup_state():
    replay = {
        "objects": [
            "TAGame.Car_TA:TeamPaint",
            "TAGame.RBActor_TA:ReplicatedRBState",
            "TAGame.VehiclePickup_TA:NewReplicatedPickupData",
        ],
        "network_frames": {
            "frames": [
                {
                    "time": 0.0,
                    "updated_actors": [
                        {
                            "actor_id": 1,
                            "object_id": 0,
                            "attribute": {"TeamPaint": {"team": 0}},
                        },
                        {
                            "actor_id": 1,
                            "object_id": 1,
                            "attribute": {"RigidBody": {"location": {"x": 0, "y": 100}}},
                        },
                        {
                            "actor_id": 10,
                            "object_id": 2,
                            "attribute": {"PickupNew": {"picked_up": 1, "instigator": 1}},
                        },
                    ],
                },
                {
                    "time": 0.1,
                    "updated_actors": [
                        {
                            "actor_id": 10,
                            "object_id": 2,
                            "attribute": {"PickupNew": {"picked_up": 1, "instigator": 1}},
                        }
                    ],
                },
                {
                    "time": 0.2,
                    "updated_actors": [
                        {
                            "actor_id": 10,
                            "object_id": 2,
                            "attribute": {"PickupNew": {"picked_up": 3, "instigator": 1}},
                        }
                    ],
                },
            ]
        },
    }

    assert _extract_boost_stats(replay, tracked_team=0, game_mode="3v3") == (
        24,
        0,
        24,
        0,
    )


def test_player_movement_stats_tracking():
    conn = ingest_fixture("match.json")
    rows = conn.execute("""
        SELECT p.name, mp.boost_per_minute, mp.avg_speed, mp.time_supersonic_pct
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        ORDER BY p.name
    """).fetchall()

    assert len(rows) == 6
    for name, bpm, avg_spd, supersonic in rows:
        assert bpm is not None, f"{name} boost_per_minute is null"
        assert avg_spd is not None, f"{name} avg_speed is null"
        assert supersonic is not None, f"{name} time_supersonic_pct is null"
        assert bpm >= 0, f"{name} boost_per_minute negative"
        assert avg_spd >= 0, f"{name} avg_speed negative"
        assert 0 <= supersonic <= 100, f"{name} supersonic% out of range"


def test_player_movement_stats_none_without_network_data():
    conn = in_memory_db()
    replay = load_replay("match.json")
    replay = {k: v for k, v in replay.items() if k not in ("network_frames", "objects")}
    ingest_match(conn, replay)
    rows = conn.execute(
        "SELECT boost_per_minute, avg_speed, time_supersonic_pct FROM match_players"
    ).fetchall()
    for row in rows:
        assert row == (None, None, None)


def test_extract_player_movement_stats():
    replay = load_replay("match.json")
    duration = replay["properties"].get("TotalSecondsPlayed")
    stats = _extract_player_movement_stats(replay, duration)

    assert isinstance(stats, dict)
    assert len(stats) > 0

    # Known players should be present
    drew_key = ("steam", "76561197969365901")
    jeff_key = ("steam", "76561197964215253")
    assert drew_key in stats
    assert jeff_key in stats

    for identity, s in stats.items():
        assert "boost_per_minute" in s
        assert "avg_speed" in s
        assert "time_supersonic_pct" in s
        assert s["boost_per_minute"] >= 0
        assert s["avg_speed"] >= 0
        assert 0 <= s["time_supersonic_pct"] <= 100
        assert "small_pads" in s
        assert "large_pads" in s
        assert "stolen_small_pads" in s
        assert "stolen_large_pads" in s


def test_actor_id_recycling_separates_boost_consumption():
    """When a boost component actor ID is recycled for a different player's car,
    each player should only get their own boost consumption attributed."""
    # Minimal replay with two players whose boost component shares actor ID 10.
    # Player A (car 1) uses boost, then actor 10 is deleted and recycled for
    # Player B (car 2) who also uses boost.
    objects = [
        "Archetypes.Car.Car_Default",         # 0 - car archetype
        "Archetypes.Ball.Ball_Default",        # 1 - ball archetype
        "Archetypes.CarComponents.CarComponent_Boost",  # 2 - boost comp archetype
        "TAGame.RBActor_TA:ReplicatedRBState", # 3 - rigid body
        "TAGame.CarComponent_Boost_TA:ReplicatedBoost", # 4 - boost amount
        "TAGame.CarComponent_TA:Vehicle",      # 5 - component->car link
        "Engine.Pawn:PlayerReplicationInfo",   # 6 - car->PRI link
        "Engine.PlayerReplicationInfo:UniqueId", # 7 - PRI->identity
        "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam",  # 8
        "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",  # 9
        "TAGame.VehiclePickup_TA:NewReplicatedPickupData",  # 10
        "TAGame.Car_TA:TeamPaint",             # 11
    ]
    frames = [
        # Frame 0: Countdown finishes -> play begins
        {"time": 0.0, "delta": 0.033,
         "new_actors": [],
         "updated_actors": [
             {"actor_id": 200, "object_id": 9,
              "attribute": {"Int": 0}},
         ],
         "deleted_actors": []},
        # Frame 1: Create car 1 (player A) and car 2 (player B)
        {"time": 0.01, "delta": 0.033,
         "new_actors": [
             {"actor_id": 1, "object_id": 0},  # car 1
             {"actor_id": 2, "object_id": 0},  # car 2
         ],
         "updated_actors": [], "deleted_actors": []},
        # Frame 1: Set up PRI and identity for both cars
        {"time": 0.033, "delta": 0.033,
         "new_actors": [],
         "updated_actors": [
             # car 1 -> PRI 101
             {"actor_id": 1, "object_id": 6,
              "attribute": {"ActiveActor": {"actor": 101}}},
             # car 2 -> PRI 102
             {"actor_id": 2, "object_id": 6,
              "attribute": {"ActiveActor": {"actor": 102}}},
             # PRI 101 = player A (steam AAA)
             {"actor_id": 101, "object_id": 7,
              "attribute": {"UniqueId": {"remote_id": {"Steam": "AAA"}}}},
             # PRI 102 = player B (steam BBB)
             {"actor_id": 102, "object_id": 7,
              "attribute": {"UniqueId": {"remote_id": {"Steam": "BBB"}}}},
         ],
         "deleted_actors": []},
        # Frame 2: Create boost component 10, link to car 1 (player A)
        {"time": 0.066, "delta": 0.033,
         "new_actors": [{"actor_id": 10, "object_id": 2}],
         "updated_actors": [
             {"actor_id": 10, "object_id": 5,
              "attribute": {"ActiveActor": {"actor": 1}}},
             # Initial boost = 85 (~33%)
             {"actor_id": 10, "object_id": 4,
              "attribute": {"ReplicatedBoost": {"boost_amount": 85}}},
         ],
         "deleted_actors": []},
        # Frame 3: Player A uses some boost (85 -> 50 = 35 consumed)
        {"time": 0.1, "delta": 0.033,
         "new_actors": [],
         "updated_actors": [
             {"actor_id": 10, "object_id": 4,
              "attribute": {"ReplicatedBoost": {"boost_amount": 50}}},
         ],
         "deleted_actors": []},
        # Frame 4: Delete boost component 10 (goal scored, etc.)
        {"time": 0.133, "delta": 0.033,
         "new_actors": [], "updated_actors": [],
         "deleted_actors": [10]},
        # Frame 5: Recycle actor ID 10 as boost component for car 2 (player B)
        {"time": 0.166, "delta": 0.033,
         "new_actors": [{"actor_id": 10, "object_id": 2}],
         "updated_actors": [
             {"actor_id": 10, "object_id": 5,
              "attribute": {"ActiveActor": {"actor": 2}}},
             {"actor_id": 10, "object_id": 4,
              "attribute": {"ReplicatedBoost": {"boost_amount": 100}}},
         ],
         "deleted_actors": []},
        # Frame 6: Player B uses boost (100 -> 20 = 80 consumed)
        {"time": 0.2, "delta": 0.033,
         "new_actors": [],
         "updated_actors": [
             {"actor_id": 10, "object_id": 4,
              "attribute": {"ReplicatedBoost": {"boost_amount": 20}}},
         ],
         "deleted_actors": []},
    ]

    replay = {
        "objects": objects,
        "network_frames": {"frames": frames},
    }
    stats = _extract_player_movement_stats(replay, duration=300)

    player_a = stats.get(("steam", "AAA"))
    player_b = stats.get(("steam", "BBB"))
    assert player_a is not None, "Player A missing from stats"
    assert player_b is not None, "Player B missing from stats"

    # Player A consumed 35 units, Player B consumed 80 units (0-255 scale).
    # Without the recycling fix, Player B would get 35+80=115.
    a_bpm = player_a["boost_per_minute"]
    b_bpm = player_b["boost_per_minute"]
    assert b_bpm > a_bpm, f"Player B ({b_bpm}) should have higher boost/min than A ({a_bpm})"


def test_extract_player_pad_stats():
    replay = load_replay("match.json")
    duration = replay["properties"].get("TotalSecondsPlayed")
    stats = _extract_player_movement_stats(replay, duration, game_mode="standard")

    assert len(stats) > 0

    for identity, s in stats.items():
        small = s["small_pads"]
        large = s["large_pads"]
        stolen_small = s["stolen_small_pads"]
        stolen_large = s["stolen_large_pads"]
        assert small >= 0
        assert large >= 0
        assert stolen_small >= 0
        assert stolen_large >= 0
        assert stolen_small <= small, f"stolen_small ({stolen_small}) > small ({small})"
        assert stolen_large <= large, f"stolen_large ({stolen_large}) > large ({large})"

    # At least some players should have collected pads
    total_pads = sum(s["small_pads"] + s["large_pads"] for s in stats.values())
    assert total_pads > 0, "Expected at least some pad pickups"


def test_player_name_updates_on_change():
    conn = in_memory_db()
    get_or_create_player(conn, "steam", "123", "OldName")
    row = conn.execute("SELECT name FROM players WHERE platform_id = '123'").fetchone()
    assert row[0] == "OldName"

    get_or_create_player(conn, "steam", "123", "NewName")
    row = conn.execute("SELECT name FROM players WHERE platform_id = '123'").fetchone()
    assert row[0] == "NewName"
