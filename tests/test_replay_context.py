"""Tests for ingest.build_replay_context.

The single owner of the pre-frame-analysis preamble (bot-filtered player stats,
match perspective, game-mode detection, the identity -> display-name map, and
the match-present tracked identities) shared by ingest.analyze_replay and
replay_view.build_replay_frames. See CONTEXT.md: "Replay Context".
"""

from ingest import ReplayContext, build_replay_context
from player_identity import PlayerIdentity
from rrrocket_schema import parse as parse_replay
from tests.fixtures import TRACKED_PLAYERS, load_replay


def _ctx(name: str) -> ReplayContext:
    return build_replay_context(parse_replay(load_replay(name)), TRACKED_PLAYERS)


def test_perspective_is_resolved_for_the_tracked_team() -> None:
    # match.json: tracked trio on team 1, which won 5-4; Jeff top-scored.
    p = _ctx("match.json").perspective
    assert p.team == 1
    assert (p.team_score, p.opponent_score) == (5, 4)
    assert p.result == "win"
    assert p.mvp_identity == PlayerIdentity("steam", "76561197964215253")


def test_detects_game_mode_per_replay() -> None:
    assert _ctx("match.json").game_mode == "3v3"
    assert _ctx("team_size_2.json").game_mode == "2v2"
    assert _ctx("hoops.json").game_mode == "hoops"


def test_player_stats_is_bot_filtered_and_identity_keyed() -> None:
    ctx = _ctx("match.json")
    assert ctx.player_stats
    assert all(isinstance(k, PlayerIdentity) for k in ctx.player_stats)
    assert len(ctx.player_stats) == 6  # six non-bot players


def test_tracked_identities_is_the_match_present_subset() -> None:
    ctx = _ctx("team_size_2.json")
    assert isinstance(ctx.tracked_identities, frozenset)
    assert ctx.tracked_identities <= set(ctx.player_stats)
    assert ctx.tracked_identities <= set(TRACKED_PLAYERS)
    # Drew and Steve play this 2v2; Jeff (also tracked) does not.
    assert len(ctx.tracked_identities) == 2


def test_names_prefer_the_configured_display_name_for_tracked_players() -> None:
    ctx = _ctx("team_size_2.json")
    assert {ctx.player_names[i] for i in ctx.tracked_identities} == {"Drew", "Steve"}


def test_names_fall_back_to_the_in_game_name_for_untracked_players() -> None:
    ctx = _ctx("team_size_2.json")
    untracked = set(ctx.player_stats) - ctx.tracked_identities
    assert {ctx.player_names[i] for i in untracked} == {"BlurredVision33", "DQRW"}


def test_names_cover_every_player_with_a_non_empty_string() -> None:
    ctx = _ctx("match.json")
    assert set(ctx.player_names) == set(ctx.player_stats)
    assert all(ctx.player_names.values())
