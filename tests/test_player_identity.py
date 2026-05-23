import pytest

from player_identity import PlayerIdentity, from_network_frame, from_player_stats
from rrrocket_schema import PlayerStatEntry

# --- from_player_stats ---


@pytest.mark.parametrize(
    "player, expected",
    [
        (
            {
                "Platform": {"value": "OnlinePlatform_Steam"},
                "OnlineID": "76561197969365901",
            },
            PlayerIdentity("steam", "76561197969365901"),
        ),
        (
            {
                "Platform": {"value": "OnlinePlatform_Epic"},
                "OnlineID": "0",
                "PlayerID": {
                    "fields": {"EpicAccountId": "23ce79e90944478599a96ed5402a99e6"}
                },
            },
            PlayerIdentity("epic", "23ce79e90944478599a96ed5402a99e6"),
        ),
        (
            {"Platform": {"value": "OnlinePlatform_PS4"}, "OnlineID": "987654321"},
            PlayerIdentity("ps4", "987654321"),
        ),
    ],
)
def test_from_player_stats_known_platforms(
    player: PlayerStatEntry, expected: PlayerIdentity
):
    assert from_player_stats(player) == expected


def test_from_player_stats_unknown_platform_returns_none():
    assert (
        from_player_stats(
            {"Platform": {"value": "OnlinePlatform_Unknown"}, "OnlineID": "123"}
        )
        is None
    )


def test_from_player_stats_steam_zero_online_id_returns_none():
    assert (
        from_player_stats(
            {"Platform": {"value": "OnlinePlatform_Steam"}, "OnlineID": "0"}
        )
        is None
    )


def test_from_player_stats_epic_missing_account_id_returns_none():
    player: PlayerStatEntry = {
        "Platform": {"value": "OnlinePlatform_Epic"},
        "OnlineID": "0",
        "PlayerID": {"fields": {"EpicAccountId": ""}},
    }
    assert from_player_stats(player) is None


# --- from_network_frame ---


@pytest.mark.parametrize(
    "uid, expected",
    [
        (
            {"remote_id": {"Steam": "76561197969365901"}},
            PlayerIdentity("steam", "76561197969365901"),
        ),
        (
            {"remote_id": {"Epic": "23ce79e90944478599a96ed5402a99e6"}},
            PlayerIdentity("epic", "23ce79e90944478599a96ed5402a99e6"),
        ),
        (
            {"remote_id": {"PlayStation": "987654321"}},
            PlayerIdentity("ps4", "987654321"),
        ),
        (
            {
                "remote_id": {
                    "Steam": {"online_id": "76561197969365901", "extra": "ignored"}
                }
            },
            PlayerIdentity("steam", "76561197969365901"),
        ),
    ],
)
def test_from_network_frame_known_platforms(
    uid: dict[str, object], expected: PlayerIdentity
):
    assert from_network_frame(uid) == expected


def test_from_network_frame_unknown_platform_returns_none():
    assert from_network_frame({"remote_id": {"Splitgate": "123"}}) is None


def test_from_network_frame_empty_remote_id_returns_none():
    assert from_network_frame({"remote_id": {}}) is None


def test_from_network_frame_missing_remote_id_returns_none():
    assert from_network_frame({}) is None


# --- Join invariant: both paths must agree for the same player ---
# Player data drawn from tests/data/match.json to verify end-to-end agreement.


def test_join_invariant_steam_player():
    player_stats: PlayerStatEntry = {
        "Platform": {"value": "OnlinePlatform_Steam"},
        "OnlineID": "76561197969365901",
    }
    network_uid = {"remote_id": {"Steam": "76561197969365901"}}
    assert from_player_stats(player_stats) == from_network_frame(network_uid)


def test_join_invariant_epic_player():
    player_stats: PlayerStatEntry = {
        "Platform": {"value": "OnlinePlatform_Epic"},
        "OnlineID": "0",
        "PlayerID": {"fields": {"EpicAccountId": "23ce79e90944478599a96ed5402a99e6"}},
    }
    network_uid = {"remote_id": {"Epic": "23ce79e90944478599a96ed5402a99e6"}}
    assert from_player_stats(player_stats) == from_network_frame(network_uid)
