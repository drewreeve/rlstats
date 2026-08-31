"""Tests for rrrocket_schema — chiefly the NetObj drift guard.

NetObj's values are rrrocket wire-format object names; the frame walks resolve
them against ParsedReplay.object_index. If a value here stops matching what
rrrocket emits, every dependent handler silently goes dark (its create() gets
None and returns None). This test pins the exact string set.
"""

from rrrocket_schema import NetObj

# The exact wire-format names, spelled out once independently of the enum body.
_EXPECTED = {
    "TAGame.Ball_TA:HitTeamNum",
    "Archetypes.Ball.Ball_Default",
    "Archetypes.Car.Car_Default",
    "Archetypes.CarComponents.CarComponent_Boost",
    "TAGame.RBActor_TA:ReplicatedRBState",
    "TAGame.CarComponent_Boost_TA:ReplicatedBoost",
    "TAGame.CarComponent_TA:Vehicle",
    "Engine.Pawn:PlayerReplicationInfo",
    "Engine.PlayerReplicationInfo:UniqueId",
    "TAGame.GameEvent_Soccar_TA:ReplicatedScoredOnTeam",
    "TAGame.GameEvent_TA:ReplicatedRoundCountDownNumber",
    "TAGame.VehiclePickup_TA:NewReplicatedPickupData",
    "TAGame.Car_TA:TeamPaint",
    "TAGame.PRI_TA:MatchDemolishes",
    "TAGame.Car_TA:ReplicatedDemolishExtended",
    "TAGame.GameEvent_Soccar_TA:SecondsRemaining",
    "Engine.PlayerReplicationInfo:Team",
    "TAGame.PRI_TA:MatchGoals",
    "TAGame.PRI_TA:MatchShots",
    "TAGame.PRI_TA:MatchSaves",
    "TAGame.PRI_TA:MatchAssists",
}


def test_netobj_values_have_not_drifted() -> None:
    assert {m.value for m in NetObj} == _EXPECTED


def test_netobj_members_are_plain_strings() -> None:
    # StrEnum members must compare equal to the raw string so that
    # object_index.get(NetObj.X) matches a dict keyed by the literal.
    assert isinstance(NetObj.RB_STATE, str)
    assert NetObj.RB_STATE == "TAGame.RBActor_TA:ReplicatedRBState"
    # frame_analysis will call obj_index.get(NetObj.X) on a dict keyed by the
    # raw rrrocket names (as the handler tests build it).
    assert {"Archetypes.Car.Car_Default": 7}.get(NetObj.CAR_ARCHETYPE) == 7
