"""TypedDict definitions for the rrrocket JSON output schema.

rrrocket is the external binary that parses .replay files into JSON. Both
ingest.py and frame_analysis.py consume this output; this module is the single
place to update when the rrrocket format changes.

The `attribute` field on UpdatedActor is intentionally left as dict[str, Any]:
it's a discriminated union keyed on rrrocket attribute type names ("Byte",
"Int", "RigidBody", etc.) dispatched at runtime by object_id, and is too
polymorphic to type exhaustively without a full discriminated union.

Call `parse(raw)` to convert a raw `ReplayJSON` dict into a `ParsedReplay`
dataclass. All downstream consumers (ingest.py, frame_analysis.py) accept
`ParsedReplay`; `ReplayJSON` is only used at the boundary where rrrocket JSON
is first read.
"""

from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict


class PlayerStatEntry(TypedDict, total=False):
    Name: str
    Platform: dict[str, str]  # {"value": "OnlinePlatform_Steam"} etc.
    OnlineID: str
    PlayerID: dict[str, Any]  # {"fields": {"EpicAccountId": "..."}} for Epic
    Team: int
    Score: int
    Goals: int
    Assists: int
    Saves: int
    Shots: int
    bBot: bool


class DebugInfoEntry(TypedDict):
    frame: int
    user: str
    text: str


class ReplayProperties(TypedDict, total=False):
    MatchGUID: str
    MatchGuid: str  # alternate casing present in some replays
    MatchStartEpoch: int
    PlayerStats: list[PlayerStatEntry]
    Team0Score: int
    Team1Score: int
    TotalSecondsPlayed: int
    bForfeit: bool
    TeamSize: int
    MapName: str


class NewActor(TypedDict):
    actor_id: int
    object_id: NotRequired[int]


class UpdatedActor(TypedDict):
    actor_id: int
    object_id: NotRequired[int]
    attribute: NotRequired[dict[str, Any]]


class FrameData(TypedDict):
    time: float
    delta: NotRequired[float]
    new_actors: NotRequired[list[NewActor]]
    updated_actors: NotRequired[list[UpdatedActor]]
    deleted_actors: NotRequired[list[int]]


class NetworkFrames(TypedDict, total=False):
    frames: list[FrameData]


class ReplayJSON(TypedDict, total=False):
    properties: ReplayProperties
    network_frames: NetworkFrames
    debug_info: list[DebugInfoEntry]
    objects: list[str]


@dataclass(frozen=True)
class ParsedReplay:
    match_guid: str | None
    properties: ReplayProperties
    objects: list[str]
    frames: list[FrameData]
    debug_info: list[DebugInfoEntry]


def parse(raw: ReplayJSON) -> ParsedReplay:
    props: ReplayProperties = raw.get("properties") or {}
    return ParsedReplay(
        match_guid=props.get("MatchGUID") or props.get("MatchGuid"),
        properties=props,
        objects=raw.get("objects") or [],
        frames=(raw.get("network_frames") or {}).get("frames") or [],
        debug_info=raw.get("debug_info") or [],
    )
