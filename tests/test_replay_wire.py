"""Drift guard for the replay-viewer wire contract — see CONTEXT.md "Replay Wire".

Modelled on ``tests/test_stats_registry.py``: run a real parsed replay through
``extract_replay_frames``, serialize it exactly as the ``/api/matches/{id}/replay``
route does (``jsonable_encoder(frames.meta_dict())``), and assert every emitted
key set and packed-row width matches the hand-written ``WIRE_*`` manifest in
``replay_frames``. A renamed ``ActorSlot`` field or a changed tuple width fails
here — fast, no browser.

The manifest is stated independently of the dataclasses on purpose; this file is
what binds the two, in both directions.
"""

from dataclasses import fields
from typing import Any

from fastapi.encoders import jsonable_encoder

from replay_frames import (
    WIRE_GOAL_FIELDS,
    WIRE_META_KEYS,
    WIRE_SLOT_FIELDS,
    WIRE_TUPLE_WIDTHS,
    ActorSlot,
    GoalMarker,
    ReplayFrames,
    packed_buffer_bytes,
)
from tests.fixtures import replay_frames_of


def _wire(name: str = "match.json") -> tuple[ReplayFrames, Any]:
    """A real ``ReplayFrames`` and its serialized meta — the exact pair the
    route and ``tests/e2e/dump_fixture`` produce."""
    frames = replay_frames_of(name)
    return frames, jsonable_encoder(frames.meta_dict())


# --- the manifest vs the dataclasses (fixture-free, both directions) ---


def test_manifest_mirrors_the_dataclass_fields_exactly() -> None:
    # The emitted-shape checks below need a fixture that exercises every slot
    # kind; this pins the manifest to the dataclass with no fixture at all, so a
    # new field can't reach the wire unlisted even if no test replay hits it.
    assert WIRE_SLOT_FIELDS == {f.name for f in fields(ActorSlot)}
    assert WIRE_GOAL_FIELDS == {f.name for f in fields(GoalMarker)}


# --- the manifest vs the serialized wire ---


def test_meta_top_level_keys_match_the_manifest() -> None:
    frames, meta = _wire()
    assert set(frames.meta_dict()) == WIRE_META_KEYS
    assert set(meta) == WIRE_META_KEYS


def test_every_slot_has_exactly_the_manifest_fields() -> None:
    _, meta = _wire()
    slots = meta["slots"]
    assert slots
    assert {s["kind"] for s in slots} == {"car", "ball"}
    for s in slots:
        assert set(s) == WIRE_SLOT_FIELDS
        assert s["identity"] is None or (
            isinstance(s["identity"], list) and len(s["identity"]) == 2
        )
        assert all(len(seg) == WIRE_TUPLE_WIDTHS["segments"] for seg in s["segments"])


def test_every_goal_has_exactly_the_manifest_fields() -> None:
    _, meta = _wire()
    assert meta["goals"]
    for g in meta["goals"]:
        assert set(g) == WIRE_GOAL_FIELDS


def test_packed_positional_rows_have_the_manifest_widths() -> None:
    _, meta = _wire()
    for key in ("countdowns", "dead_periods", "boost_pads"):
        rows = meta[key]
        assert rows, f"{key} empty in the fixture — pick a replay that exercises it"
        assert all(len(row) == WIRE_TUPLE_WIDTHS[key] for row in rows)


def test_bin_length_is_packed_buffer_bytes_of_the_meta_dimensions() -> None:
    frames, meta = _wire()
    assert len(frames.positions) == packed_buffer_bytes(
        len(meta["frame_times"]), len(meta["slots"])
    )


# --- the no-network-data replay still emits the full shape ---


def test_empty_replay_frames_still_carries_every_meta_key() -> None:
    empty = ReplayFrames(
        frame_times=[], slots=[], positions=b"", tracked_team=None, game_mode=None
    )
    meta = jsonable_encoder(empty.meta_dict())
    assert set(meta) == WIRE_META_KEYS
    assert meta["frame_times"] == []
    assert meta["slots"] == []
    assert meta["goals"] == meta["countdowns"] == meta["dead_periods"] == []
    assert meta["boost_pads"] == []
    assert empty.positions == b""
    assert packed_buffer_bytes(0, 0) == 0
