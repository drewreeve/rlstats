"""Tests for replay_view's HTTP-facing serialization: encode_replay_meta and
serialize_replay_envelope — the merged-endpoint wire format documented in
docs/adr/0004-browser-replay-viewer-design.md's addendum.
"""

from replay_view import encode_replay_meta, serialize_replay_envelope
from tests.fixtures import replay_frames_of, unpack_replay_envelope


def test_envelope_sections_round_trip_with_no_trailing_bytes() -> None:
    frames = replay_frames_of("match.json")
    envelope = serialize_replay_envelope(frames)

    # unpack_replay_envelope itself asserts the header's lengths account for
    # every byte in the envelope (no truncation, no trailing garbage); here we
    # additionally check each section's *content* matches what was encoded.
    positions, boost, meta = unpack_replay_envelope(envelope)
    assert positions == frames.positions
    assert boost == frames.boost
    assert meta == encode_replay_meta(frames)
