from pathlib import Path

import pytest

from file_outcome import (
    Skipped,
    SkipReason,
    Written,
    decode,
    encode,
    sentinel_path,
)

# -- sentinel_path --


def test_sentinel_path_appends_ingested_suffix():
    assert sentinel_path(Path("/x/abc.replay")) == Path("/x/abc.replay.ingested")


# -- encode --


def test_encode_written():
    assert encode(Written()) == "written"


def test_encode_skipped_uses_reason_value():
    assert (
        encode(Skipped(SkipReason.NO_TRACKED_PLAYERS)) == "skipped:no_tracked_players"
    )


def test_encode_skipped_without_reason():
    assert encode(Skipped(None)) == "skipped:"


# -- decode --


def test_decode_written():
    assert decode("written") == Written()


def test_decode_empty_is_written():
    """Older sentinels were empty touch-files; they mean written."""
    assert decode("") == Written()
    assert decode("  \n") == Written()


def test_decode_skipped_with_known_reason():
    assert decode("skipped:missing_date") == Skipped(SkipReason.MISSING_DATE)


def test_decode_skipped_with_unknown_reason_is_lenient():
    """A reason a newer writer used that this reader doesn't know decodes to
    Skipped(None) rather than raising."""
    assert decode("skipped:teleported_away") == Skipped(None)


def test_decode_strips_surrounding_whitespace():
    assert decode(" written\n") == Written()
    assert decode("skipped:missing_date\n") == Skipped(SkipReason.MISSING_DATE)


# -- round trip --


@pytest.mark.parametrize(
    "outcome",
    [
        Written(),
        Skipped(SkipReason.NO_MATCH_GUID),
        Skipped(SkipReason.MISSING_DATE),
        Skipped(SkipReason.NO_TRACKED_PLAYERS),
        Skipped(None),
    ],
)
def test_encode_decode_round_trip(outcome: Written | Skipped):
    assert decode(encode(outcome)) == outcome
