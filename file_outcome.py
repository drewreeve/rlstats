"""File Outcome — the terminal record of what the ingest pipeline did with one
replay file.

Three variants:
  - ``Written``          the replay was analyzed and its match row written
  - ``Skipped(reason)``  the replay was valid but not ingested (see ``SkipReason``)
  - ``Failed(message)``  the replay could not be parsed, or its DB write failed

This is what the ``.replay.ingested`` sentinel records and what
``UploadProcessor.status()`` reports. It is deliberately payload-free:
``ingest.Analyzed`` carries the heavy ``ReplayAnalysis`` through the pipeline
until the outcome is known, and is mapped to ``Written`` only once the match row
is committed. ``encode``/``decode`` are the sole owners of the sentinel's
on-disk string format.

See CONTEXT.md: "File Outcome".
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class SkipReason(Enum):
    NO_MATCH_GUID = "no_match_guid"
    MISSING_DATE = "missing_date"
    NO_TRACKED_PLAYERS = "no_tracked_players"

    @property
    def message(self) -> str:
        return {
            SkipReason.NO_MATCH_GUID: "Replay has no match GUID",
            SkipReason.MISSING_DATE: "Replay has no match date",
            SkipReason.NO_TRACKED_PLAYERS: "No tracked players in this replay",
        }[self]


@dataclass(frozen=True)
class Written:
    pass


@dataclass(frozen=True)
class Skipped:
    reason: SkipReason | None


@dataclass(frozen=True)
class Failed:
    message: str


FileOutcome = Written | Skipped | Failed


SENTINEL_SUFFIX = ".ingested"
_SKIPPED_PREFIX = "skipped:"


def sentinel_path(replay_path: Path) -> Path:
    """The ``.ingested`` sentinel path that sits beside a replay file."""
    return replay_path.with_suffix(replay_path.suffix + SENTINEL_SUFFIX)


def encode(outcome: Written | Skipped) -> str:
    """Serialize a terminal outcome for the ``.ingested`` sentinel.

    ``Failed`` outcomes get no sentinel — they retry on the next run — so they
    are not encodable.
    """
    if isinstance(outcome, Skipped):
        reason = outcome.reason.value if outcome.reason is not None else ""
        return f"{_SKIPPED_PREFIX}{reason}"
    return "written"


def decode(text: str) -> Written | Skipped:
    """Parse a ``.ingested`` sentinel's content back into a terminal outcome.

    Empty content is an older sentinel written before the written/skipped
    distinction existed; it means ``Written``. A skip reason this reader does
    not recognize decodes to ``Skipped(None)`` rather than raising, so a newer
    writer can never break an older reader.
    """
    content = text.strip()
    if content.startswith(_SKIPPED_PREFIX):
        raw = content[len(_SKIPPED_PREFIX) :]
        try:
            return Skipped(SkipReason(raw))
        except ValueError:
            return Skipped(None)
    return Written()
