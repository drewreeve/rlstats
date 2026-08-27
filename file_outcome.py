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
is committed.

See CONTEXT.md: "File Outcome".
"""

from dataclasses import dataclass
from enum import Enum


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
