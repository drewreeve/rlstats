import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from file_outcome import Failed, FileOutcome, Skipped, reconcile, sentinel_path
from player_identity import PlayerIdentity
from process import open_write_conn, parallel_parse, write_parsed_batch

logger = logging.getLogger(__name__)


class UploadState(Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class UploadStatus:
    """Reconciled status for one uploaded file: folds together the .ingested
    sentinel, this processor's in-memory pipeline state, and file existence
    into the single answer a caller needs."""

    state: UploadState
    stage: str | None = None
    batch: tuple[int, int] | None = None
    error: str | None = None
    reason: str | None = None


class _BatchProgress:
    """Mutable (completed, total) counter shared by every file in one flush's
    batch. Scoped per-batch (not processor-wide) so two overlapping flushes —
    possible because Timer.cancel() doesn't interrupt an in-flight flush() —
    can't clobber each other's displayed progress."""

    __slots__ = ("completed", "total")

    def __init__(self, total: int) -> None:
        self.completed = 0
        self.total = total


@dataclass(frozen=True)
class Queued:
    pass


@dataclass(frozen=True)
class Processing:
    batch: _BatchProgress


@dataclass(frozen=True)
class Parsed:
    batch: _BatchProgress


@dataclass(frozen=True)
class Errored:
    message: str
    recorded_at: float


FileRecord = Queued | Processing | Parsed | Errored


class _UploadFiles:
    """Per-file pipeline state for one UploadProcessor: queued -> processing
    -> parsed -> resolved (removed), or -> errored (kept until it expires).

    Not thread-safe on its own — the owning UploadProcessor serializes all
    access via its own lock, the same lock that also guards the upload queue
    and timer, so a flush's "drain queue, mark files processing" step stays
    one atomic unit.
    """

    def __init__(self) -> None:
        self._records: dict[str, FileRecord] = {}

    def get(self, name: str) -> FileRecord | None:
        return self._records.get(name)

    def mark_queued(self, name: str) -> None:
        self._records[name] = Queued()

    def mark_processing(self, name: str, batch: _BatchProgress) -> None:
        self._records[name] = Processing(batch)

    def mark_parsed(self, name: str, batch: _BatchProgress) -> None:
        if name in self._records:
            self._records[name] = Parsed(batch)

    def record_error(self, name: str, message: str) -> None:
        self._records[name] = Errored(message, time.monotonic())

    def resolve(self, name: str) -> None:
        self._records.pop(name, None)

    def prune_stale(self, cutoff: float) -> None:
        stale = [
            name
            for name, record in self._records.items()
            if isinstance(record, Errored) and record.recorded_at < cutoff
        ]
        for name in stale:
            self._records.pop(name, None)


def _status_from_outcome(outcome: FileOutcome) -> UploadStatus:
    """Map a terminal file_outcome variant to the endpoint's UploadStatus."""
    if isinstance(outcome, Skipped):
        reason = outcome.reason
        return UploadStatus(
            UploadState.SKIPPED,
            reason=reason.message if reason is not None else None,
        )
    if isinstance(outcome, Failed):
        return UploadStatus(UploadState.ERROR, error=outcome.message or None)
    return UploadStatus(UploadState.PROCESSED)


class UploadProcessor:
    """Debounced batch processor for uploaded replay files."""

    def __init__(
        self,
        db_path: str | Path,
        tracked_players: dict[PlayerIdentity, str],
        replay_dir: str | Path,
        delay: float = 2.0,
        error_retention: float = 1800.0,
    ):
        self.db_path = db_path
        self.tracked_players = tracked_players
        self.replay_dir = Path(replay_dir)
        self.delay = delay
        self.error_retention = error_retention
        self._queue: list[Path] = []
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        # Progress state for the server's /api/upload/status endpoint. Resolved
        # (processed or skipped) files are removed once flush ends; the .ingested
        # sentinel is the durable record from then on. Errored records are kept
        # for reporting, but expire after error_retention so abandoned uploads
        # don't accumulate.
        self._files = _UploadFiles()

    def enqueue(self, path: Path):
        with self._lock:
            self._files.prune_stale(time.monotonic() - self.error_retention)
            self._queue.append(path)
            self._files.mark_queued(path.name)
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        """Arm (or reset) the debounce timer. Internal seam: tests that want
        enqueue() to stay synchronous-only can no-op this instead of reaching
        in to cancel a real threading.Timer by hand."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.delay, self.flush)
            self._timer.daemon = True
            self._timer.start()

    def file_status(self, name: str) -> str | None:
        with self._lock:
            record = self._files.get(name)
        if record is None:
            return None
        if isinstance(record, Queued):
            return "queued"
        if isinstance(record, Processing):
            return "processing"
        if isinstance(record, Parsed):
            return "parsed"
        return f"error:{record.message}"

    def batch_progress(self, name: str) -> tuple[int, int] | None:
        with self._lock:
            record = self._files.get(name)
            batch = record.batch if isinstance(record, Processing | Parsed) else None
            return None if batch is None else (batch.completed, batch.total)

    def status(self, name: str) -> UploadStatus:
        """One answer for the /api/upload/status endpoint.

        file_outcome.reconcile() folds the two durable recorded signals (the
        sentinel, a recorded processing error) into a terminal outcome. What's
        left is this processor's own to answer: in-flight pipeline stage and
        batch position, and a replay file that has vanished off disk.
        """
        replay_path = self.replay_dir / name
        try:
            sentinel_text = sentinel_path(replay_path).read_text()
        except FileNotFoundError:
            sentinel_text = None

        # The sentinel, when present, is the whole answer — skip the lock.
        stage = None if sentinel_text is not None else self.file_status(name)
        recorded_error = (
            stage[len("error:") :]
            if stage is not None and stage.startswith("error:")
            else None
        )

        outcome = reconcile(sentinel_text, recorded_error)
        if outcome is not None:
            return _status_from_outcome(outcome)

        if stage is not None:  # in flight: queued / processing / parsed
            return UploadStatus(
                UploadState.PENDING, stage=stage, batch=self.batch_progress(name)
            )
        if not replay_path.exists():
            return UploadStatus(UploadState.ERROR)
        return UploadStatus(UploadState.PENDING)

    def _on_parsed(self, path: Path, batch: _BatchProgress) -> None:
        with self._lock:
            self._files.mark_parsed(path.name, batch)
            batch.completed += 1

    def flush(self) -> None:
        with self._lock:
            files = list(self._queue)
            self._queue.clear()
            self._timer = None
            batch = _BatchProgress(total=len(files)) if files else None
            if files and batch is not None:
                for path in files:
                    self._files.mark_processing(path.name, batch)
        if not files or batch is None:
            return
        logger.info("Processing %d uploaded replay(s)", len(files))
        results = parallel_parse(
            files, self.tracked_players, on_parsed=lambda p: self._on_parsed(p, batch)
        )
        conn = open_write_conn(self.db_path)
        try:
            outcomes = write_parsed_batch(conn, self.tracked_players, results)
        finally:
            conn.close()
        with self._lock:
            for name, outcome in outcomes.items():
                if isinstance(outcome, Failed):
                    self._files.record_error(name, outcome.message)
                else:
                    self._files.resolve(name)
