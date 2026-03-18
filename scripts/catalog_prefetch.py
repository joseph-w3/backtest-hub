from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
import os
from pathlib import Path
from queue import Empty
from queue import Full
from queue import Queue
import re
import threading
from typing import Protocol


PARQUET_WINDOW_RE = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{9}Z)"
    r"_(?P<end>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{9}Z)\.parquet$"
)


def _iso_filename_to_ns(value: str) -> int:
    date_part, time_part = value[:-1].split("T", 1)
    hour, minute, second, nanos = time_part.split("-")
    base = datetime.fromisoformat(
        f"{date_part}T{hour}:{minute}:{second}+00:00"
    )
    return int(base.timestamp()) * 1_000_000_000 + int(nanos)


def parse_parquet_window_ns(path: str | Path) -> tuple[int | None, int | None]:
    candidate = Path(path)
    match = PARQUET_WINDOW_RE.fullmatch(candidate.name)
    if match is None:
        return None, None
    return _iso_filename_to_ns(match.group("start")), _iso_filename_to_ns(match.group("end"))


def ns_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    seconds, nanos = divmod(int(value), 1_000_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def time_like_to_ns(value: datetime | int | float | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        timestamp = value.timestamp()
        return int(timestamp * 1_000_000_000)
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return None
    normalized = value.replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(normalized).timestamp()
    return int(timestamp * 1_000_000_000)


@dataclass(frozen=True)
class CatalogWindowedFile:
    path: str
    start_ns: int | None
    end_ns: int | None
    data_type: str | None
    instrument_id: str | None


def build_windowed_files(file_paths: list[str]) -> list[CatalogWindowedFile]:
    records: list[CatalogWindowedFile] = []
    for raw_path in file_paths:
        path = Path(raw_path)
        start_ns, end_ns = parse_parquet_window_ns(path)
        data_type = path.parent.parent.name if path.parent.parent != path.parent else None
        instrument_id = path.parent.name if path.parent != path else None
        records.append(
            CatalogWindowedFile(
                path=str(path),
                start_ns=start_ns,
                end_ns=end_ns,
                data_type=data_type,
                instrument_id=instrument_id,
            )
        )
    records.sort(
        key=lambda record: (
            record.start_ns if record.start_ns is not None else -1,
            record.path,
        )
    )
    return records


class PrefetchBackend(Protocol):
    name: str

    def prefetch(self, paths: list[str]) -> dict:
        ...


class NoopPrefetchBackend:
    name = "disabled"

    def prefetch(self, paths: list[str]) -> dict:
        return {
            "prefetched_files": 0,
            "prefetched_bytes": 0,
            "skipped_files": len(paths),
        }


class LocalFileReadPrefetchBackend:
    name = "local-read"

    def __init__(self, *, read_chunk_bytes: int = 4 * 1024 * 1024) -> None:
        self._read_chunk_bytes = read_chunk_bytes

    def prefetch(self, paths: list[str]) -> dict:
        prefetched_files = 0
        prefetched_bytes = 0
        skipped_files = 0
        for raw_path in paths:
            path = Path(raw_path)
            if not path.is_file():
                skipped_files += 1
                continue
            with path.open("rb") as handle:
                while True:
                    chunk = handle.read(self._read_chunk_bytes)
                    if not chunk:
                        break
                    prefetched_bytes += len(chunk)
            prefetched_files += 1
        return {
            "prefetched_files": prefetched_files,
            "prefetched_bytes": prefetched_bytes,
            "skipped_files": skipped_files,
        }


def build_prefetch_backend(mode: str | None = None) -> PrefetchBackend:
    if mode is None:
        mode = os.environ.get("BACKTEST_PREFETCH_BACKEND", "local-read")
    mode = mode.strip().lower()
    if mode in {"", "off", "disabled", "none"}:
        return NoopPrefetchBackend()
    if mode == "local-read":
        return LocalFileReadPrefetchBackend()
    raise ValueError(
        "Unsupported BACKTEST_PREFETCH_BACKEND: "
        f"{mode}. Expected one of: local-read, off."
    )


class ReplayPrefetchController:
    def __init__(
        self,
        *,
        files: list[CatalogWindowedFile],
        backend: PrefetchBackend,
        ahead_hours: int,
        max_files_per_batch: int,
    ) -> None:
        self._files = files
        self._backend = backend
        self._ahead_ns = max(0, ahead_hours) * 3_600 * 1_000_000_000
        self._max_files_per_batch = max(1, max_files_per_batch)
        self._completed_paths: set[str] = set()
        self._pending_paths: set[str] = set()
        self._pending_queue: Queue[list[str] | None] = Queue(maxsize=1)
        self._result_queue: Queue[dict] = Queue()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._state = {
            "stage": "disabled" if not self.enabled else "idle",
            "backend": backend.name,
            "ahead_hours": ahead_hours,
            "max_files_per_batch": self._max_files_per_batch,
            "cursor_time": None,
            "window_end_time": None,
            "pending_files": 0,
            "prefetched_files": 0,
            "requested_files_total": 0,
            "completed_files_total": 0,
            "last_batch_files": None,
            "last_batch_bytes": None,
            "last_error": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="catalog-prefetch",
            daemon=True,
        )
        self._worker.start()

    @property
    def enabled(self) -> bool:
        return self._backend.name != "disabled" and self._ahead_ns > 0 and bool(self._files)

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                batch = self._pending_queue.get(timeout=0.2)
            except Empty:
                continue
            if batch is None:
                break
            try:
                result = self._backend.prefetch(batch)
                self._result_queue.put(
                    {
                        "paths": batch,
                        "result": result,
                        "error": None,
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive path
                self._result_queue.put(
                    {
                        "paths": batch,
                        "result": None,
                        "error": str(exc),
                    }
                )

    def _drain_results(self) -> None:
        drained = False
        while True:
            try:
                item = self._result_queue.get_nowait()
            except Empty:
                break
            drained = True
            paths = item["paths"]
            with self._lock:
                for path in paths:
                    self._pending_paths.discard(path)
                    if item["error"] is None:
                        self._completed_paths.add(path)
                self._state["pending_files"] = len(self._pending_paths)
                self._state["prefetched_files"] = len(self._completed_paths)
                self._state["last_batch_files"] = len(paths)
                if item["error"] is None:
                    result = item["result"] or {}
                    self._state["completed_files_total"] = int(
                        self._state["completed_files_total"]
                    ) + int(result.get("prefetched_files", 0))
                    self._state["last_batch_bytes"] = int(result.get("prefetched_bytes", 0))
                    self._state["stage"] = "completed"
                    self._state["last_error"] = None
                else:
                    self._state["stage"] = "error"
                    self._state["last_error"] = item["error"]
                self._state["updated_at"] = datetime.now(timezone.utc).isoformat()
        if drained:
            return

    def snapshot(self) -> dict:
        self._drain_results()
        with self._lock:
            return dict(self._state)

    def advance(self, cursor_ns: int | None) -> dict:
        self._drain_results()
        with self._lock:
            self._state["cursor_time"] = ns_to_iso(cursor_ns)
            self._state["window_end_time"] = ns_to_iso(
                cursor_ns + self._ahead_ns if cursor_ns is not None else None
            )
            self._state["updated_at"] = datetime.now(timezone.utc).isoformat()
            if not self.enabled or cursor_ns is None:
                self._state["stage"] = "disabled" if not self.enabled else "idle"
                return dict(self._state)

            window_end_ns = cursor_ns + self._ahead_ns
            batch: list[str] = []
            for record in self._files:
                if record.path in self._completed_paths or record.path in self._pending_paths:
                    continue
                if record.end_ns is not None and record.end_ns < cursor_ns:
                    continue
                if record.start_ns is not None and record.start_ns > window_end_ns:
                    continue
                batch.append(record.path)
                if len(batch) >= self._max_files_per_batch:
                    break

            if not batch:
                if self._state["pending_files"]:
                    self._state["stage"] = "inflight"
                else:
                    self._state["stage"] = "idle"
                return dict(self._state)

            try:
                self._pending_queue.put_nowait(batch)
            except Full:
                self._state["stage"] = "inflight"
                return dict(self._state)

            self._pending_paths.update(batch)
            self._state["pending_files"] = len(self._pending_paths)
            self._state["requested_files_total"] = int(self._state["requested_files_total"]) + len(batch)
            self._state["last_batch_files"] = len(batch)
            self._state["stage"] = "queued"
            self._state["last_error"] = None
            self._state["updated_at"] = datetime.now(timezone.utc).isoformat()
            return dict(self._state)

    def close(self) -> None:
        self._drain_results()
        self._stop_event.set()
        try:
            self._pending_queue.put_nowait(None)
        except Full:
            pass
        self._worker.join(timeout=0.1)
        self._drain_results()
