from __future__ import annotations

import json
import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


LOGGER = logging.getLogger("backtest_hub.run_store")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_dumps(value: dict[str, Any] | None) -> str | None:
    if not value:
        return None
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


@dataclass(frozen=True)
class RunRow:
    backtest_id: str
    entry: dict[str, Any]


class SqliteRunStore:
    """
    Store for backtest run mapping/state.

    Design goals:
    - Avoid full-file read/write contention of run_mapping.json
    - Provide point lookups and range queries efficiently
    - Keep unknown fields in extra_json for forward-compatibility
    """

    _COLUMNS = [
        "backtest_id",
        "status",
        "created_at",
        "queued_at",
        "submitted_at",
        "cancelled_at",
        "requested_by",
        "required_memory_gb",
        "backtest_docker_run_id",
        "backtest_api_base",
        "last_error",
        "last_error_at",
        "extra_json",
    ]

    _KNOWN_FIELDS = set(_COLUMNS) - {"backtest_id", "extra_json"}

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self.ensure_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=3000")
        return conn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    backtest_id TEXT PRIMARY KEY,
                    status TEXT,
                    created_at TEXT,
                    queued_at TEXT,
                    submitted_at TEXT,
                    cancelled_at TEXT,
                    requested_by TEXT,
                    required_memory_gb REAL,
                    backtest_docker_run_id TEXT,
                    backtest_api_base TEXT,
                    last_error TEXT,
                    last_error_at TEXT,
                    extra_json TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_submitted_at ON runs(submitted_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_backtest_api_base ON runs(backtest_api_base)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (
                    backtest_id TEXT PRIMARY KEY,
                    queued_at TEXT NOT NULL,
                    queue_position INTEGER NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_position ON queue(queue_position)")

    # ---- Queue operations ------------------------------------------------

    def enqueue(self, backtest_id: str, queued_at: str) -> None:
        """Append a task to the end of the queue (idempotent on backtest_id)."""
        with self._write_lock:
            with self._connect() as conn:
                # Determine the next position (max + 1, or 0 for empty queue).
                row = conn.execute("SELECT COALESCE(MAX(queue_position), -1) AS mx FROM queue").fetchone()
                next_pos = int(row["mx"]) + 1 if row else 0
                conn.execute(
                    """
                    INSERT INTO queue (backtest_id, queued_at, queue_position)
                    VALUES (?, ?, ?)
                    ON CONFLICT(backtest_id) DO UPDATE SET queued_at = excluded.queued_at
                    """,
                    (backtest_id, queued_at, next_pos),
                )

    def dequeue(self, backtest_id: str) -> None:
        """Remove a specific task from the queue."""
        with self._write_lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM queue WHERE backtest_id = ?", (backtest_id,))

    def read_queue(self) -> list[dict[str, str]]:
        """Return all queued items ordered by queue_position (FIFO)."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT backtest_id, queued_at FROM queue ORDER BY queue_position ASC"
            ).fetchall()
        return [{"backtest_id": r["backtest_id"], "queued_at": r["queued_at"]} for r in rows]

    def queue_length(self) -> int:
        """Return the number of items in the queue."""
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(1) AS n FROM queue").fetchone()
        return int(row["n"]) if row else 0

    def queue_position(self, backtest_id: str) -> int | None:
        """Return the 0-based index of *backtest_id* in the queue, or None."""
        items = self.read_queue()
        for idx, item in enumerate(items):
            if item["backtest_id"] == backtest_id:
                return idx
        return None

    def remove_from_queue_batch(self, backtest_ids: list[str]) -> set[str]:
        """Remove multiple items from the queue. Returns the set of ids that were actually removed."""
        wanted = {bid for bid in backtest_ids if isinstance(bid, str) and bid}
        if not wanted:
            return set()
        with self._write_lock:
            with self._connect() as conn:
                placeholders = ",".join(["?"] * len(wanted))
                # Find which ones actually exist in the queue.
                existing = conn.execute(
                    f"SELECT backtest_id FROM queue WHERE backtest_id IN ({placeholders})",
                    tuple(wanted),
                ).fetchall()
                removed = {r["backtest_id"] for r in existing}
                if removed:
                    del_ph = ",".join(["?"] * len(removed))
                    conn.execute(
                        f"DELETE FROM queue WHERE backtest_id IN ({del_ph})",
                        tuple(removed),
                    )
        return removed

    # ---- Run operations ---------------------------------------------------

    def count_runs(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(1) AS n FROM runs").fetchone()
        return int(row["n"]) if row else 0

    def get_run(self, backtest_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE backtest_id = ?",
                (backtest_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_entry(row)

    def get_runs_by_ids(self, backtest_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = [x for x in backtest_ids if isinstance(x, str) and x]
        if not ids:
            return {}
        placeholders = ",".join(["?"] * len(ids))
        sql = f"SELECT * FROM runs WHERE backtest_id IN ({placeholders})"
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(ids)).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            bid = row["backtest_id"]
            if isinstance(bid, str) and bid:
                out[bid] = self._row_to_entry(row)
        return out

    def existing_ids(self, backtest_ids: Iterable[str]) -> set[str]:
        ids = [x for x in backtest_ids if isinstance(x, str) and x]
        if not ids:
            return set()
        placeholders = ",".join(["?"] * len(ids))
        sql = f"SELECT backtest_id FROM runs WHERE backtest_id IN ({placeholders})"
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(ids)).fetchall()
        return {str(r["backtest_id"]) for r in rows if r and r["backtest_id"]}

    def list_submitted_ids(
        self,
        submitted_after: datetime | None = None,
        submitted_before: datetime | None = None,
    ) -> list[str]:
        where = ["submitted_at IS NOT NULL", "submitted_at <> ''"]
        params: list[Any] = []
        if submitted_after is not None:
            where.append("submitted_at >= ?")
            params.append(self._dt_to_iso(submitted_after))
        if submitted_before is not None:
            where.append("submitted_at < ?")
            params.append(self._dt_to_iso(submitted_before))
        sql = "SELECT backtest_id FROM runs WHERE " + " AND ".join(where) + " ORDER BY submitted_at DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [str(r["backtest_id"]) for r in rows if r and r["backtest_id"]]

    def get_runs_by_status(self, status: str) -> list[dict[str, Any]]:
        """Return all runs matching *status*, each dict includes ``backtest_id``."""
        sql = "SELECT * FROM runs WHERE status = ? ORDER BY created_at"
        with self._connect() as conn:
            rows = conn.execute(sql, (status,)).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            entry = self._row_to_entry(row)
            entry["backtest_id"] = row["backtest_id"]
            result.append(entry)
        return result

    def upsert_run(self, backtest_id: str, updates: dict[str, Any]) -> None:
        if not isinstance(backtest_id, str) or not backtest_id:
            raise ValueError("backtest_id must be a non-empty string")
        if not isinstance(updates, dict):
            raise ValueError("updates must be a dict")

        # Preserve "merge" semantics similar to the legacy JSON mapping:
        # read existing -> update -> write, serialized to avoid lost updates.
        with self._write_lock:
            existing = self.get_run(backtest_id) or {}
            merged = dict(existing)
            merged.update(updates)
            merged.setdefault("created_at", utc_now())

            known: dict[str, Any] = {}
            extra: dict[str, Any] = {}
            for key, value in merged.items():
                if key in self._KNOWN_FIELDS:
                    known[key] = value
                elif key not in {"backtest_id"}:
                    extra[key] = value

            # Normalize common UTC timestamps to the legacy "Z" format for stable sorting/filtering.
            for ts_key in ("created_at", "queued_at", "submitted_at", "cancelled_at", "last_error_at"):
                raw = known.get(ts_key)
                if isinstance(raw, str) and raw.endswith("+00:00"):
                    known[ts_key] = raw[:-6] + "Z"

            required_memory_gb = known.get("required_memory_gb")
            if required_memory_gb is not None:
                try:
                    known["required_memory_gb"] = float(required_memory_gb)
                except Exception:
                    # Keep the previous value if coercion fails.
                    known.pop("required_memory_gb", None)

            params = {
                "backtest_id": backtest_id,
                "status": known.get("status"),
                "created_at": known.get("created_at"),
                "queued_at": known.get("queued_at"),
                "submitted_at": known.get("submitted_at"),
                "cancelled_at": known.get("cancelled_at"),
                "requested_by": known.get("requested_by"),
                "required_memory_gb": known.get("required_memory_gb"),
                "backtest_docker_run_id": known.get("backtest_docker_run_id"),
                "backtest_api_base": known.get("backtest_api_base"),
                "last_error": known.get("last_error"),
                "last_error_at": known.get("last_error_at"),
                "extra_json": _json_dumps(extra),
            }

            columns = ",".join(self._COLUMNS)
            placeholders = ",".join([f":{c}" for c in self._COLUMNS])
            updates_sql = ",".join([f"{c}=excluded.{c}" for c in self._COLUMNS if c != "backtest_id"])
            sql = f"""
                INSERT INTO runs ({columns})
                VALUES ({placeholders})
                ON CONFLICT(backtest_id) DO UPDATE SET {updates_sql}
            """
            with self._connect() as conn:
                conn.execute(sql, params)

    def import_from_json_mapping(self, mapping_path: Path) -> int:
        if not mapping_path.exists():
            return 0
        try:
            size_bytes = mapping_path.stat().st_size
        except Exception:
            size_bytes = None
        LOGGER.info(
            "run_store_import_json_start path=%s size_bytes=%s",
            mapping_path,
            size_bytes if size_bytes is not None else "-",
        )
        raw = mapping_path.read_text("utf-8")
        try:
            mapping = json.loads(raw)
        except Exception as exc:
            raise ValueError(f"invalid json mapping: {exc}") from exc
        if not isinstance(mapping, dict):
            raise ValueError("run_mapping.json must be an object")

        imported = 0
        for backtest_id, entry in mapping.items():
            if not isinstance(backtest_id, str) or not backtest_id:
                continue
            if isinstance(entry, dict):
                updates = entry
            elif entry is None:
                updates = {}
            else:
                updates = {"backtest_docker_run_id": entry}
            self.upsert_run(backtest_id, updates)
            imported += 1
        LOGGER.info(
            "run_store_import_json_ok path=%s total_keys=%s imported_rows=%s",
            mapping_path,
            len(mapping),
            imported,
        )
        return imported

    def _row_to_entry(self, row: sqlite3.Row) -> dict[str, Any]:
        entry: dict[str, Any] = {}
        for key in self._KNOWN_FIELDS:
            if key in row.keys():
                value = row[key]
                if value is not None:
                    entry[key] = value
        extra = _json_loads(row["extra_json"] if "extra_json" in row.keys() else None)
        entry.update(extra)
        return entry

    def _dt_to_iso(self, dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
