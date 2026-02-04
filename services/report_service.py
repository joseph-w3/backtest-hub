from __future__ import annotations

import json
import sqlite3
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ReportServiceError(Exception):
    """Base error for report service failures."""


class ReportHttpError(ReportServiceError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class ReportInvalidPayload(ReportServiceError):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class ReportFetchError(ReportServiceError):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def parse_backtest_id_timestamp(backtest_id: str) -> str | None:
    if not backtest_id:
        return None
    token = backtest_id.split("_", 1)[0]
    try:
        parsed = datetime.strptime(token, "%Y%m%dT%H%M%SZ")
    except Exception:
        return None
    return parsed.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def get_entry_created_at(backtest_id: str, entry: dict[str, Any] | None) -> str:
    if isinstance(entry, dict):
        created_at = entry.get("created_at")
        if isinstance(created_at, str) and created_at:
            return created_at
    parsed = parse_backtest_id_timestamp(backtest_id)
    if parsed:
        return parsed
    return "0001-01-01T00:00:00Z"


def report_requested_by(payload: dict[str, Any]) -> str | None:
    value = payload.get("requested_by")
    if isinstance(value, str) and value:
        return value
    return None


def sorted_mapping_entries(mapping: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    items: list[tuple[str, str, dict[str, Any]]] = []
    for backtest_id, entry in mapping.items():
        if not isinstance(entry, dict):
            continue
        created_at = get_entry_created_at(backtest_id, entry)
        items.append((created_at, backtest_id, entry))
    items.sort(key=lambda item: item[0], reverse=True)
    return items


@dataclass(frozen=True)
class ReportCacheConfig:
    path: Path


class ReportCache:
    def __init__(self, config: ReportCacheConfig) -> None:
        self._path = config.path
        self._lock = threading.Lock()

    def init_db(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with sqlite3.connect(self._path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS backtest_reports (
                      backtest_id TEXT PRIMARY KEY,
                      requested_by TEXT,
                      run_created_at TEXT,
                      report_json TEXT NOT NULL,
                      cached_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_reports_requested_by ON backtest_reports(requested_by)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_reports_created_at ON backtest_reports(run_created_at)"
                )
                conn.commit()

    def get(self, backtest_id: str) -> dict[str, Any] | None:
        if not self._path.exists():
            return None
        with self._lock:
            with sqlite3.connect(self._path) as conn:
                row = conn.execute(
                    "SELECT report_json FROM backtest_reports WHERE backtest_id = ?",
                    (backtest_id,),
                ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row[0])
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def upsert(
        self,
        backtest_id: str,
        report: dict[str, Any],
        requested_by: str | None,
        run_created_at: str,
    ) -> None:
        if not isinstance(report, dict):
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(report, ensure_ascii=True, indent=2)
        cached_at = utc_now()
        with self._lock:
            with sqlite3.connect(self._path) as conn:
                conn.execute(
                    """
                    INSERT INTO backtest_reports (
                      backtest_id,
                      requested_by,
                      run_created_at,
                      report_json,
                      cached_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(backtest_id) DO UPDATE SET
                      requested_by = excluded.requested_by,
                      run_created_at = excluded.run_created_at,
                      report_json = excluded.report_json,
                      cached_at = excluded.cached_at
                    """,
                    (backtest_id, requested_by, run_created_at, payload, cached_at),
                )
                conn.commit()


@dataclass(frozen=True)
class ReportServiceConfig:
    cache_path: Path
    report_path: str
    max_page_size: int


class ReportService:
    def __init__(
        self,
        config: ReportServiceConfig,
        *,
        backtest_headers: Callable[[], dict[str, str]],
        ensure_backtest_routable: Callable[[str], dict[str, Any]],
        update_mapping: Callable[[str, dict[str, Any]], None],
        load_run_spec_payload: Callable[[str], dict[str, Any]],
    ) -> None:
        self._config = config
        self._backtest_headers = backtest_headers
        self._ensure_backtest_routable = ensure_backtest_routable
        self._update_mapping = update_mapping
        self._load_run_spec_payload = load_run_spec_payload
        self._cache = ReportCache(ReportCacheConfig(path=config.cache_path))

    @property
    def cache(self) -> ReportCache:
        return self._cache

    def init_cache(self) -> None:
        self._cache.init_db()

    def get_requested_by_from_entry(self, backtest_id: str, entry: dict[str, Any] | None) -> str | None:
        if isinstance(entry, dict):
            requested_by = entry.get("requested_by")
            if isinstance(requested_by, str) and requested_by:
                return requested_by
        try:
            payload = self._load_run_spec_payload(backtest_id)
        except Exception:
            return None
        requested_by = payload.get("requested_by")
        if isinstance(requested_by, str) and requested_by:
            self._update_mapping(backtest_id, {"requested_by": requested_by})
            return requested_by
        return None

    def fetch_report(self, base_url: str, backtest_id: str) -> dict[str, Any]:
        url = normalize_join_url(base_url, self._config.report_path.format(backtest_id=backtest_id))
        req = urllib.request.Request(url, headers=self._backtest_headers(), method="GET")
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8")
            raise ReportHttpError(exc.code, detail or "backtest error") from exc
        except Exception as exc:
            raise ReportFetchError(str(exc)) from exc
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            raise ReportInvalidPayload("backtest report invalid response") from exc
        if not isinstance(payload, dict):
            raise ReportInvalidPayload("backtest report invalid response")
        return payload

    def get_report(self, backtest_id: str, *, allow_running_only: bool = False) -> dict[str, Any]:
        cached = self._cache.get(backtest_id)
        if cached is not None:
            return cached

        if allow_running_only:
            entry = self._ensure_backtest_routable(backtest_id)
        else:
            entry = self._ensure_backtest_routable(backtest_id, allow_running_only=False)  # type: ignore[call-arg]
        base_url = entry["backtest_api_base"]
        payload = self.fetch_report(base_url, backtest_id)

        if isinstance(payload, dict) and payload.get("report_available") is True:
            requested_by = self.get_requested_by_from_entry(backtest_id, entry)
            cache_requested_by = requested_by or report_requested_by(payload)
            created_at = get_entry_created_at(backtest_id, entry)
            self._cache.upsert(backtest_id, payload, cache_requested_by, created_at)
        return payload

    def get_report_page(
        self,
        *,
        page: int,
        limit: int,
        requested_by: str | None,
        mapping: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if page < 1 or limit < 1:
            raise ValueError("page/limit must be >= 1")
        if limit > self._config.max_page_size:
            raise ValueError(f"limit exceeds max size {self._config.max_page_size}")

        sorted_entries = sorted_mapping_entries(mapping)
        offset = (page - 1) * limit
        matched = 0
        results: list[dict[str, Any]] = []

        for created_at, backtest_id, entry in sorted_entries:
            mapping_match = False
            requested_by_value = None
            if requested_by:
                requested_by_value = self.get_requested_by_from_entry(backtest_id, entry)
                if requested_by_value is not None:
                    if requested_by_value != requested_by:
                        continue
                    mapping_match = True

            cached = self._cache.get(backtest_id)
            if cached is not None:
                if requested_by and not mapping_match:
                    cached_requested_by = report_requested_by(cached)
                    if cached_requested_by != requested_by:
                        continue
                matched += 1
                if matched > offset:
                    results.append(cached)
                    if len(results) >= limit:
                        break
                continue

            base_url = entry.get("backtest_api_base")
            if not isinstance(base_url, str) or not base_url:
                continue

            try:
                payload = self.fetch_report(base_url, backtest_id)
            except Exception:
                continue

            if not isinstance(payload, dict) or payload.get("report_available") is not True:
                continue

            if requested_by and not mapping_match:
                payload_requested_by = report_requested_by(payload)
                if payload_requested_by != requested_by:
                    continue

            cache_requested_by = requested_by_value or report_requested_by(payload)
            self._cache.upsert(backtest_id, payload, cache_requested_by, created_at)

            matched += 1
            if matched > offset:
                results.append(payload)
                if len(results) >= limit:
                    break

        return results


def normalize_join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/" + path.lstrip("/")
