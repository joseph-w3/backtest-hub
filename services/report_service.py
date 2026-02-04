from __future__ import annotations

import json
import logging
import sqlite3
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response

LOGGER = logging.getLogger("backtest_hub.report_service")

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


def validate_backtest_id(backtest_id: str) -> None:
    if not backtest_id or any(ch in backtest_id for ch in ("\\", "/", ".")):
        raise HTTPException(status_code=400, detail="backtest_id has invalid characters")


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
        LOGGER.info("report_cache_init_start path=%s", self._path)
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
        LOGGER.info("report_cache_init_ok path=%s", self._path)

    def get(self, backtest_id: str) -> dict[str, Any] | None:
        if not self._path.exists():
            LOGGER.info("report_cache_miss backtest_id=%s reason=no_cache_file", backtest_id)
            return None
        with self._lock:
            with sqlite3.connect(self._path) as conn:
                row = conn.execute(
                    "SELECT report_json FROM backtest_reports WHERE backtest_id = ?",
                    (backtest_id,),
                ).fetchone()
        if not row:
            LOGGER.info("report_cache_miss backtest_id=%s reason=not_found", backtest_id)
            return None
        try:
            payload = json.loads(row[0])
        except Exception:
            LOGGER.info("report_cache_miss backtest_id=%s reason=invalid_json", backtest_id)
            return None
        if not isinstance(payload, dict):
            LOGGER.info("report_cache_miss backtest_id=%s reason=invalid_payload", backtest_id)
            return None
        LOGGER.info("report_cache_hit backtest_id=%s", backtest_id)
        return payload

    def upsert(
        self,
        backtest_id: str,
        report: dict[str, Any],
        requested_by: str | None,
        run_created_at: str,
    ) -> None:
        if not isinstance(report, dict):
            LOGGER.info("report_cache_upsert_skip backtest_id=%s reason=invalid_report", backtest_id)
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(report, ensure_ascii=True, indent=2)
        cached_at = utc_now()
        LOGGER.info(
            "report_cache_upsert_start backtest_id=%s requested_by=%s run_created_at=%s",
            backtest_id,
            requested_by,
            run_created_at,
        )
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
        LOGGER.info("report_cache_upsert_ok backtest_id=%s cached_at=%s", backtest_id, cached_at)


@dataclass(frozen=True)
class ReportServiceConfig:
    cache_path: Path
    report_path: str
    data_download_path: str
    runs_path: str
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

    def attach_backtest_api_base(self, payload: dict[str, Any], base_url: str | None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return payload
        enriched = dict(payload)
        if base_url:
            enriched["backtest_api_base"] = base_url
        return enriched

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
        LOGGER.info("report_fetch_start backtest_id=%s base=%s url=%s", backtest_id, base_url, url)
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8")
            LOGGER.info(
                "report_fetch_http_error backtest_id=%s status=%s detail=%s",
                backtest_id,
                exc.code,
                detail,
            )
            raise ReportHttpError(exc.code, detail or "backtest error") from exc
        except Exception as exc:
            LOGGER.info("report_fetch_error backtest_id=%s error=%s", backtest_id, exc)
            raise ReportFetchError(str(exc)) from exc
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            LOGGER.info("report_fetch_invalid_json backtest_id=%s", backtest_id)
            raise ReportInvalidPayload("backtest report invalid response") from exc
        if not isinstance(payload, dict):
            LOGGER.info("report_fetch_invalid_payload backtest_id=%s", backtest_id)
            raise ReportInvalidPayload("backtest report invalid response")
        LOGGER.info("report_fetch_ok backtest_id=%s", backtest_id)
        return payload

    def fetch_data_package(self, backtest_id: str) -> tuple[bytes, str, dict[str, str]]:
        validate_backtest_id(backtest_id)
        entry = self._ensure_backtest_routable(backtest_id, allow_running_only=False)  # type: ignore[call-arg]
        base_url = entry["backtest_api_base"]
        url = normalize_join_url(base_url, self._config.data_download_path.format(backtest_id=backtest_id))
        req = urllib.request.Request(url, headers=self._backtest_headers(), method="GET")
        LOGGER.info("data_package_fetch_start backtest_id=%s base=%s url=%s", backtest_id, base_url, url)
        try:
            with urllib.request.urlopen(req) as resp:
                data = resp.read()
                content_type = resp.headers.get("Content-Type", "application/octet-stream")
                headers: dict[str, str] = {}
                content_disposition = resp.headers.get("Content-Disposition")
                if content_disposition:
                    headers["Content-Disposition"] = content_disposition
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8")
            LOGGER.info(
                "data_package_fetch_http_error backtest_id=%s status=%s detail=%s",
                backtest_id,
                exc.code,
                detail,
            )
            raise ReportHttpError(exc.code, detail or "backtest error") from exc
        except Exception as exc:
            LOGGER.info("data_package_fetch_error backtest_id=%s error=%s", backtest_id, exc)
            raise ReportFetchError(str(exc)) from exc
        LOGGER.info("data_package_fetch_ok backtest_id=%s", backtest_id)
        return data, content_type, headers

    def fetch_runs(self, base_url: str) -> list[dict[str, Any]]:
        url = normalize_join_url(base_url, self._config.runs_path)
        req = urllib.request.Request(url, headers=self._backtest_headers(), method="GET")
        LOGGER.info("runs_fetch_start base=%s url=%s", base_url, url)
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8")
            LOGGER.info(
                "runs_fetch_http_error base=%s status=%s detail=%s",
                base_url,
                exc.code,
                detail,
            )
            raise ReportHttpError(exc.code, detail or "backtest error") from exc
        except Exception as exc:
            LOGGER.info("runs_fetch_error base=%s error=%s", base_url, exc)
            raise ReportFetchError(str(exc)) from exc

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            LOGGER.info("runs_fetch_invalid_json base=%s", base_url)
            raise ReportInvalidPayload("backtest runs invalid response") from exc
        if not isinstance(payload, dict):
            LOGGER.info("runs_fetch_invalid_payload base=%s", base_url)
            raise ReportInvalidPayload("backtest runs invalid response")

        runs = payload.get("runs")
        if not isinstance(runs, list):
            LOGGER.info("runs_fetch_missing_runs base=%s", base_url)
            raise ReportInvalidPayload("backtest runs invalid response")

        items: list[dict[str, Any]] = []
        for run in runs:
            if isinstance(run, dict):
                items.append(run)
        LOGGER.info("runs_fetch_ok base=%s count=%s", base_url, len(items))
        return items

    def list_active_runs(self, base_urls: list[str]) -> list[dict[str, Any]]:
        active_statuses = {"starting", "running", "stopping"}
        active: list[dict[str, Any]] = []
        success = 0
        for base_url in base_urls:
            try:
                runs = self.fetch_runs(base_url)
            except ReportServiceError as exc:
                LOGGER.info("active_runs_fetch_failed base=%s error=%s", base_url, exc)
                continue
            success += 1
            for run in runs:
                status = run.get("status")
                if status in active_statuses:
                    active.append(self.attach_backtest_api_base(run, base_url))
        if success == 0:
            raise ReportFetchError("backtest active runs unavailable")
        active.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return active

    def get_report(self, backtest_id: str, *, allow_running_only: bool = False) -> dict[str, Any]:
        LOGGER.info("report_get_start backtest_id=%s allow_running_only=%s", backtest_id, allow_running_only)
        cached = self._cache.get(backtest_id)
        if cached is not None:
            LOGGER.info("report_get_cache_hit backtest_id=%s", backtest_id)
            return cached
        LOGGER.info("report_get_cache_miss backtest_id=%s", backtest_id)

        if allow_running_only:
            entry = self._ensure_backtest_routable(backtest_id)
        else:
            entry = self._ensure_backtest_routable(backtest_id, allow_running_only=False)  # type: ignore[call-arg]
        base_url = entry["backtest_api_base"]
        LOGGER.info("report_get_routable backtest_id=%s base=%s", backtest_id, base_url)
        payload = self.fetch_report(base_url, backtest_id)

        if isinstance(payload, dict) and payload.get("report_available") is True:
            requested_by = self.get_requested_by_from_entry(backtest_id, entry)
            cache_requested_by = requested_by or report_requested_by(payload)
            created_at = get_entry_created_at(backtest_id, entry)
            self._cache.upsert(backtest_id, payload, cache_requested_by, created_at)
            LOGGER.info(
                "report_get_cached backtest_id=%s requested_by=%s created_at=%s",
                backtest_id,
                cache_requested_by,
                created_at,
            )
        else:
            LOGGER.info("report_get_unavailable backtest_id=%s", backtest_id)
        return payload

    def get_report_page(
        self,
        *,
        page: int,
        limit: int,
        requested_by: str | None,
        mapping: dict[str, Any],
    ) -> list[dict[str, Any]]:
        LOGGER.info(
            "report_page_start page=%s limit=%s requested_by=%s mapping_size=%s",
            page,
            limit,
            requested_by,
            len(mapping),
        )
        if page < 1 or limit < 1:
            raise ValueError("page/limit must be >= 1")
        if limit > self._config.max_page_size:
            raise ValueError(f"limit exceeds max size {self._config.max_page_size}")

        sorted_entries = sorted_mapping_entries(mapping)
        offset = (page - 1) * limit
        matched = 0
        results: list[dict[str, Any]] = []

        for created_at, backtest_id, entry in sorted_entries:
            base_url = entry.get("backtest_api_base") if isinstance(entry, dict) else None
            if not isinstance(base_url, str) or not base_url:
                base_url = None
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
                    results.append(self.attach_backtest_api_base(cached, base_url))
                    if len(results) >= limit:
                        break
                continue

            if not base_url:
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
                results.append(self.attach_backtest_api_base(payload, base_url))
                if len(results) >= limit:
                    break

        LOGGER.info(
            "report_page_ok page=%s limit=%s requested_by=%s matched=%s returned=%s",
            page,
            limit,
            requested_by,
            matched,
            len(results),
        )
        return results


def normalize_join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/" + path.lstrip("/")


def build_report_router(
    *,
    get_report_service: Callable[[], ReportService],
    read_mapping: Callable[[], dict[str, Any]],
    mapping_lock: threading.Lock,
    max_report_page_size: int,
    active_base_urls: Callable[[], list[str]],
) -> APIRouter:
    router = APIRouter(tags=["report_service"])

    @router.get("/runs/backtest/reports")
    async def get_backtest_reports(
        page: int = 1,
        limit: int = 10,
        requested_by: str | None = None,
    ) -> JSONResponse:
        if page < 1 or limit < 1:
            raise HTTPException(status_code=400, detail="page/limit must be >= 1")
        if limit > max_report_page_size:
            raise HTTPException(status_code=400, detail=f"limit exceeds max size {max_report_page_size}")

        with mapping_lock:
            mapping = read_mapping()
        service = get_report_service()
        try:
            items = service.get_report_page(
                page=page,
                limit=limit,
                requested_by=requested_by,
                mapping=mapping,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(
            {
                "page": page,
                "limit": limit,
                "count": len(items),
                "items": items,
            }
        )

    @router.get("/runs/backtest/{backtest_id}/report")
    async def get_backtest_report(backtest_id: str) -> JSONResponse:
        LOGGER.info("get_backtest_report_requested backtest_id=%s", backtest_id)
        service = get_report_service()
        try:
            payload = service.get_report(backtest_id, allow_running_only=False)
        except ReportHttpError as exc:
            LOGGER.info(
                "fetch_backtest_report_http_error backtest_id=%s status=%s detail=%s",
                backtest_id,
                exc.status_code,
                exc.detail[:200] if exc.detail else "",
            )
            raise HTTPException(status_code=exc.status_code, detail=exc.detail or "backtest error") from exc
        except ReportInvalidPayload as exc:
            LOGGER.info("fetch_backtest_report_invalid_payload backtest_id=%s error=%s", backtest_id, exc)
            raise HTTPException(status_code=502, detail="backtest report invalid response") from exc
        except ReportFetchError as exc:
            LOGGER.info("fetch_backtest_report_failed backtest_id=%s error=%s", backtest_id, exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            LOGGER.info("fetch_backtest_report_failed backtest_id=%s error=%s", backtest_id, exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return JSONResponse(payload)

    @router.get("/runs/backtest/{backtest_id}/download_data")
    async def download_backtest_data(backtest_id: str) -> Response:
        LOGGER.info("download_data_requested backtest_id=%s", backtest_id)
        service = get_report_service()
        try:
            data, content_type, headers = service.fetch_data_package(backtest_id)
        except ReportHttpError as exc:
            LOGGER.info(
                "download_data_http_error backtest_id=%s status=%s detail=%s",
                backtest_id,
                exc.status_code,
                exc.detail[:200] if exc.detail else "",
            )
            raise HTTPException(status_code=exc.status_code, detail=exc.detail or "backtest error") from exc
        except ReportFetchError as exc:
            LOGGER.info("download_data_request_failed backtest_id=%s error=%s", backtest_id, exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            LOGGER.info("download_data_request_failed backtest_id=%s error=%s", backtest_id, exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return Response(content=data, media_type=content_type, headers=headers)

    @router.get("/runs/backtest/active")
    async def get_active_backtest_runs() -> JSONResponse:
        service = get_report_service()
        try:
            runs = service.list_active_runs(active_base_urls())
        except ReportHttpError as exc:
            LOGGER.info(
                "active_runs_http_error status=%s detail=%s",
                exc.status_code,
                exc.detail[:200] if exc.detail else "",
            )
            raise HTTPException(status_code=exc.status_code, detail=exc.detail or "backtest error") from exc
        except ReportInvalidPayload as exc:
            LOGGER.info("active_runs_invalid_payload error=%s", exc)
            raise HTTPException(status_code=502, detail="backtest runs invalid response") from exc
        except ReportFetchError as exc:
            LOGGER.info("active_runs_fetch_failed error=%s", exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            LOGGER.info("active_runs_fetch_failed error=%s", exc)
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return JSONResponse({"count": len(runs), "runs": runs})

    return router
