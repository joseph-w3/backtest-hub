from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, AsyncIterator
import httpx

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

LOGGER = logging.getLogger("backtest_hub.report_service")


class ReportServiceError(Exception):
    """Base error for report service failures."""


class ReportHttpError(ReportServiceError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class ReportFetchError(ReportServiceError):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class ReportInvalidPayload(ReportServiceError):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def normalize_join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/" + path.lstrip("/")


def parse_iso8601(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class ReportServiceConfig:
    report_batch_path: str


class ReportService:
    def __init__(
        self,
        config: ReportServiceConfig,
        *,
        backtest_headers: Callable[[], dict[str, str]],
    ) -> None:
        self._config = config
        self._backtest_headers = backtest_headers

    def fetch_reports_batch(
        self, base_url: str, backtest_ids: list[str]
    ) -> dict[str, dict[str, Any]]:
        if not backtest_ids:
            return {}
        url = normalize_join_url(base_url, self._config.report_batch_path)
        body = json.dumps({"backtest_ids": backtest_ids}, ensure_ascii=True).encode("utf-8")
        headers = dict(self._backtest_headers())
        headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        LOGGER.info("report_batch_fetch_start base=%s count=%s", base_url, len(backtest_ids))
        try:
            with urllib.request.urlopen(req) as resp:
                payload_bytes = resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8")
            LOGGER.info("report_batch_fetch_http_error base=%s status=%s", base_url, exc.code)
            raise ReportHttpError(exc.code, detail or "backtest error") from exc
        except Exception as exc:
            LOGGER.info("report_batch_fetch_error base=%s error=%s", base_url, exc)
            raise ReportFetchError(str(exc)) from exc

        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as exc:
            raise ReportInvalidPayload("invalid json response") from exc
        if not isinstance(payload, dict):
            raise ReportInvalidPayload("invalid response")

        reports = payload.get("reports")
        if not isinstance(reports, list):
            raise ReportInvalidPayload("missing reports in response")

        results: dict[str, dict[str, Any]] = {}
        for item in reports:
            if not isinstance(item, dict):
                continue
            bid = item.get("backtest_id")
            report = item.get("report")
            if not isinstance(bid, str) or not bid:
                continue
            if not isinstance(report, dict):
                continue
            results[bid] = report
        LOGGER.info(
            "report_batch_fetch_ok base=%s requested=%s returned=%s",
            base_url, len(backtest_ids), len(results),
        )
        return results

    async def _create_download_request(self, base_url: str, path: str) -> tuple[httpx.AsyncClient, httpx.Response]:
        url = normalize_join_url(base_url, path)
        headers = dict(self._backtest_headers())
        
        # Increased timeout for downloads
        timeout = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)
        client = httpx.AsyncClient(timeout=timeout)
        
        try:
            req = client.build_request("GET", url, headers=headers)
            response = await client.send(req, stream=True)
            return client, response
        except Exception:
            await client.aclose()
            raise

    async def download_logs(self, base_url: str, backtest_id: str) -> tuple[httpx.AsyncClient, httpx.Response]:
        return await self._create_download_request(
            base_url, 
            f"/runs/backtest/{backtest_id}/logs/download"
        )

    async def download_code(self, base_url: str, backtest_id: str) -> tuple[httpx.AsyncClient, httpx.Response]:
        return await self._create_download_request(
            base_url, 
            f"/runs/backtest/{backtest_id}/download_code"
        )


class BatchReportsRequest(BaseModel):
    backtest_ids: list[str]


def build_report_router(
    *,
    get_report_service: Callable[[], ReportService],
    read_mapping: Callable[[], dict[str, Any]],
    mapping_lock: threading.Lock,
) -> APIRouter:
    router = APIRouter(tags=["report_service"])

    @router.get("/runs/backtest/ids")
    async def list_backtest_ids(
        submitted_after: str | None = None,
        submitted_before: str | None = None,
    ) -> JSONResponse:
        after_dt: datetime | None = None
        before_dt: datetime | None = None
        try:
            if submitted_after:
                after_dt = parse_iso8601(submitted_after)
            if submitted_before:
                before_dt = parse_iso8601(submitted_before)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid datetime format: {exc}") from exc

        if after_dt and before_dt and after_dt >= before_dt:
            raise HTTPException(status_code=400, detail="submitted_after must be before submitted_before")

        with mapping_lock:
            mapping = read_mapping()

        results: list[str] = []
        for backtest_id, entry in mapping.items():
            if not isinstance(entry, dict):
                continue
            submitted_at = entry.get("submitted_at")
            if not isinstance(submitted_at, str) or not submitted_at:
                continue
            try:
                dt = parse_iso8601(submitted_at)
            except Exception:
                continue
            if after_dt and dt < after_dt:
                continue
            if before_dt and dt >= before_dt:
                continue
            results.append(backtest_id)

        results.sort(
            key=lambda bid: mapping[bid].get("submitted_at", ""),
            reverse=True,
        )

        return JSONResponse({"count": len(results), "backtest_ids": results})

    @router.post("/runs/backtest/reports")
    async def batch_get_reports(req: BatchReportsRequest) -> JSONResponse:
        if not req.backtest_ids:
            raise HTTPException(status_code=400, detail="backtest_ids must be non-empty")

        with mapping_lock:
            mapping = read_mapping()

        # 读取每个 backtest_id 的 entry，按 backtest_api_base 分组
        groups: dict[str, list[str]] = {}
        entries: dict[str, dict[str, Any]] = {}
        not_found: list[str] = []

        for bid in req.backtest_ids:
            entry = mapping.get(bid)
            if not isinstance(entry, dict):
                not_found.append(bid)
                continue
            base_url = entry.get("backtest_api_base")
            if not isinstance(base_url, str) or not base_url:
                not_found.append(bid)
                continue
            entries[bid] = entry
            groups.setdefault(base_url, []).append(bid)

        service = get_report_service()

        # 按 backtest_api_base 分组批量拉取报告
        results: dict[str, dict[str, Any]] = {}
        errors: dict[str, str] = {}

        for base_url, bids in groups.items():
            try:
                batch = service.fetch_reports_batch(base_url, bids)
            except ReportServiceError as exc:
                LOGGER.info("batch_reports_failed base=%s error=%s", base_url, exc)
                for bid in bids:
                    errors[bid] = str(exc)
                continue

            for bid in bids:
                report = batch.get(bid)
                if report is not None:
                    results[bid] = {
                        "backtest_id": bid,
                        "backtest_api_base": base_url,
                        "report": report,
                    }
                else:
                    errors[bid] = "report not available"

        return JSONResponse({
            "count": len(results),
            "results": results,
            "not_found": not_found,
            "errors": errors,
        })

    @router.get("/runs/backtest/{backtest_id}/logs/download")
    async def download_logs_endpoint(backtest_id: str) -> StreamingResponse:
        with mapping_lock:
            mapping = read_mapping()
            entry = mapping.get(backtest_id)
        
        if not entry or not isinstance(entry, dict):
            raise HTTPException(status_code=404, detail="Backtest not found")
            
        base_url = entry.get("backtest_api_base")
        if not isinstance(base_url, str) or not base_url:
             raise HTTPException(status_code=404, detail="Backtest API base not found")

        service = get_report_service()
        client = None
        response = None
        try:
            client, response = await service.download_logs(base_url, backtest_id)
            
            if response.status_code != 200:
                # Read body before closing to provide error detail
                try:
                    body = await response.aread()
                    detail = body.decode("utf-8", errors="replace")
                except Exception:
                    detail = "Unknown error"
                
                await response.aclose()
                await client.aclose()
                raise HTTPException(status_code=response.status_code, detail=f"Failed to download logs: {detail}")

            async def stream_with_cleanup():
                try:
                    async for chunk in response.aiter_bytes():
                        yield chunk
                except Exception:
                    # Log error during streaming if needed
                    LOGGER.error(f"stream_error backtest_id={backtest_id}", exc_info=True)
                    raise
                finally:
                    await response.aclose()
                    await client.aclose()

            return StreamingResponse(
                stream_with_cleanup(),
                media_type="text/plain",
                headers={"Content-Disposition": f'attachment; filename="{backtest_id}.log"'}
            )

        except HTTPException:
            if client:
                await client.aclose()
            raise
        except Exception as e:
            if client:
                await client.aclose()
            LOGGER.error(f"download_logs_error backtest_id={backtest_id} error={e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    @router.get("/runs/backtest/{backtest_id}/download_code")
    async def download_code_endpoint(backtest_id: str) -> StreamingResponse:
        with mapping_lock:
            mapping = read_mapping()
            entry = mapping.get(backtest_id)
        
        if not entry or not isinstance(entry, dict):
            raise HTTPException(status_code=404, detail="Backtest not found")
            
        base_url = entry.get("backtest_api_base")
        if not isinstance(base_url, str) or not base_url:
             raise HTTPException(status_code=404, detail="Backtest API base not found")

        service = get_report_service()
        client = None
        response = None
        try:
            client, response = await service.download_code(base_url, backtest_id)

            if response.status_code != 200:
                try:
                    body = await response.aread()
                    detail = body.decode("utf-8", errors="replace")
                except Exception:
                    detail = "Unknown error"
                    
                await response.aclose()
                await client.aclose()
                raise HTTPException(status_code=response.status_code, detail=f"Failed to download code: {detail}")

            async def stream_with_cleanup():
                try:
                    async for chunk in response.aiter_bytes():
                        yield chunk
                except Exception:
                    LOGGER.error(f"stream_error backtest_id={backtest_id}", exc_info=True)
                    raise
                finally:
                    await response.aclose()
                    await client.aclose()

            return StreamingResponse(
                stream_with_cleanup(),
                media_type="application/zip",
                headers={"Content-Disposition": f'attachment; filename="{backtest_id}_code.zip"'}
            )

        except HTTPException:
            if client:
                await client.aclose()
            raise
        except Exception as e:
            if client:
                 await client.aclose()
            LOGGER.error(f"download_code_error backtest_id={backtest_id} error={e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    return router
