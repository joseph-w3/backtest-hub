import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI
import httpx

from services.report_service import (
    ReportService,
    ReportServiceConfig,
    build_report_router,
)

# Helper for async iteration
class AsyncIterator:
    def __init__(self, seq):
        self.seq = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.seq)
        except StopIteration:
            raise StopAsyncIteration

class TestReportServiceStreaming(unittest.TestCase):
    def setUp(self):
        self.mapping = {
            "test_id": {
                "backtest_api_base": "http://mock-api",
                "submitted_at": "2026-02-01T00:00:00Z"
            }
        }
        self.service = ReportService(
            ReportServiceConfig(report_batch_path="/batch"),
            backtest_headers=lambda: {"Auth": "Token"},
        )
        # We need to patch httpx.AsyncClient inside the service method
        # But since we want to test the *endpoint* logic which calls service.download_logs,
        # we can mock service.download_logs instead OR mock httpx.AsyncClient.
        # Mocking httpx.AsyncClient is better to verify service -> client interaction too,
        # but mocking service.download_logs is easier to verify endpoint logic.
        # Given the refactor was about endpoint handling (client, response), let's mock httpx.AsyncClient.

    def make_client(self):
        lock = MagicMock()
        app = FastAPI()
        app.include_router(
            build_report_router(
                get_report_service=lambda: self.service,
                read_mapping=lambda: self.mapping,
                mapping_lock=lock,
            )
        )
        return TestClient(app)

    @patch("httpx.AsyncClient")
    def test_download_logs_success(self, mock_client_cls):
        # Setup mock client and response
        mock_client = AsyncMock() 
        mock_response = AsyncMock()
        
        mock_client_cls.return_value = mock_client
        mock_client.send.return_value = mock_response
        
        # Setup successful response
        mock_response.status_code = 200
        # aiter_bytes should be a method that returns the iterator when called
        # Since it's an AsyncMock, calling it returns a coroutine by default if we don't specify otherwise?
        # No, AsyncMock makes all methods async. aiter_bytes in httpx is indeed an async method (returns async generator)
        # BUT if we mock it, we want `aiter_bytes()` to return the async iterator.
        # If we use AsyncMock for `aiter_bytes`, `await response.aiter_bytes()` would be needed if it was awaited, 
        # but `async for chunk in response.aiter_bytes()` expects an object with `__aiter__`.
        # httpx.Response.aiter_bytes IS an async generator method, so calling it returns an async generator.
        # So we should make the mock method return the iterator directly.
        # However AsyncMock calling returns a coroutine.
        # We need `mock_response.aiter_bytes` to be a MagicMock (or simple function) that returns our iterator, NOT an AsyncMock.
        mock_response.aiter_bytes = MagicMock(return_value=AsyncIterator([b"line1\n", b"line2\n"]))
        
        client = self.make_client()
        resp = client.get("/runs/backtest/test_id/logs/download")
        
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, b"line1\nline2\n")
        
        # Verify cleanup
        mock_response.aclose.assert_awaited()
        mock_client.aclose.assert_awaited()

    @patch("httpx.AsyncClient")
    def test_download_logs_upstream_error(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        
        mock_client_cls.return_value = mock_client
        mock_client.send.return_value = mock_response
        
        # Setup error response
        mock_response.status_code = 404
        mock_response.aread.return_value = b"Log not found"
        
        client = self.make_client()
        resp = client.get("/runs/backtest/test_id/logs/download")
        
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()["detail"], "Failed to download logs: Log not found")
        
        # Verify cleanup (should close immediately)
        mock_response.aclose.assert_awaited()
        mock_client.aclose.assert_awaited()

    @patch("httpx.AsyncClient")
    def test_download_logs_connection_error(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client
        
        # Simulate connection error
        mock_client.send.side_effect = httpx.ConnectError("Connection refused")
        
        client = self.make_client()
        resp = client.get("/runs/backtest/test_id/logs/download")
        
        self.assertEqual(resp.status_code, 500)
        
        # Verify client closed (send raises, so we catch exception and close)
        mock_client.aclose.assert_awaited()

    @patch("httpx.AsyncClient")
    def test_download_code_success(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        
        mock_client_cls.return_value = mock_client
        mock_client.send.return_value = mock_response
        
        mock_response.status_code = 200
        # Same fix for download_code
        mock_response.aiter_bytes = MagicMock(return_value=AsyncIterator([b"PK\x03\x04..."]))
        
        client = self.make_client()
        resp = client.get("/runs/backtest/test_id/download_code")
        
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, b"PK\x03\x04...")
        
        mock_response.aclose.assert_awaited()
        mock_client.aclose.assert_awaited()

    def test_download_logs_invalid_id(self):
        client = self.make_client()
        # Test dots
        resp = client.get("/runs/backtest/test.id/logs/download")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("invalid characters", resp.json()["detail"])
        
        # Test backslash
        resp = client.get("/runs/backtest/test\\id/logs/download")
        self.assertEqual(resp.status_code, 400)

    def test_download_code_invalid_id(self):
        client = self.make_client()
        resp = client.get("/runs/backtest/id_with_forward/slash/download_code")
        # This will actually 404 because of extra segment, but let's test a valid segment with invalid char
        resp = client.get("/runs/backtest/test.zip/download_code")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("invalid characters", resp.json()["detail"])

