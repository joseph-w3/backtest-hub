"""
Tests for kill_backtest_run endpoint.

Covers:
1. Normal kill - mapping exists, backtest docker returns success
2. 404 - backtest_id not found in mapping
3. 409 - backtest is still queued (no backtest_api_base)
4. 502 - backtest docker request fails
5. Upstream HTTP error forwarding
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

from fastapi.testclient import TestClient

import app


class TestKillEndpoint(unittest.TestCase):
    """Test the /runs/backtest/{backtest_id}/kill endpoint."""

    def setUp(self) -> None:
        self.client = TestClient(app.app)

    def test_kill_not_found_returns_404(self) -> None:
        """When backtest_id is not in mapping, return 404."""
        with patch.object(app, "get_mapping_entry", return_value=None):
            response = self.client.post("/runs/backtest/nonexistent_id/kill")
            self.assertEqual(response.status_code, 404)
            self.assertIn("not found", response.json()["detail"])

    def test_kill_queued_not_in_queue_returns_409(self) -> None:
        """When backtest status is queued but not found in queue, return 409."""
        mock_entry = {
            "status": "queued",
            "queued_at": "2026-02-05T00:00:00Z",
        }

        def mock_remove_empty(ids):
            return set()  # Not found in queue

        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch.object(app, "remove_from_queue_batch", side_effect=mock_remove_empty):
                with patch.object(app, "QUEUE_STATE_LOCK", app.asyncio.Lock()):
                    response = self.client.post("/runs/backtest/bt_queued/kill")
                    self.assertEqual(response.status_code, 409)
                    self.assertIn("queued", response.json()["detail"].lower())

    def test_kill_queued_removes_from_queue_and_cancels(self) -> None:
        """
        When backtest is queued, kill should:
        1. Remove from queue
        2. Mark mapping as cancelled
        3. Return success with cancelled status
        """
        mock_entry = {
            "status": "queued",
            "queued_at": "2026-02-05T00:00:00Z",
        }
        removed_ids = set()
        updated_mappings = {}

        def mock_remove_from_queue_batch(ids):
            removed_ids.update(ids)
            return {"bt_queued"}

        def mock_update_mapping(bid, updates):
            updated_mappings[bid] = updates

        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch.object(app, "remove_from_queue_batch", side_effect=mock_remove_from_queue_batch):
                with patch.object(app, "update_mapping", side_effect=mock_update_mapping):
                    with patch.object(app, "QUEUE_STATE_LOCK", app.asyncio.Lock()):
                        response = self.client.post("/runs/backtest/bt_queued/kill")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "cancelled")
        self.assertIn("bt_queued", removed_ids)
        self.assertIn("bt_queued", updated_mappings)
        self.assertEqual(updated_mappings["bt_queued"]["status"], "cancelled")

    def test_kill_success_proxies_response(self) -> None:
        """When backtest docker returns success, proxy the response."""
        mock_entry = {
            "status": "submitted",
            "backtest_api_base": "http://backtest-docker:8000",
            "backtest_docker_run_id": "run_123",
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "run_id": "run_123",
            "status": "stopping",
            "message": "Kill signal sent"
        }).encode("utf-8")
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch("urllib.request.urlopen", return_value=mock_response):
                response = self.client.post("/runs/backtest/bt_running/kill")
                self.assertEqual(response.status_code, 200)
                data = response.json()
                self.assertEqual(data["run_id"], "run_123")
                self.assertEqual(data["status"], "stopping")

    def test_kill_upstream_http_error_forwarded(self) -> None:
        """When backtest docker returns HTTP error, forward it."""
        import urllib.error

        mock_entry = {
            "status": "submitted",
            "backtest_api_base": "http://backtest-docker:8000",
        }
        http_error = urllib.error.HTTPError(
            url="http://backtest-docker:8000/v1/runs/backtest/bt_running/kill",
            code=400,
            msg="Bad Request",
            hdrs={},  # type: ignore
            fp=None,
        )
        http_error.read = MagicMock(return_value=b"Task not in running state")

        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch("urllib.request.urlopen", side_effect=http_error):
                response = self.client.post("/runs/backtest/bt_running/kill")
                self.assertEqual(response.status_code, 400)
                self.assertIn("not in running", response.json()["detail"])

    def test_kill_upstream_connection_error_returns_502(self) -> None:
        """When backtest docker is unreachable, return 502."""
        mock_entry = {
            "status": "submitted",
            "backtest_api_base": "http://backtest-docker:8000",
        }
        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch("urllib.request.urlopen", side_effect=Exception("Connection refused")):
                response = self.client.post("/runs/backtest/bt_running/kill")
                self.assertEqual(response.status_code, 502)
                self.assertIn("Connection refused", response.json()["detail"])

    def test_kill_invalid_upstream_response_returns_502(self) -> None:
        """When backtest docker returns non-dict, return 502."""
        mock_entry = {
            "status": "submitted",
            "backtest_api_base": "http://backtest-docker:8000",
        }
        mock_response = MagicMock()
        mock_response.read.return_value = b'"just a string"'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch("urllib.request.urlopen", return_value=mock_response):
                response = self.client.post("/runs/backtest/bt_running/kill")
                self.assertEqual(response.status_code, 502)
                self.assertIn("invalid response", response.json()["detail"])

    def test_kill_constructs_correct_url(self) -> None:
        """Verify kill endpoint constructs the correct upstream URL."""
        mock_entry = {
            "status": "submitted",
            "backtest_api_base": "http://backtest-docker:8000/v1",
        }
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"status": "stopping"}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        captured_request = {}

        def capture_request(req):
            captured_request["url"] = req.full_url
            captured_request["method"] = req.method
            return mock_response

        with patch.object(app, "get_mapping_entry", return_value=mock_entry):
            with patch("urllib.request.urlopen", side_effect=capture_request):
                self.client.post("/runs/backtest/bt_123/kill")

        self.assertIn("url", captured_request)
        self.assertIn("bt_123", captured_request["url"])
        self.assertIn("/kill", captured_request["url"])
        self.assertEqual(captured_request["method"], "POST")


if __name__ == "__main__":
    unittest.main()
