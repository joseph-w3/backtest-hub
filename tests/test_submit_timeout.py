"""Tests that submit_to_backtest and fetch_backtest_status pass timeout to urlopen."""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json

# app.py requires BACKTEST_API_BASES at module level; set before import
os.environ.setdefault("BACKTEST_API_BASES", "http://dummy:8000")


class TestSubmitToBacktestTimeout(unittest.TestCase):
    """urlopen must be called with a timeout parameter to prevent blocking."""

    @patch("app.urllib.request.urlopen")
    def test_urlopen_called_with_timeout(self, mock_urlopen: MagicMock) -> None:
        """submit_to_backtest should pass timeout to urlopen."""
        from app import submit_to_backtest

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"run_id": "abc123"}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        submit_to_backtest(
            base_url="http://worker:8000",
            backtest_id="test-id",
            run_spec_bytes=b'{"symbols": ["BTCUSDT"]}',
            strategy_filename="strat.py",
            strategy_bytes=b"# strategy",
            bundle_filename=None,
            bundle_bytes=None,
            runner_bytes=b"# runner",
        )

        mock_urlopen.assert_called_once()
        call_args = mock_urlopen.call_args
        # timeout can be positional arg[1] or keyword arg
        if call_args.kwargs.get("timeout") is not None:
            timeout_val = call_args.kwargs["timeout"]
        elif len(call_args.args) >= 2:
            timeout_val = call_args.args[1]
        else:
            self.fail("urlopen was called WITHOUT timeout parameter")

        self.assertIsInstance(timeout_val, (int, float))
        self.assertGreater(timeout_val, 0)
        # Should be a generous timeout (>= 60s) since backtests can be slow
        self.assertGreaterEqual(timeout_val, 60)

    @patch("app.urllib.request.urlopen")
    def test_timeout_value_is_300_seconds(self, mock_urlopen: MagicMock) -> None:
        """submit_to_backtest timeout should be 300 seconds."""
        from app import submit_to_backtest

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"run_id": "abc123"}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        submit_to_backtest(
            base_url="http://worker:8000",
            backtest_id="test-id",
            run_spec_bytes=b'{"symbols": ["BTCUSDT"]}',
            strategy_filename="strat.py",
            strategy_bytes=b"# strategy",
            bundle_filename=None,
            bundle_bytes=None,
            runner_bytes=b"# runner",
        )

        call_args = mock_urlopen.call_args
        if call_args.kwargs.get("timeout") is not None:
            timeout_val = call_args.kwargs["timeout"]
        elif len(call_args.args) >= 2:
            timeout_val = call_args.args[1]
        else:
            self.fail("urlopen was called WITHOUT timeout parameter")

        self.assertEqual(timeout_val, 300)


class TestFetchBacktestStatusTimeout(unittest.TestCase):
    """fetch_backtest_status should also have a timeout."""

    @patch("app.urllib.request.urlopen")
    def test_urlopen_called_with_timeout(self, mock_urlopen: MagicMock) -> None:
        from app import fetch_backtest_status

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "status": "running",
            "pid": 1234,
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": None,
        }).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        fetch_backtest_status(
            base_url="http://worker:8000",
            backtest_id="test-id",
        )

        mock_urlopen.assert_called_once()
        call_args = mock_urlopen.call_args
        if call_args.kwargs.get("timeout") is not None:
            timeout_val = call_args.kwargs["timeout"]
        elif len(call_args.args) >= 2:
            timeout_val = call_args.args[1]
        else:
            self.fail("urlopen was called WITHOUT timeout parameter")

        self.assertIsInstance(timeout_val, (int, float))
        self.assertGreater(timeout_val, 0)


if __name__ == "__main__":
    unittest.main()
