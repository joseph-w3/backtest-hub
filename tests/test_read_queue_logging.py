"""Tests for read_queue error logging (P1 fix).

Validates that read_queue logs errors instead of silently swallowing them.
"""

import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

import app


class TestReadQueueLogging(unittest.TestCase):
    """read_queue must log exceptions instead of silently returning []."""

    def test_corrupted_json_logs_error(self) -> None:
        """When the queue file contains invalid JSON, logger.error should be called."""
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            qpath.write_text("{corrupted!", encoding="utf-8")

            with patch.object(app.logger, "error") as mock_error:
                result = app.read_queue(path=qpath)

            self.assertEqual(result, [])
            mock_error.assert_called_once()
            call_args = mock_error.call_args
            # First positional arg is the format string
            self.assertIn("queue_file_read_failed", call_args[0][0])
            # path should be in the call
            self.assertEqual(call_args[0][1], qpath)

    def test_permission_error_logs_error(self) -> None:
        """When the queue file is unreadable, logger.error should be called."""
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            qpath.write_text("[]", encoding="utf-8")
            qpath.chmod(0o000)

            try:
                with patch.object(app.logger, "error") as mock_error:
                    result = app.read_queue(path=qpath)

                self.assertEqual(result, [])
                mock_error.assert_called_once()
                call_args = mock_error.call_args
                self.assertIn("queue_file_read_failed", call_args[0][0])
            finally:
                qpath.chmod(0o644)

    def test_valid_json_no_error_logged(self) -> None:
        """When the queue file is valid, no error should be logged."""
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            qpath.write_text(
                json.dumps([{"backtest_id": "bt1", "queued_at": "2026-01-01T00:00:00Z"}]),
                encoding="utf-8",
            )

            with patch.object(app.logger, "error") as mock_error:
                result = app.read_queue(path=qpath)

            self.assertEqual(len(result), 1)
            mock_error.assert_not_called()

    def test_missing_file_no_error_logged(self) -> None:
        """When the queue file does not exist, no error should be logged (normal case)."""
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "nonexistent.json"

            with patch.object(app.logger, "error") as mock_error:
                result = app.read_queue(path=qpath)

            self.assertEqual(result, [])
            mock_error.assert_not_called()

    def test_exc_info_included_in_log(self) -> None:
        """The error log should include exc_info=True for traceback."""
        with tempfile.TemporaryDirectory() as td:
            qpath = Path(td) / "queue.json"
            qpath.write_text("not json at all", encoding="utf-8")

            with patch.object(app.logger, "error") as mock_error:
                app.read_queue(path=qpath)

            mock_error.assert_called_once()
            # exc_info=True should be in kwargs
            self.assertTrue(mock_error.call_args[1].get("exc_info", False))


if __name__ == "__main__":
    unittest.main()
