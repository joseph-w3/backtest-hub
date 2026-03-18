"""Tests for runner support bundle packaging and submit forwarding."""

import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy:8000")

import app


class TestRunnerSupportBundle(unittest.TestCase):
    def test_build_runner_support_bundle_contains_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            support_dir = root / "scripts"
            support_dir.mkdir(parents=True, exist_ok=True)
            filenames = ("alpha.py", "beta.py")
            for name in filenames:
                (support_dir / name).write_text(f"# {name}\n", encoding="utf-8")

            with patch.object(app, "RUNNER_SUPPORT_PATH", support_dir), patch.object(
                app,
                "RUNNER_SUPPORT_FILENAMES",
                filenames,
            ):
                data = app.build_runner_support_bundle_bytes()

        with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
            self.assertEqual(sorted(zf.namelist()), sorted(filenames))
            self.assertEqual(zf.read("alpha.py").decode("utf-8"), "# alpha.py\n")

    @patch("app.urllib.request.urlopen")
    def test_submit_to_backtest_forwards_runner_support_bundle(self, mock_urlopen: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"run_id":"abc123"}'
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        app.submit_to_backtest(
            base_url="http://worker:8000",
            backtest_id="test-id",
            run_spec_bytes=b'{"symbols":["BTCUSDT"]}',
            strategy_filename="strat.py",
            strategy_bytes=b"# strategy",
            bundle_filename=None,
            bundle_bytes=None,
            runner_bytes=b"# runner",
            runner_support_bundle_filename="runner-support-bundle.zip",
            runner_support_bundle_bytes=b"zip-bytes",
        )

        request = mock_urlopen.call_args.args[0]
        body = request.data
        assert isinstance(body, bytes)
        self.assertIn(b'name="runner_support_bundle"', body)
        self.assertIn(b"runner-support-bundle.zip", body)
        self.assertIn(b"zip-bytes", body)


if __name__ == "__main__":
    unittest.main()
