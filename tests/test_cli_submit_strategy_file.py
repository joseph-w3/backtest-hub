import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch


from backtest_hub_cli import cli


class _DummyResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class TestSubmitStrategyFileResolution(unittest.TestCase):
    def _args(
        self,
        *,
        run_spec: str,
        name: str = "test",
        strategy_file: str | None = None,
        strategy_bundle: str | None = None,
        no_generate: bool = True,
        follow_logs: bool = False,
    ) -> Namespace:
        return Namespace(
            run_spec=run_spec,
            name=name,
            strategy_file=strategy_file,
            strategy_bundle=strategy_bundle,
            no_generate=no_generate,
            follow_logs=follow_logs,
        )

    def test_strategy_file_cli_arg_has_priority_over_run_spec(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                strategy_a = Path(td) / "a.py"
                strategy_b = Path(td) / "b.py"
                strategy_a.write_text("print('a')\n", encoding="utf-8")
                strategy_b.write_text("print('b')\n", encoding="utf-8")

                run_spec = Path(td) / "run_spec.json"
                run_spec.write_text(
                    json.dumps({"strategy_file": str(strategy_b)}), encoding="utf-8"
                )

                captured: dict[str, object] = {}

                def _fake_build_multipart_form(files):
                    captured["files"] = files
                    return b"BODY", "multipart/form-data; boundary=test"

                def _fake_urlopen(req):
                    captured["req"] = req
                    return _DummyResponse(b'{"backtest_id":"x"}')

                with patch.object(cli, "build_multipart_form", _fake_build_multipart_form), patch.object(
                    cli.urllib.request, "urlopen", _fake_urlopen
                ), patch.object(cli, "write_run_id_history", lambda *a, **k: None):
                    out = StringIO()
                    with redirect_stdout(out):
                        rc = cli.command_submit(
                            self._args(run_spec=str(run_spec), strategy_file=str(strategy_a))
                        )

                self.assertEqual(rc, 0)
                self.assertIn("Using strategy file:", out.getvalue())
                self.assertIn(str(strategy_a.resolve()), out.getvalue())
                files = captured["files"]
                # Expect second file to be the strategy file.
                self.assertEqual(files[1][0], "strategy_file")
                self.assertEqual(files[1][1], "a.py")
                self.assertEqual(files[1][3], strategy_a.read_bytes())
            finally:
                os.chdir(cwd)

    def test_strategy_file_falls_back_to_run_spec(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                strategy = Path(td) / "s.py"
                strategy.write_text("print('s')\n", encoding="utf-8")

                run_spec = Path(td) / "run_spec.json"
                run_spec.write_text(
                    json.dumps({"strategy_file": str(strategy)}), encoding="utf-8"
                )

                captured: dict[str, object] = {}

                def _fake_build_multipart_form(files):
                    captured["files"] = files
                    return b"BODY", "multipart/form-data; boundary=test"

                def _fake_urlopen(req):
                    captured["req"] = req
                    return _DummyResponse(b'{"backtest_id":"x"}')

                with patch.object(cli, "build_multipart_form", _fake_build_multipart_form), patch.object(
                    cli.urllib.request, "urlopen", _fake_urlopen
                ), patch.object(cli, "write_run_id_history", lambda *a, **k: None):
                    out = StringIO()
                    with redirect_stdout(out):
                        rc = cli.command_submit(self._args(run_spec=str(run_spec), strategy_file=None))

                self.assertEqual(rc, 0)
                self.assertIn("Using strategy file:", out.getvalue())
                self.assertIn(str(strategy.resolve()), out.getvalue())
                files = captured["files"]
                self.assertEqual(files[1][0], "strategy_file")
                self.assertEqual(files[1][1], "s.py")
                self.assertEqual(files[1][3], strategy.read_bytes())
            finally:
                os.chdir(cwd)

    def test_strategy_file_falls_back_to_strategies_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                strategies_dir = Path(td) / "strategies"
                strategies_dir.mkdir(parents=True, exist_ok=True)
                strategy = strategies_dir / "spot_futures_arb_diagnostics.py"
                strategy.write_text("print('diag')\n", encoding="utf-8")

                run_spec = Path(td) / "run_spec.json"
                # Use basename only (common case): should resolve to ./strategies/<same-name>.
                run_spec.write_text(
                    json.dumps({"strategy_file": "spot_futures_arb_diagnostics.py"}),
                    encoding="utf-8",
                )

                captured: dict[str, object] = {}

                def _fake_build_multipart_form(files):
                    captured["files"] = files
                    return b"BODY", "multipart/form-data; boundary=test"

                def _fake_urlopen(req):
                    captured["req"] = req
                    return _DummyResponse(b'{"backtest_id":"x"}')

                with patch.object(cli, "build_multipart_form", _fake_build_multipart_form), patch.object(
                    cli.urllib.request, "urlopen", _fake_urlopen
                ), patch.object(cli, "write_run_id_history", lambda *a, **k: None):
                    out = StringIO()
                    with redirect_stdout(out):
                        rc = cli.command_submit(self._args(run_spec=str(run_spec), strategy_file=None))

                self.assertEqual(rc, 0)
                self.assertIn("Using strategy file:", out.getvalue())
                self.assertIn(str(strategy.resolve()), out.getvalue())
                files = captured["files"]
                self.assertEqual(files[1][0], "strategy_file")
                self.assertEqual(files[1][1], "spot_futures_arb_diagnostics.py")
                self.assertEqual(files[1][3], strategy.read_bytes())
            finally:
                os.chdir(cwd)

    def test_missing_strategy_file_in_args_and_run_spec_returns_1(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                run_spec = Path(td) / "run_spec.json"
                run_spec.write_text(json.dumps({}), encoding="utf-8")

                with patch.object(cli.urllib.request, "urlopen") as mock_urlopen:
                    err = StringIO()
                    with redirect_stderr(err):
                        rc = cli.command_submit(self._args(run_spec=str(run_spec), strategy_file=None))

                self.assertEqual(rc, 1)
                self.assertEqual(mock_urlopen.call_count, 0)
                self.assertIn("strategy file/bundle not specified", err.getvalue())
            finally:
                os.chdir(cwd)
