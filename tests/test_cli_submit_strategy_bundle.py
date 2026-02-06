import json
import os
import tempfile
import unittest
import zipfile
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
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


class TestSubmitStrategyBundle(unittest.TestCase):
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

    def test_strategy_bundle_directory_uses_ignore(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                run_spec = Path(td) / "run_spec.json"
                run_spec.write_text(json.dumps({}), encoding="utf-8")

                bundle_dir = Path(td) / "my_strategy"
                bundle_dir.mkdir(parents=True, exist_ok=True)
                (bundle_dir / "__init__.py").write_text("# init\n", encoding="utf-8")
                (bundle_dir / "main.py").write_text("print('ok')\n", encoding="utf-8")
                (bundle_dir / "ignored.py").write_text("print('ignored')\n", encoding="utf-8")

                (bundle_dir / "__pycache__").mkdir(parents=True, exist_ok=True)
                (bundle_dir / "__pycache__" / "x.pyc").write_bytes(b"\x00\x01")

                (bundle_dir / ".strategyignore").write_text("ignored.py\n", encoding="utf-8")

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
                            self._args(run_spec=str(run_spec), strategy_bundle=str(bundle_dir))
                        )

                self.assertEqual(rc, 0)
                files = captured["files"]
                self.assertEqual(files[1][0], "strategy_bundle")
                self.assertEqual(files[1][1], "my_strategy.zip")

                bundle_bytes = files[1][3]
                with zipfile.ZipFile(BytesIO(bundle_bytes)) as zf:
                    names = [name for name in zf.namelist() if not name.endswith("/")]
                self.assertIn("my_strategy/__init__.py", names)
                self.assertIn("my_strategy/main.py", names)
                self.assertNotIn("my_strategy/ignored.py", names)
                self.assertNotIn("my_strategy/.strategyignore", names)
                self.assertNotIn("my_strategy/__pycache__/x.pyc", names)
            finally:
                os.chdir(cwd)

    def test_strategy_bundle_from_run_spec_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                bundle_path = Path(td) / "bundle.zip"
                buffer = BytesIO()
                with zipfile.ZipFile(buffer, "w") as zf:
                    zf.writestr("bundle/__init__.py", "# ok\n")
                bundle_path.write_bytes(buffer.getvalue())

                run_spec = Path(td) / "run_spec.json"
                run_spec.write_text(json.dumps({"strategy_bundle": "bundle.zip"}), encoding="utf-8")

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
                        rc = cli.command_submit(self._args(run_spec=str(run_spec)))

                self.assertEqual(rc, 0)
                files = captured["files"]
                self.assertEqual(files[1][0], "strategy_bundle")
                self.assertEqual(files[1][1], "bundle.zip")
            finally:
                os.chdir(cwd)

    def test_strategy_bundle_and_file_mutually_exclusive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            os.chdir(td)
            try:
                run_spec = Path(td) / "run_spec.json"
                run_spec.write_text(json.dumps({}), encoding="utf-8")
                err = StringIO()
                with patch.object(cli.urllib.request, "urlopen") as mock_urlopen:
                    with redirect_stderr(err):
                        rc = cli.command_submit(
                            self._args(
                                run_spec=str(run_spec),
                                strategy_file="s.py",
                                strategy_bundle="bundle.zip",
                            )
                        )
                self.assertEqual(rc, 1)
                self.assertEqual(mock_urlopen.call_count, 0)
                self.assertIn("mutually exclusive", err.getvalue())
            finally:
                os.chdir(cwd)
