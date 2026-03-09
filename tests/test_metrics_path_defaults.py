from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]


def _extract(pattern: str, text: str) -> str:
    match = re.search(pattern, text)
    assert match is not None
    return match.group(1)


def test_compose_default_metrics_path_matches_app_default() -> None:
    app_text = (REPO_ROOT / "app.py").read_text()
    compose_text = (REPO_ROOT / "docker-compose.yml").read_text()

    app_default = _extract(
        r'BACKTEST_METRICS_PATH\s*=\s*env_or_default\("BACKTEST_METRICS_PATH",\s*"([^"]+)"\)',
        app_text,
    )
    compose_default = _extract(
        r"BACKTEST_METRICS_PATH:\s*\$\{BACKTEST_METRICS_PATH:-([^}]+)\}",
        compose_text,
    )

    assert app_default == "v1/system/metrics"
    assert compose_default == app_default
