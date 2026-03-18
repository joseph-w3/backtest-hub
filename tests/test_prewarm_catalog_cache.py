import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import prewarm_catalog_cache


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


class TestPrewarmCatalogCache(unittest.TestCase):
    def test_build_manifest_includes_expected_spot_and_futures_files(self) -> None:
        with TemporaryDirectory() as td:
            catalog_root = Path(td) / "catalog"
            _touch(
                catalog_root
                / "data"
                / "currency_pair"
                / "BTCUSDT.BINANCE_SPOT"
                / "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "order_book_deltas"
                / "BTCUSDT.BINANCE_SPOT"
                / "2025-11-09T00-00-00-000000000Z_2025-11-09T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "order_book_deltas"
                / "BTCUSDT.BINANCE_SPOT"
                / "2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "trade_tick"
                / "BTCUSDT.BINANCE_SPOT"
                / "2025-11-11T00-00-00-000000000Z_2025-11-11T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "crypto_perpetual"
                / "BTCUSDT-PERP.BINANCE_FUTURES"
                / "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "order_book_deltas"
                / "BTCUSDT-PERP.BINANCE_FUTURES"
                / "2025-11-10T12-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "trade_tick"
                / "BTCUSDT-PERP.BINANCE_FUTURES"
                / "2025-11-11T00-00-00-000000000Z_2025-11-11T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "funding_rate_update"
                / "BTCUSDT-PERP.BINANCE_FUTURES"
                / "2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "mark_price_update"
                / "BTCUSDT-PERP.BINANCE_FUTURES"
                / "2025-11-12T00-00-00-000000000Z_2025-11-12T23-59-59-000000000Z.parquet"
            )

            result = prewarm_catalog_cache.build_manifest(
                run_spec={
                    "symbols": ["BTCUSDT", "BTCUSDT-PERP"],
                    "start": "2025-11-10T00:00:00.000Z",
                    "end": "2025-11-12T00:00:00.000Z",
                },
                catalog_root=catalog_root,
            )

            self.assertEqual(
                [path.relative_to(catalog_root).as_posix() for path in result.paths],
                [
                    "data/crypto_perpetual/BTCUSDT-PERP.BINANCE_FUTURES/1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet",
                    "data/currency_pair/BTCUSDT.BINANCE_SPOT/1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet",
                    "data/funding_rate_update/BTCUSDT-PERP.BINANCE_FUTURES/2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet",
                    "data/mark_price_update/BTCUSDT-PERP.BINANCE_FUTURES/2025-11-12T00-00-00-000000000Z_2025-11-12T23-59-59-000000000Z.parquet",
                    "data/order_book_deltas/BTCUSDT-PERP.BINANCE_FUTURES/2025-11-10T12-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet",
                    "data/order_book_deltas/BTCUSDT.BINANCE_SPOT/2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet",
                    "data/trade_tick/BTCUSDT-PERP.BINANCE_FUTURES/2025-11-11T00-00-00-000000000Z_2025-11-11T23-59-59-000000000Z.parquet",
                    "data/trade_tick/BTCUSDT.BINANCE_SPOT/2025-11-11T00-00-00-000000000Z_2025-11-11T23-59-59-000000000Z.parquet",
                ],
            )
            self.assertEqual(
                result.category_counts,
                {
                    "crypto_perpetual": 1,
                    "currency_pair": 1,
                    "funding_rate_update": 1,
                    "mark_price_update": 1,
                    "order_book_deltas": 2,
                    "trade_tick": 2,
                },
            )

    def test_build_manifest_skips_trade_ticks_when_disabled(self) -> None:
        with TemporaryDirectory() as td:
            catalog_root = Path(td) / "catalog"
            _touch(
                catalog_root
                / "data"
                / "currency_pair"
                / "ETHUSDT.BINANCE_SPOT"
                / "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "order_book_deltas"
                / "ETHUSDT.BINANCE_SPOT"
                / "2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "trade_tick"
                / "ETHUSDT.BINANCE_SPOT"
                / "2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet"
            )

            result = prewarm_catalog_cache.build_manifest(
                run_spec={
                    "symbols": ["ETHUSDT"],
                    "start": "2025-11-10T00:00:00.000Z",
                    "end": "2025-11-10T23:59:59.000Z",
                    "load_trade_ticks": False,
                },
                catalog_root=catalog_root,
            )

            self.assertEqual(
                [path.relative_to(catalog_root).as_posix() for path in result.paths],
                [
                    "data/currency_pair/ETHUSDT.BINANCE_SPOT/1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet",
                    "data/order_book_deltas/ETHUSDT.BINANCE_SPOT/2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet",
                ],
            )
            self.assertNotIn("trade_tick", result.category_counts)

    def test_main_writes_manifest_and_summary(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            catalog_root = root / "catalog"
            _touch(
                catalog_root
                / "data"
                / "currency_pair"
                / "SOLUSDT.BINANCE_SPOT"
                / "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
            )
            _touch(
                catalog_root
                / "data"
                / "order_book_deltas"
                / "SOLUSDT.BINANCE_SPOT"
                / "bad-name.parquet"
            )
            run_spec_path = root / "run_spec.json"
            run_spec_path.write_text(
                json.dumps(
                    {
                        "symbols": ["SOLUSDT"],
                        "start": "2025-11-10T00:00:00.000Z",
                        "end": "2025-11-10T01:00:00.000Z",
                        "load_trade_ticks": False,
                    }
                ),
                encoding="utf-8",
            )
            manifest_path = root / "manifest.txt"

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                rc = prewarm_catalog_cache.main(
                    [
                        "--run-spec",
                        str(run_spec_path),
                        "--catalog-root",
                        str(catalog_root),
                        "--manifest",
                        str(manifest_path),
                    ]
                )

            self.assertEqual(rc, 0)
            self.assertEqual(
                manifest_path.read_text(encoding="utf-8").splitlines(),
                [
                    str(
                        catalog_root
                        / "data"
                        / "currency_pair"
                        / "SOLUSDT.BINANCE_SPOT"
                        / "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
                    ),
                    str(
                        catalog_root
                        / "data"
                        / "order_book_deltas"
                        / "SOLUSDT.BINANCE_SPOT"
                        / "bad-name.parquet"
                    ),
                ],
            )
            summary = json.loads(stdout.getvalue())
            self.assertEqual(summary["file_count"], 2)
            self.assertEqual(summary["manifest_path"], str(manifest_path.resolve()))
            self.assertEqual(
                summary["category_counts"],
                {"currency_pair": 1, "order_book_deltas": 1},
            )


if __name__ == "__main__":
    unittest.main()
