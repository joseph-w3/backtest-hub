import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import catalog_prefetch


def _write_bytes(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


class _RecordingBackend:
    name = "recording"

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def prefetch(self, paths: list[str]) -> dict:
        self.calls.append(list(paths))
        return {
            "prefetched_files": len(paths),
            "prefetched_bytes": len(paths) * 123,
            "skipped_files": 0,
        }


class TestCatalogPrefetch(unittest.TestCase):
    def test_build_windowed_files_parses_ordered_ranges(self) -> None:
        records = catalog_prefetch.build_windowed_files(
            [
                "/catalog/data/order_book_deltas/BTCUSDT.BINANCE_SPOT/2025-11-11T00-00-00-000000000Z_2025-11-11T23-59-59-000000000Z.parquet",
                "/catalog/data/order_book_deltas/BTCUSDT.BINANCE_SPOT/2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet",
            ]
        )
        self.assertEqual(records[0].instrument_id, "BTCUSDT.BINANCE_SPOT")
        self.assertEqual(records[0].data_type, "order_book_deltas")
        self.assertLess(records[0].start_ns, records[1].start_ns)

    def test_local_file_read_prefetch_backend_reads_files(self) -> None:
        with TemporaryDirectory() as td:
            root = Path(td)
            file_a = root / "a.parquet"
            file_b = root / "b.parquet"
            _write_bytes(file_a, 256)
            _write_bytes(file_b, 512)
            backend = catalog_prefetch.LocalFileReadPrefetchBackend(read_chunk_bytes=64)
            result = backend.prefetch([str(file_a), str(file_b)])
            self.assertEqual(result["prefetched_files"], 2)
            self.assertEqual(result["prefetched_bytes"], 768)
            self.assertEqual(result["skipped_files"], 0)

    def test_build_prefetch_backend_accepts_explicit_mode(self) -> None:
        backend = catalog_prefetch.build_prefetch_backend(mode="off")
        self.assertEqual(backend.name, "disabled")

    def test_replay_prefetch_controller_advances_window_without_storage_coupling(self) -> None:
        backend = _RecordingBackend()
        controller = catalog_prefetch.ReplayPrefetchController(
            files=catalog_prefetch.build_windowed_files(
                [
                    "/catalog/data/order_book_deltas/BTCUSDT.BINANCE_SPOT/2025-11-10T00-00-00-000000000Z_2025-11-10T23-59-59-000000000Z.parquet",
                    "/catalog/data/order_book_deltas/BTCUSDT.BINANCE_SPOT/2025-11-11T00-00-00-000000000Z_2025-11-11T23-59-59-000000000Z.parquet",
                    "/catalog/data/order_book_deltas/BTCUSDT.BINANCE_SPOT/2025-11-15T00-00-00-000000000Z_2025-11-15T23-59-59-000000000Z.parquet",
                ]
            ),
            backend=backend,
            ahead_hours=48,
            max_files_per_batch=2,
        )
        try:
            snapshot = controller.advance(
                catalog_prefetch.time_like_to_ns("2025-11-10T00:00:00.000Z")
            )
            self.assertEqual(snapshot["stage"], "queued")
            deadline = time.time() + 2.0
            while time.time() < deadline:
                snapshot = controller.snapshot()
                if snapshot["completed_files_total"] == 2:
                    break
                time.sleep(0.01)
            self.assertEqual(snapshot["completed_files_total"], 2)
            self.assertEqual(len(backend.calls), 1)
            self.assertEqual(len(backend.calls[0]), 2)
            snapshot = controller.advance(
                catalog_prefetch.time_like_to_ns("2025-11-15T00:00:00.000Z")
            )
            self.assertEqual(snapshot["requested_files_total"], 3)
        finally:
            controller.close()


if __name__ == "__main__":
    unittest.main()
