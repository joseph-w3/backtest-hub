import unittest

from scripts import catalog_controls


class TestCatalogControls(unittest.TestCase):
    def test_parse_catalog_controls_accepts_valid_values(self) -> None:
        parsed = catalog_controls.parse_catalog_controls(
            {
                "prewarm_before_run": True,
                "prewarm_threads": 25,
                "prefetch_backend": "LOCAL-READ",
                "prefetch_ahead_hours": 48,
                "prefetch_max_files_per_batch": 3,
            }
        )
        self.assertEqual(
            parsed,
            {
                "prewarm_before_run": True,
                "prewarm_threads": 25,
                "prefetch_backend": "local-read",
                "prefetch_ahead_hours": 48,
                "prefetch_max_files_per_batch": 3,
            },
        )

    def test_parse_catalog_controls_rejects_unknown_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown keys"):
            catalog_controls.parse_catalog_controls({"oops": True})

    def test_resolve_catalog_controls_defaults(self) -> None:
        controls = catalog_controls.resolve_catalog_controls({})
        self.assertFalse(controls.prewarm_before_run)
        self.assertEqual(
            controls.prewarm_threads, catalog_controls.DEFAULT_CATALOG_PREWARM_THREADS
        )
        self.assertIsNone(controls.prefetch_backend)
        self.assertIsNone(controls.prefetch_ahead_hours)
        self.assertIsNone(controls.prefetch_max_files_per_batch)
