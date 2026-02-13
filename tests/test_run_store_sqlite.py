import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("BACKTEST_API_BASES", "http://dummy")

import app
from services.run_store_sqlite import SqliteRunStore


class TestSqliteRunStore(unittest.TestCase):
    def test_upsert_and_merge_preserves_extra_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "hub.sqlite3"
            store = SqliteRunStore(db_path)

            store.upsert_run(
                "bt_1",
                {
                    "status": "queued",
                    "queued_at": "2026-02-01T00:00:00Z",
                    "required_memory_gb": 4,
                    "custom_field": {"a": 1},
                },
            )
            entry = store.get_run("bt_1")
            self.assertIsNotNone(entry)
            assert entry is not None
            self.assertEqual(entry["status"], "queued")
            self.assertEqual(entry["required_memory_gb"], 4.0)
            self.assertEqual(entry["custom_field"], {"a": 1})
            self.assertIn("created_at", entry)

            # Merge update should not drop queued_at / custom_field.
            store.upsert_run(
                "bt_1",
                {
                    "status": "submitted",
                    "submitted_at": "2026-02-01T00:00:05Z",
                    "backtest_api_base": "http://a",
                },
            )
            entry2 = store.get_run("bt_1")
            self.assertIsNotNone(entry2)
            assert entry2 is not None
            self.assertEqual(entry2["status"], "submitted")
            self.assertEqual(entry2["queued_at"], "2026-02-01T00:00:00Z")
            self.assertEqual(entry2["custom_field"], {"a": 1})
            self.assertEqual(entry2["backtest_api_base"], "http://a")

    def test_list_submitted_ids_filters_range(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = SqliteRunStore(Path(td) / "hub.sqlite3")
            store.upsert_run("a", {"submitted_at": "2026-02-01T00:00:00Z"})
            store.upsert_run("b", {"submitted_at": "2026-02-03T00:00:00Z"})
            store.upsert_run("c", {"submitted_at": "2026-02-05T00:00:00Z"})
            store.upsert_run("queued", {"status": "queued"})

            after_dt = datetime(2026, 2, 2, tzinfo=timezone.utc)
            before_dt = datetime(2026, 2, 4, tzinfo=timezone.utc)
            ids = store.list_submitted_ids(after_dt, before_dt)
            self.assertEqual(ids, ["b"])


class TestMigrationFromLegacyJson(unittest.TestCase):
    def test_app_migrates_json_to_db_and_renames(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "hub.sqlite3"
            mapping_path = base / "run_mapping.json"
            mapping_path.write_text(
                json.dumps(
                    {
                        "bt_1": {
                            "status": "submitted",
                            "submitted_at": "2026-02-01T00:00:00Z",
                            "backtest_api_base": "http://a",
                        },
                        "bt_2": {"status": "queued", "queued_at": "2026-02-02T00:00:00Z"},
                    },
                    ensure_ascii=True,
                ),
                encoding="utf-8",
            )

            # Point app globals to temp paths and force re-init.
            app.HUB_DB_PATH = db_path
            app.RUN_MAPPING_PATH = mapping_path
            app.RUN_STORE = None

            app.maybe_migrate_run_mapping_json()

            self.assertFalse(mapping_path.exists(), "legacy json should be renamed after migration")
            bak = base / "run_mapping.json.bak"
            self.assertTrue(bak.exists(), "expected .bak after migration")

            store = SqliteRunStore(db_path)
            self.assertGreater(store.count_runs(), 0)
            self.assertIsNotNone(store.get_run("bt_1"))


if __name__ == "__main__":
    unittest.main()

