"""Tests for S3 catalog configuration in run_backtest.py.

Validates:
- S3 mode: correct s3fs params in catalog_fs_storage_options
- S3 mode: virtual_hosted_style_request=false in catalog_fs_rust_storage_options
- Fallback to local catalog when B2 credentials absent
- B2_S3_BUCKET env var used instead of hardcoded bucket in path
- Dual backend: fs_storage_options keys != fs_rust_storage_options keys
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

# We import the helper after it's extracted; initially this will fail (RED).
from scripts.catalog_config import build_catalog_config


class TestS3CatalogConfig:
    """Tests for build_catalog_config()."""

    _S3_ENV = {
        "B2_KEY_ID": "test-key-id",
        "B2_APPLICATION_KEY": "test-app-key",
        "B2_S3_ENDPOINT": "https://s3.us-west-004.backblazeb2.com",
        "B2_S3_REGION": "us-west-004",
        "B2_S3_BUCKET": "my-bucket",
    }

    # ── S3 mode basic params ──────────────────────────────────────

    def test_s3_mode_fs_protocol_is_s3(self):
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        assert cfg["catalog_fs_protocol"] == "s3"

    def test_s3_mode_fs_storage_options_has_s3fs_keys(self):
        """s3fs backend uses 'key', 'secret', 'endpoint_url', 'client_kwargs'."""
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        opts = cfg["catalog_fs_storage_options"]
        assert opts["key"] == "test-key-id"
        assert opts["secret"] == "test-app-key"
        assert opts["endpoint_url"] == "https://s3.us-west-004.backblazeb2.com"
        assert opts["client_kwargs"] == {"region_name": "us-west-004"}

    def test_s3_mode_rust_storage_options_has_object_store_keys(self):
        """Rust object_store uses 'access_key_id', 'secret_access_key', 'endpoint_url', 'region'."""
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        opts = cfg["catalog_fs_rust_storage_options"]
        assert opts["access_key_id"] == "test-key-id"
        assert opts["secret_access_key"] == "test-app-key"
        assert opts["endpoint_url"] == "https://s3.us-west-004.backblazeb2.com"
        assert opts["region"] == "us-west-004"

    # ── BUG FIX: virtual_hosted_style_request ─────────────────────

    def test_s3_mode_rust_has_virtual_hosted_style_false(self):
        """B2 requires path-style requests; object_store must set this."""
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        opts = cfg["catalog_fs_rust_storage_options"]
        assert opts.get("virtual_hosted_style_request") == "false"

    # ── BUG FIX: bucket not hardcoded in CATALOG_PATH ─────────────

    def test_s3_mode_uses_b2_s3_bucket_env(self):
        """Catalog path should be 'bucket/prefix', bucket from B2_S3_BUCKET."""
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        assert cfg["catalog_path"].startswith("my-bucket/")

    def test_s3_mode_default_catalog_prefix(self):
        """Default catalog prefix (after bucket) should be 'backtest/catalog'."""
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        assert cfg["catalog_path"] == "my-bucket/backtest/catalog"

    def test_s3_mode_custom_catalog_prefix(self):
        """CATALOG_PREFIX env overrides the path portion after the bucket."""
        env = {**self._S3_ENV, "CATALOG_PREFIX": "custom/path"}
        with patch.dict(os.environ, env, clear=False):
            cfg = build_catalog_config()
        assert cfg["catalog_path"] == "my-bucket/custom/path"

    def test_s3_mode_catalog_path_no_leading_slash(self):
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        assert not cfg["catalog_path"].startswith("/")

    # ── Dual backend: param names differ ──────────────────────────

    def test_dual_backend_keys_differ(self):
        """s3fs and object_store use different key names for credentials."""
        with patch.dict(os.environ, self._S3_ENV, clear=False):
            cfg = build_catalog_config()
        py_keys = set(cfg["catalog_fs_storage_options"].keys())
        rs_keys = set(cfg["catalog_fs_rust_storage_options"].keys())
        # They must NOT be identical (different param conventions)
        assert py_keys != rs_keys

    # ── Local fallback (no B2 creds) ──────────────────────────────

    def test_local_fallback_when_no_b2_creds(self):
        """Without B2_KEY_ID, should fall back to local catalog."""
        env_clean = {k: v for k, v in os.environ.items() if not k.startswith("B2_")}
        with patch.dict(os.environ, env_clean, clear=True):
            cfg = build_catalog_config()
        assert cfg["catalog_fs_protocol"] is None
        assert cfg["catalog_fs_storage_options"] is None
        assert cfg["catalog_fs_rust_storage_options"] is None

    def test_local_fallback_default_path(self):
        env_clean = {k: v for k, v in os.environ.items() if not k.startswith("B2_")}
        with patch.dict(os.environ, env_clean, clear=True):
            cfg = build_catalog_config()
        assert cfg["catalog_path"] == "/opt/catalog"

    def test_local_fallback_custom_path(self):
        env_clean = {k: v for k, v in os.environ.items() if not k.startswith("B2_")}
        env_clean["CATALOG_PATH"] = "/mnt/data/catalog"
        with patch.dict(os.environ, env_clean, clear=True):
            cfg = build_catalog_config()
        assert cfg["catalog_path"] == "/mnt/data/catalog"
