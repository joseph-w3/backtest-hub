"""S3 / local catalog configuration for backtest runs.

Reads environment variables and returns a config dict suitable for
``ParquetDataCatalog`` construction.  Kept in its own module so it can
be unit-tested without pulling in the heavy *quant_trade_v1* stack.

NOTE: A copy of build_catalog_config() lives in run_backtest.py — keep in sync.
"""

from __future__ import annotations

import os
from pathlib import Path


def build_catalog_config() -> dict:
    """Return catalog configuration derived from environment variables.

    When ``B2_KEY_ID`` **and** ``B2_APPLICATION_KEY`` are set the function
    returns S3-backed configuration (protocol ``"s3"``).  Otherwise it
    falls back to a local filesystem path.

    Returns
    -------
    dict
        Keys: ``catalog_path``, ``catalog_fs_protocol``,
        ``catalog_fs_storage_options``, ``catalog_fs_rust_storage_options``.
    """
    b2_key_id = os.environ.get("B2_KEY_ID")
    b2_app_key = os.environ.get("B2_APPLICATION_KEY")

    if b2_key_id and b2_app_key:
        endpoint = os.environ.get(
            "B2_S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com"
        )
        region = os.environ.get("B2_S3_REGION", "us-west-004")
        bucket = os.environ.get("B2_S3_BUCKET", "trade-data")
        catalog_prefix = os.environ.get("CATALOG_PREFIX", "backtest/catalog")

        catalog_fs_protocol = "s3"

        # Python (s3fs) backend
        catalog_fs_storage_options = {
            "endpoint_url": endpoint,
            "key": b2_key_id,
            "secret": b2_app_key,
            "client_kwargs": {"region_name": region},
        }

        # Rust (object_store) backend
        catalog_fs_rust_storage_options = {
            "endpoint_url": endpoint,
            "access_key_id": b2_key_id,
            "secret_access_key": b2_app_key,
            "region": region,
            "virtual_hosted_style_request": "false",
        }

        catalog_path = f"{bucket}/{catalog_prefix}"
    else:
        catalog_fs_protocol = None
        catalog_fs_storage_options = None
        catalog_fs_rust_storage_options = None
        catalog_path = Path(
            os.environ.get("CATALOG_PATH", "/opt/catalog")
        ).expanduser().as_posix()

    return {
        "catalog_path": catalog_path,
        "catalog_fs_protocol": catalog_fs_protocol,
        "catalog_fs_storage_options": catalog_fs_storage_options,
        "catalog_fs_rust_storage_options": catalog_fs_rust_storage_options,
    }
