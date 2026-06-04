#!/usr/bin/env python3
"""
Download the gold_analytics parquet from Databricks DBFS to data/.

Prerequisite: run notebooks/04_export_parquet.py on Databricks first.
That notebook writes the Gold Delta table to DBFS as a single parquet file.

Usage:
    python scripts/download_gold.py

After this completes, the Streamlit dashboard can be launched locally:
    streamlit run dashboard/app.py
"""

import base64
import os
import sys

import requests


def _load_env(path: str = ".env") -> None:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", path)
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())


DBFS_PATH  = "/FileStore/gold_analytics.parquet"
CHUNK_SIZE = 1024 * 1024  # 1 MB


def main() -> None:
    _load_env()

    host  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env or environment.")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {token}"}
    read_url = f"{host}/api/2.0/dbfs/read"

    local_dir  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
    local_path = os.path.join(local_dir, "gold_analytics.parquet")
    os.makedirs(local_dir, exist_ok=True)

    print(f"Source : {host}{DBFS_PATH}")
    print(f"Target : {local_path}")
    print()

    offset = 0
    chunks: list[bytes] = []

    while True:
        resp = requests.get(
            read_url,
            headers=headers,
            params={"path": DBFS_PATH, "offset": offset, "length": CHUNK_SIZE},
            timeout=60,
        )

        if resp.status_code == 404:
            print(f"ERROR: {DBFS_PATH} not found on DBFS.")
            print("Run notebooks/04_export_parquet.py in Databricks first, then retry.")
            sys.exit(1)

        resp.raise_for_status()
        body  = resp.json()
        chunk = base64.b64decode(body["data"])
        chunks.append(chunk)
        offset += body["bytes_read"]
        print(f"  {offset:>12,} bytes downloaded...", end="\r")

        if body["bytes_read"] < CHUNK_SIZE:
            break

    payload = b"".join(chunks)
    with open(local_path, "wb") as f:
        f.write(payload)

    print(f"\n\nDone. {len(payload):,} bytes saved to {local_path}")
    print("Start the dashboard: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
