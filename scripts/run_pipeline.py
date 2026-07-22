#!/usr/bin/env python3
"""
Submit the Databricks pipeline job and track it to completion.

Usage:
    python scripts/run_pipeline.py --job-id 12345
    python scripts/run_pipeline.py --job-id 12345 --poll-interval 60

Reads DATABRICKS_HOST and DATABRICKS_TOKEN from .env or environment variables.
Exits with code 0 on SUCCESS, 1 on any failure or timeout.
"""

import argparse
import base64
import datetime
import json
import os
import sys
import time

import requests


def _load_env(path: str = ".env") -> None:
    """Read KEY=VALUE pairs from .env into os.environ without overwriting."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", path)
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())


def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def _get(url: str, headers: dict, params: dict = None) -> dict:
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _post(url: str, headers: dict, payload: dict) -> dict:
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

RESULT_EMOJI = {
    "SUCCESS":       "✅",
    "FAILED":        "❌",
    "TIMEDOUT":      "⏱",
    "CANCELED":      "🚫",
    "INTERNAL_ERROR":"💥",
    "SKIPPED":       "⏭",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trigger and track a Databricks pipeline job."
    )
    parser.add_argument(
        "--job-id", required=True, type=int,
        help="Databricks job ID to run (from jobs/pipeline_job.json after registration)"
    )
    parser.add_argument(
        "--poll-interval", type=int, default=30,
        help="Seconds between status polls (default: 30)"
    )
    args = parser.parse_args()

    _load_env()

    host  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if not host:
        print("ERROR: DATABRICKS_HOST not set. Add it to .env or export it.")
        sys.exit(1)
    if not token:
        print("ERROR: DATABRICKS_TOKEN not set. Add it to .env or export it.")
        sys.exit(1)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    base = f"{host}/api/2.1/jobs"

    # -----------------------------------------------------------------------
    # Submit run
    # -----------------------------------------------------------------------
    print(f"[{_ts()}] Submitting job {args.job_id}...")
    data = _post(f"{base}/run-now", headers, {"job_id": args.job_id})
    run_id = data["run_id"]
    run_url = f"{host}/#job/{args.job_id}/run/{run_id}"
    print(f"[{_ts()}] Run ID  : {run_id}")
    print(f"[{_ts()}] Track at: {run_url}")
    print()

    # -----------------------------------------------------------------------
    # Poll until terminal
    # -----------------------------------------------------------------------
    life_cycle = "PENDING"

    while life_cycle not in TERMINAL_STATES:
        time.sleep(args.poll_interval)

        run = _get(f"{base}/runs/get", headers, {"run_id": run_id})
        state = run["state"]
        life_cycle    = state["life_cycle_state"]
        result_state  = state.get("result_state", "")
        state_message = state.get("state_message", "")

        # Task-level status line
        tasks = run.get("tasks", [])
        task_parts = []
        for t in tasks:
            ts_lc = t["state"]["life_cycle_state"]
            ts_rs = t["state"].get("result_state", "")
            emoji = RESULT_EMOJI.get(ts_rs, "⏳")
            task_parts.append(f"{emoji} {t['task_key']}: {ts_lc}{f'/{ts_rs}' if ts_rs else ''}")

        status_line = f"[{_ts()}] {life_cycle}{f'/{result_state}' if result_state else ''}"
        if state_message:
            status_line += f" — {state_message}"
        print(status_line)
        if task_parts:
            print("         " + " | ".join(task_parts))

    # -----------------------------------------------------------------------
    # Final summary
    # -----------------------------------------------------------------------
    result_state = run["state"].get("result_state", "UNKNOWN")
    duration_ms  = run.get("execution_duration", 0)
    duration_s   = duration_ms // 1000
    emoji        = RESULT_EMOJI.get(result_state, "❓")

    print()
    print(f"[{_ts()}] {emoji} Pipeline {result_state} in {duration_s}s")

    if tasks:
        print()
        print("Task results:")
        for t in tasks:
            rs   = t["state"].get("result_state", "—")
            em   = RESULT_EMOJI.get(rs, "❓")
            dur  = t.get("execution_duration", 0) // 1000
            print(f"  {em}  {t['task_key']:30s} {rs:12s}  {dur}s")

    print()
    if result_state == "SUCCESS":
        print(f"[{_ts()}] All tasks completed. financial_sas_project.default.gold_analytics is ready.")
        print(f"[{_ts()}] The Streamlit dashboard reads this table directly via the SQL connector —")
        print(f"[{_ts()}] no export step needed.")
        sys.exit(0)
    else:
        print(f"[{_ts()}] Run details: {run_url}")
        sys.exit(1)


if __name__ == "__main__":
    main()
