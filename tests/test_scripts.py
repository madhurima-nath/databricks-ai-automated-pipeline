"""
Tests for scripts/run_pipeline.py.

Uses unittest.mock to intercept HTTP calls — no real Databricks connection needed.
Run with: pytest tests/test_scripts.py -v
"""

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock, call, patch

import pytest

# Load scripts as modules without executing their __main__ blocks
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "scripts")


def _load_script(name: str) -> types.ModuleType:
    path = os.path.join(SCRIPTS_DIR, name)
    spec = importlib.util.spec_from_file_location(name.replace(".py", ""), path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


run_pipeline = _load_script("run_pipeline.py")


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        from requests.exceptions import HTTPError
        resp.raise_for_status.side_effect = HTTPError(response=resp)
    return resp


# ---------------------------------------------------------------------------
# run_pipeline: _load_env
# ---------------------------------------------------------------------------

class TestLoadEnv:
    def test_reads_key_value_pairs(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("MYKEY=myvalue\nOTHER=123\n")
        old = os.environ.pop("MYKEY", None)
        run_pipeline._load_env(str(env_file))
        assert os.environ.get("MYKEY") == "myvalue"
        if old is not None:
            os.environ["MYKEY"] = old
        else:
            os.environ.pop("MYKEY", None)

    def test_ignores_comments(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\nFOO=bar\n")
        os.environ.pop("FOO", None)
        run_pipeline._load_env(str(env_file))
        assert os.environ.get("FOO") == "bar"
        os.environ.pop("FOO", None)

    def test_does_not_overwrite_existing(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("EXISTING=new\n")
        os.environ["EXISTING"] = "original"
        run_pipeline._load_env(str(env_file))
        assert os.environ["EXISTING"] == "original"
        os.environ.pop("EXISTING", None)


# ---------------------------------------------------------------------------
# run_pipeline: _db_api (via main)
# ---------------------------------------------------------------------------

class TestRunPipelineSubmit:
    """Verify that main() submits a job and polls correctly."""

    def _make_run_response(self, life_cycle: str, result: str = "") -> dict:
        state = {"life_cycle_state": life_cycle}
        if result:
            state["result_state"] = result
        return {
            "state": state,
            "tasks": [
                {"task_key": "bronze_ingest",   "state": {"life_cycle_state": life_cycle, "result_state": result}, "execution_duration": 10000},
                {"task_key": "silver_transform","state": {"life_cycle_state": life_cycle, "result_state": result}, "execution_duration": 8000},
                {"task_key": "gold_analytics",  "state": {"life_cycle_state": life_cycle, "result_state": result}, "execution_duration": 6000},
            ],
            "execution_duration": 24000,
        }

    def test_success_exits_zero(self):
        with (
            patch.object(sys, "argv", ["run_pipeline.py", "--job-id", "1", "--poll-interval", "0"]),
            patch.dict(os.environ, {"DATABRICKS_HOST": "https://test.databricks.com", "DATABRICKS_TOKEN": "tok"}, clear=False),
            patch("requests.post", return_value=_mock_response({"run_id": 42})),
            patch("requests.get",  return_value=_mock_response(self._make_run_response("TERMINATED", "SUCCESS"))),
            patch("time.sleep"),
            pytest.raises(SystemExit) as exc,
        ):
            run_pipeline.main()
        assert exc.value.code == 0

    def test_failure_exits_one(self):
        with (
            patch.object(sys, "argv", ["run_pipeline.py", "--job-id", "1", "--poll-interval", "0"]),
            patch.dict(os.environ, {"DATABRICKS_HOST": "https://test.databricks.com", "DATABRICKS_TOKEN": "tok"}, clear=False),
            patch("requests.post", return_value=_mock_response({"run_id": 99})),
            patch("requests.get",  return_value=_mock_response(self._make_run_response("TERMINATED", "FAILED"))),
            patch("time.sleep"),
            pytest.raises(SystemExit) as exc,
        ):
            run_pipeline.main()
        assert exc.value.code == 1

    def test_missing_host_exits_one(self):
        env = {k: v for k, v in os.environ.items() if k not in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")}
        with (
            patch.object(sys, "argv", ["run_pipeline.py", "--job-id", "1"]),
            patch.dict(os.environ, env, clear=True),
            pytest.raises(SystemExit) as exc,
        ):
            run_pipeline.main()
        assert exc.value.code == 1

    def test_missing_token_exits_one(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_TOKEN"}
        env["DATABRICKS_HOST"] = "https://test.databricks.com"
        with (
            patch.object(sys, "argv", ["run_pipeline.py", "--job-id", "1"]),
            patch.dict(os.environ, env, clear=True),
            pytest.raises(SystemExit) as exc,
        ):
            run_pipeline.main()
        assert exc.value.code == 1


