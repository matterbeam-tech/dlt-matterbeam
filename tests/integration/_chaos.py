"""Helpers for chaos tests against a real dev account: talking to the ingest route
directly (precise control over load_id/job_id/seq/chaos-crash-point, faster than driving
a whole pipeline for scenarios that don't need one), and reading back
`process_state.json` -- a plain JSON blob, not internal-format-specific -- to check
server-side counters.

This suite deliberately does NOT read real segments back out of the real coldlog bucket:
doing so would mean decoding the real backend's internal on-disk format, which this
public package's test suite should not depend on even for verification purposes. What
that gives up is checking exact record-level content server-side; response-level fields
(`record_count`, `duplicate`, `segment_key`, `last_record_id`) and `process_state`'s own
counters are what's asserted on instead.

Not a test file itself.
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Optional

import requests
import zstandard


def base_url() -> str:
    return os.environ["MATTERBEAM_BASE_URL"].rstrip("/")


def token() -> str:
    return os.environ["MATTERBEAM_API_TOKEN"]


def _headers(extra: Optional[dict] = None) -> dict:
    headers = {"Authorization": f"Token {token()}"}
    headers.update(extra or {})
    return headers


def register(pipeline_key: str, dataset_name: str) -> str:
    response = requests.post(
        f"{base_url()}/collectors",
        json={"type": "external_dlt", "name": pipeline_key, "config": {"dataset_name": dataset_name}},
        headers=_headers(),
    )
    response.raise_for_status()
    return response.json()["id"]


def post_chunk(
    pid: str,
    table: str,
    load_id: str,
    job_id: str,
    seq,
    rows: list[dict],
    *,
    chaos_crash_at: Optional[str] = None,
    timeout: float = 15,
) -> requests.Response:
    lines = [json.dumps(row, separators=(",", ":")).encode("utf8") for row in rows]
    body = zstandard.ZstdCompressor(level=3).compress(b"\n".join(lines))
    headers = _headers(
        {"Content-Encoding": "zstd", "X-MB-Load-Id": load_id, "X-MB-Job-Id": job_id, "X-MB-Seq": str(seq)}
    )
    if chaos_crash_at:
        headers["X-MB-Chaos-Crash-At"] = chaos_crash_at
    return requests.post(
        f"{base_url()}/collectors/{pid}/ingest/{table}:bulk", data=body, headers=headers, timeout=timeout
    )


def complete_load(pid: str, load_id: str) -> requests.Response:
    return requests.post(f"{base_url()}/collectors/{pid}/job/{load_id}", headers=_headers())


def kick_pid(pid: str) -> requests.Response:
    return requests.post(f"{base_url()}/v2/pids/{pid}/kick", headers=_headers())


def pid_fsm_state(pid: str) -> Optional[str]:
    response = requests.get(f"{base_url()}/v2/pids/{pid}", headers=_headers())
    if response.status_code != 200:
        return None
    return response.json().get("data", {}).get("fsm_state")


def wait_for_fsm_state(pid: str, expected: str, attempts: int = 10, delay: float = 0.3) -> str:
    import time

    last = None
    for _ in range(attempts):
        last = pid_fsm_state(pid)
        if last == expected:
            return last
        time.sleep(delay)
    return last


_REST_API_SRC = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "..", "backend", "workspaces", "api", "rest_api", "src"
)


def force_release_lock(pid: str) -> None:
    """The recovery mechanism that actually works, as opposed to
    `POST /v2/pids/{pid}/kick` -- see `test_kick_pid_is_not_a_safe_recovery_path`. Calls
    `update_pid_for_complete` directly (the exact primitive `release_entry` uses) with no
    `PidAction.RUN` event, which is what makes `kick_pid` unsafe for an `EXTERNAL_DLT` pid:
    that event gets consumed by `process_manager`, which invokes `function_arn` (this
    collector type's own ingest Lambda) with nothing for it to run, and that invocation
    re-claims the entry lock and never releases it.

    Not something a real client can do (it needs the server's own AWS credentials to talk
    to DynamoDB directly) -- a stand-in for the safe admin action that testing shows is
    still missing, used here only so downstream assertions can be tested at all.
    """
    import subprocess

    script = (
        "import logging\n"
        "from beamix.configure import configure\n"
        "from beamix import pid_fsm\n"
        "from beamix.pid_fsm import PidState\n"
        "from matterbeam_shared.service_log import DynamodbServiceLog\n"
        "context = configure()\n"
        "boto3 = context.boto3\n"
        "cfg = context.customer_config\n"
        "client = boto3.client('dynamodb')\n"
        "service_log = DynamodbServiceLog(client, cfg['service_log']['table_name'])\n"
        f"fsm = pid_fsm.get_pid(service_log, {pid!r})\n"
        "pid_fsm.update_pid_for_complete(service_log, fsm['pid'] if 'pid' in fsm else "
        f"{pid!r}, PidState.LISTENING, fsm['update_version'], logging.getLogger())\n"
    )
    env = dict(os.environ)
    env.update(
        AWS_PROFILE="default",
        CUSTOMER="dev",
        AWS_ACCOUNT_ID=os.environ.get("MATTERBEAM_AWS_ACCOUNT_ID", "218354445410"),
    )
    subprocess.run(
        ["uv", "run", "--no-sync", "python", "-c", script],
        cwd=_REST_API_SRC,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def _aws_env() -> dict:
    """Reading the coldlog bucket needs the *assumed-role* profile (`dev`), not the base
    profile the server process uses to assume it -- always overridden, not merely
    defaulted, since the ambient `AWS_PROFILE` in this test's own shell is usually the
    base one (`default`)."""
    env = dict(os.environ)
    env["AWS_PROFILE"] = os.environ.get("MATTERBEAM_AWS_PROFILE", "dev")
    return env


def process_state(bucket: str, pid: str) -> dict:
    """Read `process_state.json` straight from S3 -- there is no route that returns the
    whole thing (`GET /collectors/{pid}/state` deliberately only returns the dlt-state
    subset), and the residual-window counter lives in the part that route doesn't
    expose."""
    import subprocess
    import tempfile

    key = f"process_manager/{pid}/process_state.json"
    with tempfile.NamedTemporaryFile(suffix=".json") as f:
        subprocess.run(
            ["aws", "s3", "cp", f"s3://{bucket}/{key}", f.name], env=_aws_env(), check=True, capture_output=True
        )
        return json.load(open(f.name))
