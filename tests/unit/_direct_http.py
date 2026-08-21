"""Small helpers for talking to `fake_matterbeam.server` directly over HTTP, bypassing a
real dlt pipeline. The idempotency/ordering mechanics are server-internal -- exercising
them precisely (a specific `_dlt_id`, a specific job_id/seq, a specific chaos crash point)
is far more direct this way than driving a whole pipeline run and hoping dlt's own retry
timing lines up.

Not a test file itself (no `test_` functions) -- imported by ones that need it.
"""

from __future__ import annotations

import json
from typing import Optional

import requests
import zstandard

TOKEN = "fake-token"


def register(base_url: str, pipeline_key: str, dataset_name: str) -> str:
    response = requests.post(
        f"{base_url}/collectors",
        json={"type": "external_dlt", "name": pipeline_key, "config": {"dataset_name": dataset_name}},
        headers={"Authorization": f"Token {TOKEN}"},
    )
    response.raise_for_status()
    return response.json()["id"]


def post_chunk(
    base_url: str,
    pid: str,
    table: str,
    load_id: str,
    job_id: str,
    seq,
    rows: list[dict],
    *,
    chaos_crash_at: Optional[str] = None,
) -> requests.Response:
    """`rows`: wire envelopes, e.g. `{"i": "<_dlt_id>", "v": {...}}`, optionally `"t": True`."""
    lines = [json.dumps(row, separators=(",", ":")).encode("utf8") for row in rows]
    body = zstandard.ZstdCompressor(level=3).compress(b"\n".join(lines))
    headers = {
        "Authorization": f"Token {TOKEN}",
        "Content-Encoding": "zstd",
        "X-MB-Load-Id": load_id,
        "X-MB-Job-Id": job_id,
        "X-MB-Seq": str(seq),
    }
    if chaos_crash_at:
        headers["X-MB-Chaos-Crash-At"] = chaos_crash_at
    return requests.post(f"{base_url}/collectors/{pid}/ingest/{table}:bulk", data=body, headers=headers)


def complete_load(base_url: str, pid: str, load_id: str) -> requests.Response:
    return requests.post(f"{base_url}/collectors/{pid}/job/{load_id}", headers={"Authorization": f"Token {TOKEN}"})
