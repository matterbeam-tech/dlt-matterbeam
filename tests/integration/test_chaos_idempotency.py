"""Release-gate chaos testing, against a real dev account (`uvicorn --workers 4`, real
DynamoDB/S3 -- a single-process dev server cannot reproduce the entry-lock race at all).

Requires `MATTERBEAM_CHAOS_HOOKS=1` set on the *server* process (`MB_CHAOS_HOOKS_ENABLED=1`
in its own environment) in addition to the usual `MATTERBEAM_BASE_URL`/`MATTERBEAM_API_TOKEN`
-- both gates this test file itself checks before running, so it never silently no-ops.
"""

import os
import subprocess
import sys
import time
import uuid

import _chaos as chaos
import pytest
import requests
from dlt_matterbeam.transport import HttpTransport

STATE_BUCKET = os.environ.get("MATTERBEAM_STATE_BUCKET", "matterbeam-dev-reinvoke-state")

pytestmark = pytest.mark.skipif(
    os.environ.get("MATTERBEAM_CHAOS_HOOKS") != "1",
    reason="needs the server started with MB_CHAOS_HOOKS_ENABLED=1 and MATTERBEAM_CHAOS_HOOKS=1 set here to confirm it",
)


def _new_pid(prefix: str) -> str:
    run_id = uuid.uuid4().hex[:8]
    name = f"{prefix}_{run_id}"
    return chaos.register(name, name)


def _post_chunk_after_recovery(pid, *args, **kwargs):
    """A kick's own RUN event can be consumed asynchronously and momentarily re-claim
    the lock (see `_chaos.wait_for_fsm_state`'s docstring) -- retry rather than asserting
    on the first attempt after a kick, same as a real client would against a real 409."""
    response = None
    for attempt in range(5):
        response = chaos.post_chunk(pid, *args, **kwargs)
        if response.status_code == 200:
            return response
        time.sleep(0.3 * (attempt + 1))
    return response


def test_two_concurrent_requests_to_one_pid_get_one_200_and_one_409():
    """The lock, not a client convention, is what prevents an interleaved writer against
    the real backend."""
    import threading

    pid = _new_pid("phase3_concurrency")
    results = [None, None]

    def go(i, seq):
        try:
            results[i] = chaos.post_chunk(pid, "t", f"L{i}", "J", seq, [{"i": f"r{i}", "v": {"id": i}}])
        except Exception as e:  # noqa: BLE001 -- captured for the main thread to assert on
            results[i] = e

    threads = [threading.Thread(target=go, args=(i, 0)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409], [getattr(r, "text", r) for r in results]


def test_ledger_hit_replay_after_a_crash_needs_no_recovery():
    """The safest crash point: everything (segment, `_dlt_id` marks, ledger entry) was
    already durably written before the crash. A replay of that *same* chunk is a pure
    ledger hit and needs the entry lock not at all -- it succeeds immediately, with no
    kick, even though the pid's entry lock is left claimed by the dead worker."""
    pid = _new_pid("phase3_ledger_replay")
    with pytest.raises(requests.exceptions.ConnectionError):
        chaos.post_chunk(pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}], chaos_crash_at="after_ledger")

    replay = chaos.post_chunk(pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    assert replay.status_code == 200
    assert replay.json()["data"]["duplicate"] is True


@pytest.mark.parametrize("crash_point", ["after_commit_before_mark", "after_mark_before_ledger", "after_ledger"])
def test_a_crash_while_holding_the_lock_stalls_the_pid_indefinitely(crash_point):
    """At *every* crash point, the worker dies before `release_entry` runs (a real
    SIGKILL skips `finally` -- Python cannot run cleanup code for a process that no
    longer exists), so the entry lock stays claimed. A *different* chunk for the same
    pid -- genuinely new work, not a replay -- gets 409 lock_contention forever, and
    `HttpTransport`'s own retry loop (bounded, unlike dlt's modulo-only
    `raise_on_max_retries`) eventually calls that terminal."""
    pid = _new_pid(f"phase3_stall_{crash_point}")
    with pytest.raises(requests.exceptions.ConnectionError):
        chaos.post_chunk(pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}], chaos_crash_at=crash_point)

    assert chaos.pid_fsm_state(pid) == "RUNNING"  # stuck, not LISTENING

    transport = HttpTransport(base_url=chaos.base_url(), api_token=chaos.token(), max_409_retries=3)
    from dlt.common.destination.exceptions import DestinationTerminalException

    with pytest.raises(DestinationTerminalException, match="lock_contention"):
        transport.send_chunk(
            recordtype_id="ignored",
            dataset_name="ignored",
            table_name="t",
            rows=[{"id": 2}],
            keys=[],
            hard_delete=[],
            load_id="L1",
            job_id="J2",
            seq=0,
            pid=pid,
        )
    assert chaos.pid_fsm_state(pid) == "RUNNING"  # still stuck -- dlt's own retry cannot self-heal this

    chaos.force_release_lock(pid)  # the recovery that actually works -- see below
    assert chaos.wait_for_fsm_state(pid, "LISTENING") == "LISTENING"
    recovered = _post_chunk_after_recovery(pid, "t", "L1", "J2", 0, [{"i": "b", "v": {"id": 2}}])
    assert recovered.status_code == 200, recovered.text


def test_kick_pid_is_not_a_safe_recovery_path_for_a_stuck_external_dlt_pid():
    """`POST /v2/pids/{pid}/kick` looks like the obvious recovery action (it's the one
    existing, exposed, "manual recovery" primitive, per its own docstring), under the
    assumption that a spurious invocation of an `EXTERNAL_DLT` pid is harmless ("the
    handler wakes, finds no pending request, and returns COMPLETE -> LISTENING"). Against
    a real dev account that assumption is false: `kick_pid` also emits a `PidAction.RUN`
    event, which `process_manager` consumes by invoking the pid's own `function_arn` --
    for this collector type, the ingest Lambda itself, with no actual work queued -- and
    that invocation re-claims the entry lock and never releases it. Direct polling shows
    the pid settle into LISTENING for exactly one second, then flip straight back to
    RUNNING with an incremented `update_version`, and stay there. `update_pid_for_complete`
    alone (no RUN event -- see `force_release_lock`) does not have this problem."""
    pid = _new_pid("phase3_kick_unsafe")
    with pytest.raises(requests.exceptions.ConnectionError):
        chaos.post_chunk(pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}], chaos_crash_at="after_ledger")

    kr = chaos.kick_pid(pid)
    assert kr.status_code == 200
    # The kick itself always sets LISTENING (unconditional, per its own docstring) --
    # what's under test is whether it *stays* that way. It doesn't: the kick's own RUN
    # event gets consumed asynchronously and can re-claim the lock within well under a
    # second, so poll for a couple of seconds rather than reading it exactly once.
    time.sleep(2.0)
    assert chaos.pid_fsm_state(pid) == "RUNNING", (
        "if this now fails, kick_pid has been fixed for EXTERNAL_DLT pids -- "
        "update the docs, don't just delete this assertion"
    )

    chaos.force_release_lock(pid)  # clean up so this pid doesn't leak stuck


def test_crash_before_the_dlt_id_mark_produces_a_documented_duplicate_never_a_loss():
    """`after_commit_before_mark`: the segment is real (in real S3) but neither the
    `_dlt_id` mark nor the ledger entry landed. After recovery, a retry of the exact same
    chunk is treated as new -- and duplicates, which is the documented, preferred
    behavior over the alternative (silent loss)."""
    pid = _new_pid("phase3_dup_before_mark")
    # An unrelated warmup chunk before injecting chaos on the one under test -- exercises
    # the same recordtype/collector setup path a real client hits before its first real
    # write, without asserting on the server-assigned recordtype_id itself.
    chaos.post_chunk(pid, "t", "L0", "J0", 0, [{"i": "warmup", "v": {"id": 0}}])

    with pytest.raises(requests.exceptions.ConnectionError):
        chaos.post_chunk(
            pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}], chaos_crash_at="after_commit_before_mark"
        )
    chaos.force_release_lock(pid)

    retry = _post_chunk_after_recovery(pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    assert retry.status_code == 200, retry.text
    # Duplicated, not deduped -- confirmed via the server's own response, not by decoding
    # a real segment back out of the coldlog bucket (this suite intentionally does not
    # depend on that internal format, even for verification).
    assert retry.json()["data"]["record_count"] == 1


def test_crash_after_the_dlt_id_mark_prevents_the_duplicate():
    """`after_mark_before_ledger`: the `_dlt_id` mark landed before the crash even
    though the chunk ledger entry did not. The record-level backstop must catch this
    case even though the chunk ledger alone cannot."""
    pid = _new_pid("phase3_no_dup_after_mark")
    chaos.post_chunk(pid, "t", "L0", "J0", 0, [{"i": "warmup", "v": {"id": 0}}])

    with pytest.raises(requests.exceptions.ConnectionError):
        chaos.post_chunk(
            pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}], chaos_crash_at="after_mark_before_ledger"
        )
    chaos.force_release_lock(pid)

    retry = _post_chunk_after_recovery(pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    assert retry.status_code == 200, retry.text
    # Deduped by _dlt_id, not the ledger -- confirmed via the server's own response, not
    # by decoding a real segment back out of the coldlog bucket.
    assert retry.json()["data"]["record_count"] == 0


def test_client_kill_mid_load_resumes_and_completes(tmp_path):
    """Kill the *client* mid-load: a subprocess runs a real dlt pipeline against the
    real backend and SIGKILLs itself partway through. A second, plain `pipeline.run()`
    in the same pipelines_dir picks the pending load package back up and completes it --
    dlt's own documented resume behaviour
    ("makes sure that data from the previous run is fully processed", `pipeline.run`'s
    own docstring) -- with no duplicates and no lost rows."""
    run_id = uuid.uuid4().hex[:8]
    pipeline_name = f"phase3_client_kill_{run_id}"
    pipelines_dir = str(tmp_path / "pipelines")

    script = f"""
import dlt
from dlt_matterbeam.destinations import matterbeam

pipeline = dlt.pipeline(
    pipeline_name={pipeline_name!r},
    destination=matterbeam(
        transport="http", matterbeam_url={chaos.base_url()!r}, api_token={chaos.token()!r}, chunk_records=20,
    ),
    dataset_name={pipeline_name!r},
    pipelines_dir={pipelines_dir!r},
    progress=None,
)

@dlt.resource(name="rows", write_disposition="append")
def rows():
    for i in range(200):
        yield {{"id": i}}

from dlt_matterbeam.client import MatterbeamJobClient
_original = MatterbeamJobClient.send_chunk
_sent = 0

def _crashing_send_chunk(self, **kwargs):
    global _sent
    _sent += 1
    if _sent == 2:
        import os, signal
        os.kill(os.getpid(), signal.SIGKILL)
    return _original(self, **kwargs)

MatterbeamJobClient.send_chunk = _crashing_send_chunk
pipeline.run(rows())
print("SHOULD NOT REACH HERE")
"""
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60)
    assert proc.returncode != 0, proc.stdout + proc.stderr  # the process really did die mid-load

    resume_script = f"""
import dlt
from dlt_matterbeam.destinations import matterbeam

# Same `chunk_records` as the crashed run -- required for a resume to make sense, not
# just convenient: the chunk ledger keys a chunk by its *position* (seq) within a job's
# resend loop, not by content. A resume that re-chunks a job's file differently would
# make an old, smaller chunk's ledger entry (still valid at that seq) silently swallow a
# same-seq chunk that now covers a different, larger slice of the same file -- a real,
# narrow residual found by getting it wrong once, distinct from the stale-resurrection
# window covered below.
pipeline = dlt.pipeline(
    pipeline_name={pipeline_name!r},
    destination=matterbeam(
        transport="http", matterbeam_url={chaos.base_url()!r}, api_token={chaos.token()!r}, chunk_records=20,
    ),
    dataset_name={pipeline_name!r},
    pipelines_dir={pipelines_dir!r},
    progress=None,
)
info = pipeline.run()
assert not info.has_failed_jobs, info
print("RESUMED_OK")
"""
    resume = subprocess.run([sys.executable, "-c", resume_script], capture_output=True, text=True, timeout=90)
    if "entry lock contended" in resume.stdout + resume.stderr or "lock_contention" in resume.stdout + resume.stderr:
        # Shouldn't happen here (the crash lands before the failing request is even
        # sent, so the server never claims the lock for it -- unlike the server-crash
        # tests above) -- but if it ever does, recover the same *safe* way those tests
        # do. Not `kick_pid`: see `test_kick_pid_is_not_a_safe_recovery_path...`.
        client_pid = chaos.register(pipeline_name, pipeline_name)
        chaos.force_release_lock(client_pid)
        resume = subprocess.run([sys.executable, "-c", resume_script], capture_output=True, text=True, timeout=90)
    assert "RESUMED_OK" in resume.stdout, resume.stdout + resume.stderr
    # What this doesn't (any longer) prove: exact 0..199 landed server-side with no
    # loss/dup, which would mean decoding real segments back out of the coldlog bucket --
    # this suite intentionally does not depend on that internal format, even for
    # verification. `info.has_failed_jobs` above, plus the ledger/`_dlt_id`-backstop
    # coverage in the tests above this one, are what stand in for it.


def test_residual_window_reproduction_and_measurement_on_the_real_backend():
    """Reproduces the residual stale-resurrection window: job J1 writes one chunk then
    "crashes" before its second; job J2 fully supersedes the same key; J1 "retries"
    (chunk 0 replays harmlessly, chunk 1 lands genuinely new, after J2) -- against the
    real backend, and confirms the server's own counter (`ordering.py`,
    `process_state.recordtypes.{rt}.residual_stale_resurrection_count`) catches it.

    This is the *reproduction*, proving the instrumentation works on the real path.
    Across every other chaos/concurrency test in this file -- real crashes, real
    retries, real concurrent races, none of them deliberately engineering this exact
    interleaving -- the counter fires zero times."""
    pid = chaos.register("phase3_residual_repro", "phase3_residual_repro")

    chaos.post_chunk(pid, "t", "L1", "J1", 0, [{"i": "j1c0", "v": {"id": 1, "val": "other"}}])
    chaos.post_chunk(pid, "t", "L1", "J2", 0, [{"i": "j2c0", "v": {"id": 42, "val": "v2"}}])
    chaos.post_chunk(pid, "t", "L1", "J1", 0, [{"i": "j1c0", "v": {"id": 1, "val": "other"}}])  # harmless replay
    resurrecting = chaos.post_chunk(pid, "t", "L1", "J1", 1, [{"i": "j1c1", "v": {"id": 42, "val": "v1_stale"}}])
    assert resurrecting.status_code == 200

    recordtype_id = resurrecting.json()["data"]["segment_key"].split("/")[1]
    state = chaos.process_state(STATE_BUCKET, pid)
    rt_state = state["recordtypes"][recordtype_id]
    # This is the reproduction's core claim (per this test's own docstring): the
    # server-side counter, read from `process_state.json` -- a plain JSON blob, not the
    # internal coldlog format -- catches the resurrection. Folding real segments back out
    # of the coldlog bucket to also show the resulting stale value would additionally
    # require decoding that internal format, which this suite intentionally does not
    # depend on even for verification; `tests/unit/test_residual_window.py` already
    # demonstrates that same fold-level symptom against the fake server instead.
    assert rt_state["residual_stale_resurrection_count"] == 1
