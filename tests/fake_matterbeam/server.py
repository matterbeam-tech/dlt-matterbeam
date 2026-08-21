"""A stand-in for the server side of the ingest API: the bulk ingest route, the
HTTP-driven FSM entry path, registration, and the dlt-state route.

Segment storage is `segment_store.py`, an invented, plain-JSON format local to this test
double -- deliberately not a reimplementation of the real backend's internal coldlog wire
format; nothing here needs byte parity with anything real, only the ordering/dedup/clamp
*behaviour* it's meant to model.

It is NOT a Matterbeam simulator. It reimplements only the parts that behaviour depends
on, with no Matterbeam imports, and deliberately omits a fold-key declaration route (out
of scope for this project, dropped rather than stood in for):

  * per-pid FSM entry lock as a read-then-CAS pair on (update_version, invoke_semaphore),
    with two distinguishable 409 reasons: "lock_contention" (retryable) and "load_closed"
    / "paused" (terminal)
  * one segment per request, server-allocated record_ids, one commit-equivalent write
  * server-side `mb.metadata` stamping incl. is_tombstone and source_record_id/source_load_id
  * per-request writer_id allocation and a high-water clamp on begin_timestamp kept in
    the pid's process_state
  * (load_id, job_id, seq) idempotency ledger, written strictly after the commit, plus
    the record-level `_dlt_id` backstop that must run on *every* chunk (not only once a
    ledger entry has expired)
  * `complete_load` closes a load; further chunks bearing its load_id are rejected
  * process_state as the only durable store: dlt state and cursors
  * instrumentation for the residual stale-resurrection window: a retried job landing
    after a later job already superseded the same table in the same load

Response shapes mirror the real backend's `{"data": {...}}` / `{"error": {"context": ...}}`
convention exactly, so `HttpTransport` is exercised against the same envelope it will see
in production, not a simplified stand-in shape.

Knobs (env), so tests can exercise the current configuration and the one it replaced:
  MB_LOCK=on|off      entry lock. off == today's http_collector: one shared writer
                      identity for all ingress, nothing to serialise on
  MB_CLAMP=on|off     high-water clamp on begin_timestamp
  MB_DEDUP=on|off     record-level `_dlt_id` backstop -- off reproduces the bug this
                      backstop exists to fix, for tests that want to demonstrate it
  MB_JITTER_MS=n      sleep between record_id allocation and the commit, so the ordering
                      race is deterministic instead of probabilistic
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import zstandard

from . import segment_store

TOKEN = "fake-token"

LOCK_ON = os.environ.get("MB_LOCK", "on") == "on"
CLAMP_ON = os.environ.get("MB_CLAMP", "on") == "on"
DEDUP_ON = os.environ.get("MB_DEDUP", "on") == "on"
JITTER_MS = int(os.environ.get("MB_JITTER_MS", "0"))

# How many distinct job_ids to remember per recordtype for the residual-window check
# (mirrors the real backend's domains/ingest/ordering.py).
_JOB_ORDER_WINDOW = 16


class Contention(Exception):
    """Lock contention -- retryable. Distinguished from a closed load / paused pid, both
    of which are terminal."""


class Terminal(Exception):
    def __init__(self, reason: str):
        self.reason = reason


class _ChaosCrash(Exception):
    """Test-only: a test asked `FakeMatterbeamState.ingest` to blow up at a named
    injection point (see `_chaos_crash`), to exercise the write-ordering guarantee
    directly rather than trying to race a real crash."""

    def __init__(self, point: str):
        self.point = point


class FakeMatterbeamState:
    """All server state for one fake-Matterbeam instance. Constructed fresh per test
    (or per subprocess, via `main()`) rather than a module global, so tests can run
    several fake servers concurrently without cross-talk."""

    def __init__(self, coldlog_root: str) -> None:
        self.coldlog_root = coldlog_root
        self.mu = threading.Lock()
        self.pids: dict[str, dict] = {}
        self.registry: dict[str, str] = {}
        self.pending_uploads: dict[str, tuple[str, str]] = {}
        self.uploaded_packages: dict[str, bytes] = {}
        self.process_state: dict[str, dict] = {}
        self.ledger: set[str] = set()
        self.ledger_results: dict[str, dict] = {}
        self.dlt_ids_seen: dict[tuple[str, str], set[str]] = {}
        self.closed_loads: set[tuple[str, str]] = set()
        self.counters = {
            "lock_contention": 0,
            "duplicates": 0,
            "inversions": 0,
            "record_dedup_hits": 0,
            "residual_stale_resurrection": 0,
        }
        self.visibility: dict[str, list[str]] = {}
        # Settable by a test after construction: makes the entry lock's contention window
        # wide enough to hit deterministically instead of racing real request latency.
        self.ingest_jitter_ms = 0

    def ps(self, pid: str) -> dict:
        return self.process_state.setdefault(pid, {"dlt_state": None, "recordtypes": {}})

    # ---------------------------------------------------------------- FSM entry lock

    def claim(self, pid: str) -> int:
        with self.mu:
            item = self.pids.get(pid)
            if item is None:
                raise KeyError(pid)
            if item.get("fsm_state") == "PAUSED":
                raise Terminal("paused")
            seen_version = item["update_version"]
            if not LOCK_ON:
                return seen_version
            if item["invoke_semaphore"] != 1:
                self.counters["lock_contention"] += 1
                raise Contention()
            item["update_version"] += 1
            item["invoke_semaphore"] = 0
            item["fsm_state"] = "RUNNING"
            return item["update_version"]

    def release(self, pid: str, version: int) -> None:
        if not LOCK_ON:
            return
        with self.mu:
            item = self.pids[pid]
            if item["update_version"] == version:
                item["update_version"] += 1
                item["invoke_semaphore"] = 1
                item["fsm_state"] = "LISTENING"

    # ------------------------------------------------------------------------ registration

    def register(self, pipeline_key: str, dataset_name: str, collector_type: str = "external_dlt") -> tuple[str, bool]:
        """Mirrors `_create_external_dlt_collector`'s conditional-put lookup-or-create
        (confirmed real: `legacy/handlers/collectors.py:1528-1615`) for `type=external_dlt`,
        plus this project's own `type=hosted_dlt` addition: a fresh claim gets a
        `runtime`/`execution` payload of its own; a claim that already resolves to an
        existing *external* collector is transitioned in place (`set_pid_runtime`, real
        but currently internal-only, `pid_fsm.py:415-422`) rather than duplicated -- an
        adopt-in-place design."""
        key = f"{pipeline_key}|{dataset_name}"
        with self.mu:
            pid = self.registry.get(key)
            if pid is not None:
                item = self.pids[pid]
                if collector_type == "hosted_dlt" and item.get("collector_type") == "external_dlt":
                    item["collector_type"] = "hosted_dlt"
                    item["execution"] = "hosted"
                    item["runtime"] = "ECS"
                return pid, False
            pid = "".join(random.choices("0123456789abcdefghijklmnopqrstuvwxyz", k=27))
            self.registry[key] = pid
            execution = "hosted" if collector_type == "hosted_dlt" else "external"
            runtime = "ECS" if collector_type == "hosted_dlt" else "LAMBDA"
            self.pids[pid] = {
                "update_version": 1,
                "invoke_semaphore": 1,
                "fsm_state": "LISTENING",
                "collector_type": collector_type,
                "execution": execution,
                "runtime": runtime,
                "secret": {},
            }
            self.ps(pid)
            return pid, True

    def pause(self, pid: str) -> None:
        with self.mu:
            self.pids[pid]["fsm_state"] = "PAUSED"

    # ------------------------------------------------------------------------------- secrets

    def submit_secrets(self, pid: str, secrets: dict[str, str]) -> None:
        """Lands on the encrypt-on-write path, never verbatim -- fake KMS is just a
        tagged marker (`encrypted:<value>`), enough for a test to assert nothing plaintext ever
        sits in `self.pids[pid]["secret"]` under the submitted value alone."""
        with self.mu:
            if pid not in self.pids:
                raise KeyError(pid)
            stored = self.pids[pid].setdefault("secret", {})
            for name, value in secrets.items():
                stored[name] = f"encrypted:{value}"

    def decrypt_secret(self, pid: str, name: str) -> "str | None":
        enc = self.pids.get(pid, {}).get("secret", {}).get(name)
        if enc is None:
            return None
        assert enc.startswith("encrypted:"), "fake KMS marker missing -- secret stored verbatim"
        return enc[len("encrypted:") :]

    # ------------------------------------------------------------------------------- upload

    def create_upload_url(self, pid: str, filename: str, host: str) -> dict:
        """Mirrors `create_deployment_upload_url`'s real, confirmed shape
        (`routers/collectors_ext/router.py`, `domains/deploy/repository.py`) --
        `{"upload_url": ..., "key": ...}`, unwrapped, landing in the dedicated
        `CustomerUploads` bucket's `deployments/dlt/{pid}/` prefix (no lifecycle
        expiration, unlike the CSV route's `file-uploads/` prefix): a same-process URL
        standing in for a presigned S3 `PutObject`, since this fake server has no real S3
        behind it."""
        with self.mu:
            if pid not in self.pids:
                raise KeyError(pid)
            token = "".join(random.choices("0123456789abcdef", k=16))
            key = f"deployments/dlt/{pid}/{filename}"
            self.pending_uploads[token] = (pid, key)
            return {"upload_url": f"http://{host}/uploads/{token}", "key": key}

    def receive_upload(self, token: str, data: bytes) -> str:
        with self.mu:
            entry = self.pending_uploads.pop(token, None)
            if entry is None:
                raise KeyError(token)
            pid, _key = entry
            self.uploaded_packages[pid] = data
            return pid

    # --------------------------------------------------------------------------- build

    def trigger_build(self, pid: str) -> None:
        """Mirrors `trigger_deployment_build`'s real shape (`routers/collectors_ext/
        router.py`, `domains/deploy/repository.py`'s `DeployRepository.trigger_build`):
        this fake has no pid_fsm singleton to actually dispatch to, so it just records
        `build_status=building` -- exercising `DeployClient.trigger_build`'s wire
        contract, not a real build."""
        with self.mu:
            if pid not in self.pids:
                raise KeyError(pid)
            self.pids[pid]["build_status"] = "building"
            self.pids[pid]["build_error"] = ""

    def get_build_status(self, pid: str) -> dict:
        with self.mu:
            if pid not in self.pids:
                raise KeyError(pid)
            item = self.pids[pid]
            return {
                "build_status": item.get("build_status", "never_built"),
                "build_error": item.get("build_error") or None,
                "current_package_hash": item.get("current_package_hash"),
                "installed_packages": item.get("installed_packages") or {},
            }

    # ------------------------------------------------------------------------------ ingest

    def ingest(
        self,
        pid: str,
        table: str,
        load_id: str,
        job_id: str,
        seq: str,
        lines: list[bytes],
        chaos_crash_at: "str | None" = None,
    ):
        # An already-recorded chunk is acknowledged and discarded without appending,
        # regardless of the load's closed state -- a harmless replay, not new data
        # arriving late. Checked before the entry lock, same as the real backend: a
        # replay doesn't need to contend for it.
        #
        # Keyed by (pid, load_id, job_id, seq), not load_id alone: dlt's load_id
        # (`str(increasing_precise_time())`) is unique in practice but has no
        # server-side uniqueness guarantee across different pipelines, and this is
        # server-side bookkeeping that shouldn't trust a client-generated id not to
        # collide across two unrelated pids.
        lkey = f"{pid}|{load_id}|{job_id}|{seq}"
        with self.mu:
            if lkey in self.ledger:
                self.counters["duplicates"] += 1
                return {**self.ledger_results[lkey], "duplicate": True}

        version = self.claim(pid)
        try:
            if self.ingest_jitter_ms:
                time.sleep(self.ingest_jitter_ms / 1000.0)

            # Genuinely new data arriving after the load has been closed is rejected, not
            # silently accepted.
            if (pid, load_id) in self.closed_loads:
                raise Terminal("load_closed")

            # No dataset_name here: a pid is already 1:1-bound to one dlt dataset_name at
            # registration, so (pid, table) alone identifies the recordtype -- matching
            # the real backend's `sys:dlt,pid:{pid},table:{table}`.
            recordtype_id = f"{pid}.{table}"
            state = self.ps(pid)
            rt_state = state["recordtypes"].setdefault(recordtype_id, {})

            # Per-request writer_id allocation.
            with self.mu:
                writer_id = (self.pids[pid].get("writer_seq", 0) + 1) % 65536
                self.pids[pid]["writer_seq"] = writer_id

            # Residual stale-resurrection window, instrumented: a job's chunk arriving
            # after a *different* job already touched this table in this load, and now
            # this job is back -- the exact pattern a retried, superseded job produces.
            self._track_job_arrival(rt_state, job_id, pid, table, load_id)

            parsed = []
            for ln in lines:
                env = json.loads(ln)
                parsed.append((env.get("i"), dict(env["v"]), bool(env.get("t"))))

            # The record-level `_dlt_id` backstop runs on *every* chunk, not only once a
            # ledger entry has expired -- this is what catches the
            # crash-after-commit-before-ledger-write window the chunk ledger alone can't.
            seen = self.dlt_ids_seen.setdefault((pid, load_id), set())
            records = []
            written_dlt_ids = []
            for dlt_id, body, is_tombstone in parsed:
                if DEDUP_ON and dlt_id and dlt_id in seen:
                    self.counters["record_dedup_hits"] += 1
                    continue
                body["mb.metadata"] = {
                    "version": 1,
                    "record_type_id": recordtype_id,
                    "collected_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    **({"is_tombstone": True} if is_tombstone else {}),
                    **({"source_record_id": dlt_id} if dlt_id else {}),
                    **({"source_load_id": load_id} if load_id else {}),
                }
                records.append(json.dumps(body, separators=(",", ":")).encode("utf8"))
                if dlt_id:
                    written_dlt_ids.append(dlt_id)

            # begin_timestamp stamped at batch start -- what makes the ordering-inversion
            # race reachable at all.
            begin_ms = segment_store.now_ms()
            if CLAMP_ON:
                begin_ms = max(begin_ms, rt_state.get("high_water_ms", 0) + 1)

            if JITTER_MS:
                time.sleep(random.uniform(0, JITTER_MS) / 1000.0)

            first_id = last_id = None
            segment_key = None
            if records:
                segment_key, first_id, last_id, _nbytes = segment_store.write_segment(
                    self.coldlog_root, recordtype_id, records, begin_ms, writer_id
                )
                with self.mu:
                    vis = self.visibility.setdefault(recordtype_id, [])
                    if vis and str(last_id) < max(vis):
                        # this segment sorts below an already-visible one: any reader
                        # whose cursor had advanced will never list it
                        self.counters["inversions"] += 1
                    vis.append(str(last_id))

            self._chaos_crash(chaos_crash_at, "after_commit_before_mark")

            # The mark and the ledger write both happen only now, after the commit above
            # has already succeeded -- never before.
            with self.mu:
                seen.update(written_dlt_ids)

            self._chaos_crash(chaos_crash_at, "after_mark_before_ledger")

            rt_state["high_water_ms"] = max(rt_state.get("high_water_ms", 0), begin_ms)
            rt_state["last_record_id"] = str(last_id) if last_id is not None else rt_state.get("last_record_id")

            result = {
                "record_count": len(records),
                "segment_key": segment_key,
                "writer_id": writer_id,
                "first_record_id": str(first_id) if first_id is not None else None,
                "last_record_id": str(last_id) if last_id is not None else None,
                "duplicate": False,
            }
            # The ledger write follows the commit, unconditionally -- never the other
            # order, which trades a duplicate for silent loss.
            with self.mu:
                self.ledger.add(lkey)
                self.ledger_results[lkey] = result

            self._chaos_crash(chaos_crash_at, "after_ledger")

            return result
        finally:
            self.release(pid, version)

    def _track_job_arrival(self, rt_state: dict, job_id: str, pid: str, table: str, load_id: str) -> bool:
        order: list = rt_state.setdefault("job_order", [])
        if order and order[-1] == job_id:
            return False
        resurrected = job_id in order
        order.append(job_id)
        del order[:-_JOB_ORDER_WINDOW]
        if resurrected:
            rt_state["residual_stale_resurrection_count"] = rt_state.get("residual_stale_resurrection_count", 0) + 1
            self.counters["residual_stale_resurrection"] += 1
        return resurrected

    def _chaos_crash(self, requested_point: "str | None", point: str) -> None:
        """Test-only chaos hook, mirroring the real backend's double-gated one --
        here gated just by the caller explicitly asking, since this process only ever
        exists inside a test. A real exception (not a process kill): unlike the real
        backend, killing this thread's process would take the whole test process down
        with it, so tests that want a real severed connection drive that at the
        HTTP-handler layer instead (dropping the response), not here."""
        if requested_point == point:
            raise _ChaosCrash(point)

    # -------------------------------------------------------------------------- state

    def put_dlt_state(self, pid: str, doc: dict) -> None:
        version = self.claim(pid)
        try:
            state = self.ps(pid)
            previous = state.get("dlt_state")
            if previous and doc["version"] <= previous["version"]:
                raise Terminal("stale_state_version")
            state["dlt_state"] = doc
        finally:
            self.release(pid, version)

    def get_dlt_state(self, pid: str) -> dict | None:
        return self.ps(pid).get("dlt_state")

    def complete_load(self, pid: str, load_id: str) -> None:
        with self.mu:
            self.closed_loads.add((pid, load_id))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state: FakeMatterbeamState  # set per-instance by `make_handler_class`

    def log_message(self, *a):
        pass

    def _reply(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, code: int, reason: str) -> None:
        self._reply(
            code, {"error": {"type": "error", "message": reason, "code": "CONFLICT", "context": {"reason": reason}}}
        )

    def _auth(self) -> bool:
        if self.headers.get("Authorization") != f"Token {TOKEN}":
            self._reply(
                401, {"error": {"type": "authentication_error", "message": "no token", "code": "INVALID_TOKEN"}}
            )
            return False
        return True

    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n)
        if self.headers.get("Content-Encoding") == "zstd":
            raw = zstandard.ZstdDecompressor().decompress(raw, max_output_size=1 << 28)
        return raw

    def do_GET(self):
        if not self._auth():
            return
        parts = self.path.strip("/").split("/")
        if len(parts) == 3 and parts[0] == "collectors" and parts[2] == "state":
            doc = self.state.get_dlt_state(parts[1])
            if doc is None:
                return self._reply(
                    404, {"error": {"type": "not_found_error", "message": "no state", "code": "RESOURCE_NOT_FOUND"}}
                )
            return self._reply(200, {"data": doc})

        # GET /collectors/{pid}/deployment/status
        if len(parts) == 4 and parts[0] == "collectors" and parts[2] == "deployment" and parts[3] == "status":
            try:
                return self._reply(200, self.state.get_build_status(parts[1]))
            except KeyError:
                return self._reply(
                    404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}}
                )
        self._reply(404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}})

    def do_POST(self):
        if not self._auth():
            return
        parts = self.path.strip("/").split("/")
        try:
            # POST /collectors -- registration goes through the same endpoint that
            # creates every other collector type. Bare response, no {"data": ...}
            # envelope, matching the real legacy handler's convention.
            if parts == ["collectors"]:
                payload = json.loads(self._body())
                pipeline_key = payload["name"]
                dataset_name = (payload.get("config") or {}).get("dataset_name")
                collector_type = payload.get("type", "external_dlt")
                pid, created = self.state.register(pipeline_key, dataset_name, collector_type)
                return self._reply(
                    200, {"id": pid, "type": payload.get("type"), "name": pipeline_key, "created": created}
                )

            # POST /collectors/{pid}/deployment/upload-url -- generalizes the real, confirmed
            # handle_collector_upload_url for a deployment tarball instead of a CSV. A
            # distinct path from the legacy CSV route (confirmed live server-side):
            # collectors_ext's router is included before the legacy router, so reusing the
            # CSV route's exact path would have shadowed it for every collector.
            if len(parts) == 4 and parts[0] == "collectors" and parts[2] == "deployment" and parts[3] == "upload-url":
                pid = parts[1]
                payload = json.loads(self._body())
                filename = payload.get("filename") or ""
                data = self.state.create_upload_url(pid, filename, self.headers.get("Host", ""))
                return self._reply(200, data)

            # POST /collectors/{pid}/deployment/build -- fires the (fake) async build --
            # real server-side this dispatches through the pid_fsm RUN path (a singleton
            # dlt-package-builder pid), never a direct Lambda invoke.
            if len(parts) == 4 and parts[0] == "collectors" and parts[2] == "deployment" and parts[3] == "build":
                pid = parts[1]
                self.state.trigger_build(pid)
                return self._reply(200, {"status": "building"})

            # POST /collectors/{pid}/ingest/{table}:bulk
            if len(parts) == 4 and parts[0] == "collectors" and parts[2] == "ingest" and parts[3].endswith(":bulk"):
                pid = parts[1]
                table = parts[3][: -len(":bulk")]
                load_id = self.headers["x-mb-load-id"]
                job_id = self.headers["x-mb-job-id"]
                seq = self.headers["x-mb-seq"]
                chaos_crash_at = self.headers.get("x-mb-chaos-crash-at")
                lines = [ln for ln in self._body().split(b"\n") if ln.strip()]
                result = self.state.ingest(pid, table, load_id, job_id, seq, lines, chaos_crash_at=chaos_crash_at)
                return self._reply(200, {"data": result})

            # POST /collectors/{pid}/job/{load_id}
            if len(parts) == 4 and parts[0] == "collectors" and parts[2] == "job":
                pid = parts[1]
                load_id = parts[3]
                self.state.complete_load(pid, load_id)
                return self._reply(200, {"status": "ok"})
        except Contention:
            return self._error(409, "lock_contention")
        except Terminal as t:
            return self._error(409, t.reason)
        except Exception as e:  # tests: surface, do not handle
            import traceback

            traceback.print_exc()
            return self._reply(
                500, {"error": {"type": "internal_server_error", "message": repr(e), "code": "INTERNAL_SERVER_ERROR"}}
            )

        self._reply(404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}})

    def do_PUT(self):
        parts = self.path.strip("/").split("/")

        # PUT /uploads/{token} -- stands in for the presigned S3 PutObject itself: no
        # Authorization header, matching `DeployClient.upload_package` sending none -- the
        # URL's own (fake) signature is the auth.
        if len(parts) == 2 and parts[0] == "uploads":
            try:
                pid = self.state.receive_upload(parts[1], self._body())
                return self._reply(200, {"status": "ok", "pid": pid})
            except KeyError:
                return self._reply(
                    404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}}
                )

        if not self._auth():
            return
        try:
            if len(parts) == 3 and parts[0] == "collectors" and parts[2] == "state":
                self.state.put_dlt_state(parts[1], json.loads(self._body()))
                return self._reply(200, {"status": "ok"})
        except Contention:
            return self._error(409, "lock_contention")
        except Terminal as t:
            return self._error(409, t.reason)
        self._reply(404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}})

    def do_PATCH(self):
        if not self._auth():
            return
        parts = self.path.strip("/").split("/")
        try:
            # PATCH /collectors/{pid}/secrets -- the encrypt-on-write sibling this project
            # models for a dlt pipeline's arbitrary secret names (see deploy.py's
            # `DeployClient.submit_secrets` docstring for why this isn't the legacy
            # `_reclassify_secrets` route).
            if len(parts) == 3 and parts[0] == "collectors" and parts[2] == "secrets":
                payload = json.loads(self._body())
                self.state.submit_secrets(parts[1], payload.get("secrets") or {})
                return self._reply(200, {"status": "ok"})
        except KeyError:
            return self._reply(
                404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}}
            )
        self._reply(404, {"error": {"type": "not_found_error", "message": self.path, "code": "RESOURCE_NOT_FOUND"}})


def make_handler_class(state: FakeMatterbeamState) -> type:
    return type("BoundHandler", (Handler,), {"state": state})


def start_server(coldlog_root: str, port: int = 0) -> tuple[ThreadingHTTPServer, FakeMatterbeamState]:
    state = FakeMatterbeamState(coldlog_root)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler_class(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, state


def main():
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    root = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(__file__), "_coldlog")
    os.makedirs(root, exist_ok=True)
    server, _state = start_server(root, port)
    print(f"fake matterbeam on :{port}  lock={LOCK_ON} clamp={CLAMP_ON} jitter={JITTER_MS}ms", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
