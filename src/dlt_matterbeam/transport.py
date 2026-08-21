"""The `MatterbeamTransport` seam (D6).

Everything above this line -- envelope construction, key extraction, disposition
translation, PUA stripping -- is transport-agnostic. Below it, Phase 1 shipped exactly
one implementation, `FileTransport`, writing plain newline-delimited JSON to a local
directory with no server -- for local debugging only, so it makes no attempt to
reproduce the real internal coldlog wire format. Phase 2 adds `HttpTransport`, talking to
a real Matterbeam account's REST API. The internal-only optional package (out of scope
here, BRIEF §3) later adds a `DirectColdlogTransport` using the real `ColdlogWriter`
in-process -- the "deployment seam" D6 describes. That is also why registration, state
and the ingest write are all on this Protocol rather than only in `HttpTransport`: the
in-runtime transport will answer them from `process_state` directly, in-process, with no
network call at all.

Discovery mirrors dlt's own plugin resolution (A3): a transport is looked up by name, and
anything other than the builtin `"file"` / `"http"` is resolved through the
`dlt_matterbeam.transports` entry-point group. This package never imports the internal
package directly -- it doesn't know it exists. `tests/unit/test_transport_seam.py` asserts
that holds with the internal package absent, which is the only way this seam stays honest
as the codebase changes.
"""

from __future__ import annotations

import json
import logging
import time
from importlib.metadata import entry_points
from typing import Optional, Protocol, runtime_checkable

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)
from dlt_matterbeam import envelope

TRANSPORTS_ENTRY_POINT_GROUP = "dlt_matterbeam.transports"

logger = logging.getLogger("dlt_matterbeam")


class Conflict(Exception):
    """A non-retryable 409 whose meaning is caller-specific, not transport-generic.
    `ingest`/`register`/`complete_load` should turn this into a terminal job failure
    (a closed load or a paused pid really is terminal); `put_dlt_state` should not --
    per the design doc's D5 guidance, a stale-state conflict is a bookkeeping race, not a
    reason to fail an otherwise-good data load (§8.2; see the Phase 2 report -- caught the
    hard way, by a test that ran two "laptops" against the same pipeline concurrently and
    watched the state write's 409 abort a load whose *rows* had already landed correctly).
    """

    def __init__(self, reason: Optional[str], detail: str) -> None:
        self.reason = reason
        super().__init__(detail)


@runtime_checkable
class MatterbeamTransport(Protocol):
    """Delivers one dlt table's rows for one load job chunk, and answers identity/state.

    `register`/`get_dlt_state`/`put_dlt_state`/`complete_load` are no-ops (returning
    `None`, doing nothing) for a transport with no server and no process identity, e.g.
    `FileTransport` -- that is a legitimate, silently-degrading `WithStateSync` gap (A15),
    not an error.
    """

    def register(self, pipeline_key: str, dataset_name: str) -> Optional[str]: ...

    def get_dlt_state(self, pid: str) -> Optional[dict]: ...

    def put_dlt_state(self, pid: str, doc: dict) -> None: ...

    def send_chunk(
        self,
        *,
        recordtype_id: str,
        dataset_name: str,
        table_name: str,
        rows: list[dict],
        keys: list[str],
        hard_delete: list[str],
        load_id: str,
        job_id: str,
        seq: int,
        pid: Optional[str],
    ) -> str: ...

    def complete_load(self, load_id: str, pid: Optional[str]) -> None: ...


class FileTransport:
    """Phase 1 (D6): writes plain newline-delimited JSON to a local directory, purely for
    local debugging -- there is no real Matterbeam account behind it, so it makes no
    attempt to reproduce the real internal coldlog wire format (no CRF framing, no
    compression, no record_id allocation). One human-readable `<recordtype_id>.jsonl`
    file per table, appended to across runs. No Matterbeam imports, no network, no
    server-side identity -- `register`/state are no-ops, exactly matching Phase 1's
    "WithStateSync not implemented, degrades silently" behaviour (A15).
    """

    def __init__(self, root: str) -> None:
        self.root = root

    def register(self, pipeline_key: str, dataset_name: str) -> Optional[str]:
        return None

    def get_dlt_state(self, pid: str) -> Optional[dict]:
        return None

    def put_dlt_state(self, pid: str, doc: dict) -> None:
        pass

    def send_chunk(
        self,
        *,
        recordtype_id: str,
        dataset_name: str,
        table_name: str,
        rows: list[dict],
        keys: list[str],
        hard_delete: list[str],
        load_id: str,
        job_id: str,
        seq: int,
        pid: Optional[str],
    ) -> str:
        import os

        lines = [
            envelope.encode_record(
                envelope.build_record(
                    row, record_type_id=recordtype_id, keys=keys, hard_delete=hard_delete, load_id=load_id
                )
            )
            for row in rows
        ]
        os.makedirs(self.root, exist_ok=True)
        key = f"{recordtype_id}.jsonl"
        with open(os.path.join(self.root, key), "ab") as f:
            for line in lines:
                f.write(line)
                f.write(b"\n")
        return key

    def complete_load(self, load_id: str, pid: Optional[str]) -> None:
        pass


class HttpTransport:
    """Phase 2 (D1/D6): talks to the customer REST API. The server allocates
    `record_id`s and stamps `mb.metadata` (C2) -- this transport only builds the minimal
    wire envelope (`{"t": bool, "i": str|null, "v": {...}}`; D1's `k` field is dropped,
    not replaced -- the fold-key declaration path (P3) is out of scope for this project,
    see the Phase 2 report) and handles the two distinguishable 409s (§8.2 D4 fix):
    lock contention is retried with backoff, a closed load or a paused pid is terminal.
    """

    def __init__(self, base_url: str, api_token: Optional[str], max_409_retries: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_token = api_token
        self.max_409_retries = max_409_retries
        self._session = None

    @property
    def session(self):
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _headers(self, extra: Optional[dict] = None) -> dict:
        # `Authorization: Bearer` is reserved for Cognito-issued user JWTs on the real
        # backend (verified via JWKS, requires an `iss` claim) -- an API key sent that way
        # fails authentication silently, logged server-side as "JWT validation raised
        # before signature check" rather than as an invalid-key error. API keys go through
        # `Authorization: Token <key>` instead (confirmed against a live dev account
        # while building Phase 2; the design doc's D1 originally specified `Bearer`).
        headers = {"Authorization": f"Token {self.api_token}"}
        headers.update(extra or {})
        return headers

    def _request(self, method: str, path: str, *, json_body=None, data=None, headers=None):
        for attempt in range(self.max_409_retries):
            response = self.session.request(
                method, self._url(path), json=json_body, data=data, headers=self._headers(headers)
            )
            if response.status_code == 409:
                reason = _conflict_reason(response)
                if reason == "lock_contention":
                    time.sleep(0.05 * (attempt + 1))
                    continue
                # Not retryable, but what it *means* is caller-specific -- see `Conflict`.
                raise Conflict(reason, f"{method} {path} -> 409 ({reason}): {response.text}")
            if response.status_code == 404:
                return None
            if response.status_code >= 500:
                raise DestinationTransientException(f"{method} {path} -> {response.status_code}: {response.text}")
            if response.status_code >= 400:
                raise DestinationTerminalException(f"{method} {path} -> {response.status_code}: {response.text}")
            return response
        raise DestinationTerminalException(f"{method} {path}: gave up after {self.max_409_retries}x lock_contention")

    def _terminal_on_conflict(self, method: str, path: str, **kwargs):
        """For every conflict *except* the state write, a 409 means the request really
        cannot proceed (a closed load, a paused pid) -- turn it into a terminal job
        failure. `put_dlt_state` has its own, deliberately different handling below."""
        try:
            return self._request(method, path, **kwargs)
        except Conflict as conflict:
            raise DestinationTerminalException(str(conflict)) from conflict

    def register(self, pipeline_key: str, dataset_name: str) -> Optional[str]:
        """Registration goes through the same `POST /collectors` endpoint the UI uses to
        create every other collector type (`type: "external_dlt"`) -- not a dedicated
        route -- so a dlt pipeline is one more collector, not a parallel creation path.
        That endpoint's responses are bare (no `{"data": ...}` envelope, a legacy-handler
        convention this project doesn't own)."""
        response = self._terminal_on_conflict(
            "POST",
            "/collectors",
            json_body={"type": "external_dlt", "name": pipeline_key, "config": {"dataset_name": dataset_name}},
        )
        return response.json()["id"]

    def get_dlt_state(self, pid: str) -> Optional[dict]:
        response = self._terminal_on_conflict("GET", f"/collectors/{pid}/state")
        return response.json()["data"] if response is not None else None

    def put_dlt_state(self, pid: str, doc: dict) -> None:
        """D5: guarded on `StateInfo.version` server-side. A stale write loses this race
        against a newer one and is logged, not raised -- dlt's own bookkeeping losing a
        race is not a reason to fail a load whose *rows* already landed (§8.2 report)."""
        try:
            self._request("PUT", f"/collectors/{pid}/state", json_body=doc)
        except Conflict as conflict:
            if conflict.reason != "stale_state_version":
                raise DestinationTerminalException(str(conflict)) from conflict
            logger.warning(f"dlt state write for pid {pid} lost the two-writer race (stale_state_version); ignoring")

    def send_chunk(
        self,
        *,
        recordtype_id: str,
        dataset_name: str,
        table_name: str,
        rows: list[dict],
        keys: list[str],
        hard_delete: list[str],
        load_id: str,
        job_id: str,
        seq: int,
        pid: Optional[str],
    ) -> str:
        """`recordtype_id`/`dataset_name` are accepted (the `MatterbeamTransport` Protocol
        is shared with `FileTransport`, which does need them to self-assign local
        recordtype ids) but unused here: the server computes its own recordtype identity
        from `pid` + `table_name` alone, and `pid` addresses the request directly in the
        URL now -- a pid is already the identity a dlt pipeline registers as (H2), so
        `dataset_name` would be a second, redundant axis of identity, and collides with
        Matterbeam's own unrelated `Dataset` concept. See the design doc's Phase 2 report.
        """
        import zstandard

        lines = [
            json.dumps(
                envelope.build_wire_record(row, keys=keys, hard_delete=hard_delete), separators=(",", ":")
            ).encode("utf8")
            for row in rows
        ]
        body = zstandard.ZstdCompressor(level=3).compress(b"\n".join(lines))
        response = self._terminal_on_conflict(
            "POST",
            f"/collectors/{pid}/ingest/{table_name}:bulk",
            data=body,
            headers={
                "Content-Encoding": "zstd",
                "X-MB-Load-Id": load_id,
                "X-MB-Job-Id": job_id,
                "X-MB-Seq": str(seq),
            },
        )
        return response.json()["data"].get("segment_key")

    def complete_load(self, load_id: str, pid: Optional[str]) -> None:
        self._terminal_on_conflict("POST", f"/collectors/{pid}/job/{load_id}")


def _conflict_reason(response) -> Optional[str]:
    try:
        return response.json()["error"]["context"].get("reason")
    except Exception:
        return None


def resolve_transport(name: str, config: object) -> MatterbeamTransport:
    """Select a transport by name. `"file"` and `"http"` are builtin; anything else must
    be registered under the `dlt_matterbeam.transports` entry-point group."""
    if name == "file":
        output_dir = getattr(config, "output_dir", None)
        if not output_dir:
            raise ConfigurationValueError("matterbeam: `output_dir` is required for the `file` transport")
        return FileTransport(output_dir)

    if name == "http":
        matterbeam_url = getattr(config, "matterbeam_url", None)
        if not matterbeam_url:
            raise ConfigurationValueError("matterbeam: `matterbeam_url` is required for the `http` transport")
        return HttpTransport(matterbeam_url, getattr(config, "api_token", None))

    for ep in entry_points(group=TRANSPORTS_ENTRY_POINT_GROUP):
        if ep.name == name:
            factory = ep.load()
            return factory(config)

    raise ConfigurationValueError(
        f"matterbeam: unknown transport {name!r}. Built in: 'file', 'http'. Anything else must be "
        f"registered under the '{TRANSPORTS_ENTRY_POINT_GROUP}' entry-point group."
    )
